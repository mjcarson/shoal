//! Measurement 5: listing a placement group, which is what a light scrub does
//!
//! S11's light scrub lists what a slice holds: consumer, owner, stripe, position, label and
//! length, to compare with the rows. Names come from the directories, a length from the inode,
//! and a label from wherever S6 keeps it: in the chunk's header block today, so a listing that
//! answers the scrub reads a block of every chunk. The walks go a step deeper each: names;
//! lengths; lengths with the inodes taken in order; a label kept in an extended attribute; a
//! label read from the header, one at a time and thirty-two at a time. Each is walked cold, with
//! every cache dropped, and warm. The populations are a placement group of a hundred thousand
//! and of a million chunks, deep (sixty-four chunks an object) and wide (one), each file one
//! real header block and a forty-byte label attribute. T4 is judged on them.

use std::ffi::{CStr, CString};
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;

use futures::stream::{self, StreamExt};

use super::counters::Devices;
use super::io;
use super::stats::fmt;
use super::sys;
use super::table::Table;
use super::{on_core, ordered, Ctx, SideOut};
use crate::placement::timing::pin;

/// The label attribute's name
const LABEL: &CStr = c"user.x6.label";

/// A placement group's population
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Population {
    /// Chunks in it
    chunks: usize,
    /// Chunks an object holds
    per_object: usize,
}

impl Population {
    /// Its name
    fn name(self) -> String {
        let shape = if self.per_object == 1 { "wide" } else { "deep" };
        let chunks = if self.chunks >= 1_000_000 {
            format!("{}M", self.chunks / 1_000_000)
        } else {
            format!("{}K", self.chunks / 1000)
        };
        format!("{shape}-{chunks}")
    }
}

/// One way of walking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Walk {
    /// Every name, from `getdents64`
    Names,
    /// Every length too, `statx` in the order the directory lists
    Statx,
    /// Every length, with the inodes sorted first
    StatxIno,
    /// Every length and a label kept in an extended attribute, inodes sorted
    Xattr,
    /// Every length and the label from the header block, one read at a time, inodes sorted
    HeaderQd1,
    /// The same, thirty-two reads in flight on an executor
    HeaderQd32,
}

impl Walk {
    /// Its name
    fn name(self) -> &'static str {
        match self {
            Walk::Names => "names",
            Walk::Statx => "statx",
            Walk::StatxIno => "statx-ino",
            Walk::Xattr => "xattr",
            Walk::HeaderQd1 => "header-qd1",
            Walk::HeaderQd32 => "header-qd32",
        }
    }
}

/// Make a population if it is not already there
///
/// Built by one thread a physical core, each taking a run of objects. Each file is created,
/// given one header block through the page cache and its label attribute, and closed; the
/// filesystem is synced once at the end. A marker says the population is whole.
///
/// # Arguments
///
/// * `root` - The population's directory
/// * `population` - What it holds
/// * `cores` - The cpus to build it on
fn populate(root: &Path, population: Population, cores: &[usize]) -> Option<f64> {
    let marker = root.join("complete");
    if marker.exists() {
        return None;
    }
    io::wipe(root);
    let pg = root.join("pg0");
    std::fs::create_dir_all(&pg).expect("made");
    let objects = population.chunks.div_ceil(population.per_object);
    let start = Instant::now();
    std::thread::scope(|scope| {
        for (worker, &cpu) in cores.iter().enumerate() {
            let pg = pg.clone();
            scope.spawn(move || {
                pin(cpu);
                let header = vec![0x5a_u8; 4096];
                let label = [0x4c_u8; 40];
                // this worker's objects, by their place modulo the workers
                for object in (worker..objects).step_by(cores.len()) {
                    // object ids are hashed, as a node's would be, so names do not sort by age
                    let id = super::stats::Rng::new(object as u64).next();
                    let dir = pg.join(format!("{id:016x}"));
                    std::fs::create_dir(&dir).expect("made");
                    for chunk in 0..population.per_object {
                        if object * population.per_object + chunk >= population.chunks {
                            break;
                        }
                        let path = dir.join(format!("{:06}.{}", chunk / 4, chunk % 4));
                        let file = std::fs::File::create(&path).expect("created");
                        std::io::Write::write_all(&mut &file, &header).expect("written");
                        sys::set_xattr(file.as_raw_fd(), LABEL, &label).expect("labelled");
                    }
                }
            });
        }
    });
    sys::syncfs(root).expect("synced");
    let took = start.elapsed().as_secs_f64();
    std::fs::write(&marker, population.chunks.to_string()).expect("marked");
    Some(took)
}

/// One chunk found by a walk: its object's name, its own, its inode
struct Found {
    /// The object directory's name
    object: CString,
    /// The chunk's name
    name: CString,
    /// Its inode
    ino: u64,
}

impl Found {
    /// Its path relative to the placement group
    fn relative(&self) -> CString {
        let mut path = self.object.as_bytes().to_vec();
        path.push(b'/');
        path.extend_from_slice(self.name.as_bytes());
        CString::new(path).expect("no NUL")
    }
}

/// Read every chunk of a placement group, objects in the order the directory lists them
///
/// # Arguments
///
/// * `pg_fd` - The placement group's directory
/// * `buffer` - Scratch for `getdents64`
fn list(pg_fd: &OwnedFd, buffer: &mut [u8]) -> Vec<Found> {
    let mut found = Vec::new();
    for object in sys::read_dir(pg_fd.as_raw_fd(), buffer).expect("listed") {
        if !object.is_dir {
            continue;
        }
        let fd = sys::open_dir(Some(pg_fd.as_raw_fd()), &object.name).expect("opened");
        for entry in sys::read_dir(fd.as_raw_fd(), buffer).expect("listed") {
            found.push(Found { object: object.name.clone(), name: entry.name, ino: entry.ino });
        }
    }
    found
}

/// Walk a placement group one way on this thread, returning how many chunks it found
///
/// # Arguments
///
/// * `pg` - The placement group's directory
/// * `walk` - The way
fn walk_here(pg: &Path, walk: Walk) -> usize {
    let mut buffer = vec![0_u8; 256 << 10];
    let pg_fd = sys::open_dir(None, &sys::cpath(pg)).expect("opened");
    match walk {
        Walk::Names => list(&pg_fd, &mut buffer).len(),
        Walk::Statx => {
            // each object's chunks stated as its directory lists them
            let mut count = 0;
            for object in sys::read_dir(pg_fd.as_raw_fd(), &mut buffer).expect("listed") {
                let fd = sys::open_dir(Some(pg_fd.as_raw_fd()), &object.name).expect("opened");
                for entry in sys::read_dir(fd.as_raw_fd(), &mut buffer).expect("listed") {
                    sys::statx_size(fd.as_raw_fd(), &entry.name).expect("stated");
                    count += 1;
                }
            }
            count
        }
        Walk::StatxIno | Walk::Xattr | Walk::HeaderQd1 => {
            // every chunk, then in inode order, then each one asked
            let mut found = list(&pg_fd, &mut buffer);
            found.sort_unstable_by_key(|chunk| chunk.ino);
            let mut label = [0_u8; 64];
            let mut head = sys::Aligned::new(4096);
            let prefix = pg.as_os_str().as_bytes();
            for chunk in &found {
                let relative = chunk.relative();
                sys::statx_size(pg_fd.as_raw_fd(), &relative).expect("stated");
                match walk {
                    Walk::Xattr => {
                        let mut path = prefix.to_vec();
                        path.push(b'/');
                        path.extend_from_slice(relative.as_bytes());
                        let path = CString::new(path).expect("no NUL");
                        sys::get_xattr(&path, LABEL, &mut label).expect("a label");
                    }
                    Walk::HeaderQd1 => sys::read_head(pg_fd.as_raw_fd(), &relative, &mut head).expect("read"),
                    _ => {}
                }
            }
            found.len()
        }
        Walk::HeaderQd32 => unreachable!("the executor walks thirty-two at a time"),
    }
}

/// Walk a placement group reading every header, thirty-two at a time, on the executor
///
/// # Arguments
///
/// * `pg` - The placement group's directory
async fn walk_qd32(pg: PathBuf) -> usize {
    // the names on this thread, in inode order
    let mut buffer = vec![0_u8; 256 << 10];
    let pg_fd = sys::open_dir(None, &sys::cpath(&pg)).expect("opened");
    let mut found = list(&pg_fd, &mut buffer);
    found.sort_unstable_by_key(|chunk| chunk.ino);
    let count = found.len();
    let pg = Rc::new(pg);
    // each opened, its header read, closed, thirty-two in flight
    stream::iter(found)
        .map(|chunk| {
            let pg = pg.clone();
            async move {
                let path = pg
                    .join(std::ffi::OsStr::from_bytes(chunk.object.as_bytes()))
                    .join(std::ffi::OsStr::from_bytes(chunk.name.as_bytes()));
                let file = io::open_read(&path).await;
                let _ = file.stat().await.expect("stated");
                file.read_at_aligned(0, 4096).await.expect("read");
                file.close().await.expect("closed");
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<()>>()
        .await;
    count
}

/// One walk, timed, with the device counted
///
/// # Arguments
///
/// * `ctx` - The run
/// * `pg` - The placement group
/// * `walk` - The way
/// * `devices` - The devices
fn timed(ctx: &Ctx, pg: &Path, walk: Walk, devices: &Devices) -> (usize, f64, f64, f64) {
    let before = devices.snap();
    let start = Instant::now();
    let count = if walk == Walk::HeaderQd32 {
        let pg = pg.to_path_buf();
        on_core(ctx.core, ctx.sibling, move || walk_qd32(pg))
    } else {
        let (pg, core) = (pg.to_path_buf(), ctx.core);
        std::thread::spawn(move || {
            pin(core);
            walk_here(&pg, walk)
        })
        .join()
        .expect("the walk finishes")
    };
    let took = start.elapsed().as_secs_f64();
    let delta = before.delta(&devices.snap());
    (count, took, delta.read as f64 / 1024.0, delta.written as f64 / 1024.0)
}

/// Run measurement 5 for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let populations: Vec<Population> = if ctx.quick {
        vec![Population { chunks: 2000, per_object: 64 }, Population { chunks: 2000, per_object: 1 }]
    } else {
        vec![
            Population { chunks: 100_000, per_object: 64 },
            Population { chunks: 100_000, per_object: 1 },
            Population { chunks: 1_000_000, per_object: 64 },
            Population { chunks: 1_000_000, per_object: 1 },
        ]
    };
    let cores: Vec<usize> = ctx.order.iter().map(|(cpu, _)| *cpu).collect();
    let mut built = Table::new(&["population", "files", "built in s", "files/s"]);
    let mut table = Table::new(&[
        "population", "walk", "chunks", "cold s", "cold µs/chunk", "cold dev KiB read/chunk",
        "warm s", "warm µs/chunk", "warm dev KiB written", "hours/16 TiB at 1 MiB", "at 4 MiB",
    ]);
    let mut records = Vec::new();
    let root = ctx.sub("listing");
    for population in &populations {
        let dir = root.join(population.name());
        if let Some(took) = populate(&dir, *population, &cores) {
            built.row(vec![
                population.name(),
                population.chunks.to_string(),
                fmt(took),
                fmt(population.chunks as f64 / took),
            ]);
            records.push(ctx.record(
                "listing",
                round,
                SideOut::new(population.name(), "build", &[("files", population.chunks as f64), ("build_s", took)]),
            ));
        }
        // the walks a population gets: every one at a hundred thousand, the telling ones at a million
        let walks: Vec<Walk> = match (population.chunks >= 1_000_000, population.per_object == 1) {
            (false, _) => vec![Walk::Names, Walk::Statx, Walk::StatxIno, Walk::Xattr, Walk::HeaderQd1, Walk::HeaderQd32],
            (true, false) => vec![Walk::Names, Walk::StatxIno, Walk::Xattr, Walk::HeaderQd32],
            (true, true) => vec![Walk::Names, Walk::StatxIno, Walk::HeaderQd32],
        };
        let pg = dir.join("pg0");
        for walk in ordered(&walks, round) {
            // cold: every cache dropped, then the walk; warm: the same walk again at once
            let cold_ok = sys::drop_caches().is_ok();
            let (count, cold_s, cold_read, _) = timed(ctx, &pg, walk, &ctx.facts.devices);
            let (_, warm_s, _, warm_written) = timed(ctx, &pg, walk, &ctx.facts.devices);
            let per = |seconds: f64| seconds * 1e6 / count.max(1) as f64;
            let hours = |chunk: f64| per(cold_s) * (16.0 * f64::from(1 << 30) * 1024.0 / chunk) / 3.6e9;
            table.row(vec![
                population.name(),
                walk.name().to_string(),
                count.to_string(),
                if cold_ok { fmt(cold_s) } else { format!("{} (not cold: not root)", fmt(cold_s)) },
                fmt(per(cold_s)),
                fmt(cold_read / count.max(1) as f64),
                fmt(warm_s),
                fmt(per(warm_s)),
                fmt(warm_written),
                fmt(hours(f64::from(1 << 20))),
                fmt(hours(f64::from(4 << 20))),
            ]);
            records.push(ctx.record(
                "listing",
                round,
                SideOut::new(
                    population.name(),
                    walk.name(),
                    &[
                        ("chunks", count as f64),
                        ("cold", if cold_ok { 1.0 } else { 0.0 }),
                        ("cold_s", cold_s),
                        ("cold_us", per(cold_s)),
                        ("cold_read_kib", cold_read / count.max(1) as f64),
                        ("warm_s", warm_s),
                        ("warm_us", per(warm_s)),
                        ("warm_written_kib", warm_written),
                        ("hours_1m", hours(f64::from(1 << 20))),
                        ("hours_4m", hours(f64::from(4 << 20))),
                    ],
                ),
            ));
        }
    }
    let label = ctx.label();
    print!("{}", built.render(&format!("5. Listing: the populations built, round {round}"), &label));
    print!("{}", table.render(&format!("5. Listing a placement group, round {round}"), &label));
    ctx.emit(&records);
    if !ctx.keep && ctx.quick {
        io::wipe(&root);
    }
}
