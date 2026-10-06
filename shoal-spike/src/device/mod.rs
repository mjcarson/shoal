//! X6 and X7: the device store on SSD and on a rotational disk, measured
//!
//! [Q22](../../../docs/src/object-storage/contract.md) asks how stripe chunks lie on a slice,
//! how an update is applied and what a sync costs, and which filesystems the store accepts;
//! [Q27](../../../docs/src/object-storage/contract.md) asks what one small write in place costs
//! on a device. X6 (`docs/src/object-storage/spikes.md#x6-the-device-store-on-ssd`) answers
//! them by measuring S6's layout against its alternatives on the lab's SSDs and filesystems,
//! with glommio's `DmaFile`, as a slice's executor would drive them.
//!
//! [Q23](../../../docs/src/object-storage/contract.md) asks what a rotational disk needs that an
//! SSD does not: a journal on an SSD, an executor of its own, another layout, a way of reading
//! ahead. X7 (`docs/src/object-storage/spikes.md#x7-the-device-store-on-hdd`) runs X6's
//! measurements that matter on the lab's disks, bounded for a device that answers in
//! milliseconds, and adds what only a disk shows: sequential and random rates by size and depth
//! (`seq`), whether syncs merge (`sync`), a read and a stage while applies run (`contend`), a
//! foreground under a scrub's budget (`scrub`), one executor driving an SSD and a disk
//! (`shared`), and the disk with its write cache off (`wcoff`).
//!
//! - `shoal-spike device <measurement> --dir <scratch> [flags]` runs one measurement, or `all`
//!   of them, on the filesystem `--dir` is on, after the probes
//! - `--ssd-dir <scratch>` names a directory on the host's SSD, for the sides of X7 that put a
//!   journal or a slice there
//! - `shoal-spike device report <records.json…>` merges rounds and judges the triggers
//! - `shoal-spike device slc --dir <scratch>` writes until an SSD's write cache runs out
//!
//! The measurements write bytes and never parse them back: there is no replay, no index and no
//! format here, and like every spike's this code is thrown away. M14's store is written from
//! the decision this records, not from this.

pub mod arm;
pub mod chunk;
pub mod contend;
pub mod counters;
pub mod facts;
pub mod frag;
pub mod io;
pub mod journal;
pub mod listing;
pub mod paced;
pub mod partial;
pub mod read;
pub mod record;
pub mod remove;
pub mod report;
pub mod scrub;
pub mod seq;
pub mod shared;
pub mod slices;
pub mod stats;
pub mod sync;
pub mod sys;
pub mod table;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use glommio::{CpuSet, LocalExecutorBuilder, Placement, PoolPlacement};

use self::facts::{Facts, Probes};
use self::io::DirSync;
use self::record::Record;

/// Every measurement `all` runs, in the order it runs them: reads before writes, so a read's
/// population has aged and no write's debris is in its way. X7's sequential rates come first,
/// on a filesystem nothing has fragmented yet, and its own measurements last
pub const ALL: &[&str] = &[
    "seq", "read", "listing", "frag", "partial", "journal", "chunk", "remove", "slices", "sync",
    "contend", "scrub", "shared",
];

/// Measurements run only when named, after the plan's: the recycling supplement, added when
/// the first rounds put a file a chunk at the edge of T1 on the 970 EVO; X7's run with the
/// disk's write cache off, which the lab script turns off around it; and X7's million-chunk
/// listing, run once a filesystem since a disk takes hours to walk it
pub const EXTRA: &[&str] = &["chunk-recycle", "wcoff", "listing-1m"];

/// The longest a side of a write measurement runs on a rotational disk, whatever its count
pub const SIDE_CAP: Duration = Duration::from_secs(20);

/// A directory on the host's SSD, beside a rotational disk being measured
#[derive(Debug, Clone)]
pub struct Ssd {
    /// The scratch directory on it
    pub dir: PathBuf,
    /// Where it is, and on what
    pub facts: Facts,
}

/// What every measurement is given
pub struct Ctx {
    /// The scratch directory on the filesystem being measured
    pub dir: PathBuf,
    /// The cpu single-executor measurements run on
    pub core: usize,
    /// Its sibling, where its blocking thread runs
    pub sibling: Option<usize>,
    /// The cpus slices are added on, each with its sibling
    pub order: Vec<(usize, Option<usize>)>,
    /// How many slices the slices measurement drives
    pub slices: Vec<usize>,
    /// The rounds to run
    pub rounds: Vec<u32>,
    /// Whether this is a quick run, which proves each measurement runs and measures nothing
    pub quick: bool,
    /// Bytes a cell of a write measurement may write, roughly
    pub budget: u64,
    /// Where it runs, and on what
    pub facts: Facts,
    /// What the probes found
    pub probes: Probes,
    /// The leg's name, naming host, device and filesystem
    pub leg: String,
    /// One core's checksum rate, GiB/s, for the projection of a slice's cpu
    pub crc_gibs: f64,
    /// Where records are written
    pub out: Option<PathBuf>,
    /// Whether populations are kept for the next round
    pub keep: bool,
    /// The host's SSD, for the sides that journal or keep a slice there, if one was named
    pub ssd: Option<Ssd>,
    /// What `contend` and `scrub` append to every side's name: the lab script's write cache
    /// supplement names its sides `-wb` and `-wt`, so the two run under one leg
    pub side_suffix: String,
}

impl Ctx {
    /// The line every table carries
    #[must_use]
    pub fn label(&self) -> String {
        let quick = if self.quick { " · **quick: not a measurement**" } else { "" };
        format!("{} · leg {}{quick}", self.facts.label(), self.leg)
    }

    /// The directory a measurement keeps its files in
    ///
    /// # Arguments
    ///
    /// * `name` - The measurement
    #[must_use]
    pub fn sub(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// Make a record of one side
    ///
    /// # Arguments
    ///
    /// * `measurement` - The measurement
    /// * `round` - The round
    /// * `side` - The side's cell, name and figures
    #[must_use]
    pub fn record(&self, measurement: &str, round: u32, side: SideOut) -> Record {
        Record {
            host: self.facts.host.clone(),
            fs: self.facts.fs.clone(),
            leg: self.leg.clone(),
            measurement: measurement.to_string(),
            cell: side.cell,
            side: side.side,
            round,
            quick: self.quick,
            metrics: side.metrics,
        }
    }

    /// Write records to the output file, if there is one
    ///
    /// # Arguments
    ///
    /// * `records` - The records
    pub fn emit(&self, records: &[Record]) {
        if let Some(out) = &self.out {
            record::append(out, records);
        }
    }

    /// A count scaled down for a quick run
    ///
    /// # Arguments
    ///
    /// * `full` - The count of a real run
    /// * `quick` - The count of a quick one
    #[must_use]
    pub fn count(&self, full: usize, quick: usize) -> usize {
        if self.quick {
            quick
        } else {
            full
        }
    }

    /// A duration scaled down for a quick run
    ///
    /// # Arguments
    ///
    /// * `full` - The duration of a real run
    #[must_use]
    pub fn window(&self, full: Duration) -> Duration {
        if self.quick {
            full / 15
        } else {
            full
        }
    }

    /// The directory sync the probes chose
    #[must_use]
    pub fn dir_sync(&self) -> DirSync {
        self.probes.dir_sync
    }

    /// Whether the filesystem clones
    #[must_use]
    pub fn clones(&self) -> bool {
        self.probes.clone.is_ok()
    }

    /// Whether the device measured spins
    #[must_use]
    pub fn rotational(&self) -> bool {
        self.facts.rotational
    }

    /// The longest a write measurement's side may run: bounded on a rotational disk, where a
    /// count sized for an SSD would take minutes, and unbounded elsewhere, as X6 ran
    #[must_use]
    pub fn side_cap(&self) -> Option<Duration> {
        (self.rotational() && !self.quick).then_some(SIDE_CAP)
    }

    /// The SSD's directory for a measurement, if an SSD was named
    ///
    /// # Arguments
    ///
    /// * `name` - The measurement
    #[must_use]
    pub fn ssd_sub(&self, name: &str) -> Option<PathBuf> {
        self.ssd.as_ref().map(|ssd| ssd.dir.join(name))
    }
}

/// One side's results, as an executor hands them back
#[derive(Debug, Clone)]
pub struct SideOut {
    /// The cell
    pub cell: String,
    /// The side
    pub side: String,
    /// Its figures
    pub metrics: BTreeMap<String, f64>,
}

impl SideOut {
    /// A side's results from its figures
    ///
    /// # Arguments
    ///
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `metrics` - Its figures, by name
    #[must_use]
    pub fn new(cell: impl Into<String>, side: impl Into<String>, metrics: &[(&str, f64)]) -> Self {
        SideOut {
            cell: cell.into(),
            side: side.into(),
            metrics: metrics
                .iter()
                .map(|(name, value)| ((*name).to_string(), *value))
                .collect(),
        }
    }

    /// A figure of this side, zero if it has none
    ///
    /// # Arguments
    ///
    /// * `name` - The figure
    #[must_use]
    pub fn get(&self, name: &str) -> f64 {
        self.metrics.get(name).copied().unwrap_or(0.0)
    }
}

/// Run a future on an executor pinned to a cpu, its blocking thread on the cpu's sibling
///
/// The fork's default puts the blocking thread on the executor's own cpu, so every rename,
/// unlink, mkdir and clone would take time from the reactor. The sibling is where a node would
/// put it.
///
/// # Arguments
///
/// * `cpu` - The executor's cpu
/// * `sibling` - Where its blocking thread runs, or the same cpu if `None`
/// * `task` - The future's maker
pub fn on_core<G, F, T>(cpu: usize, sibling: Option<usize>, task: G) -> T
where
    G: FnOnce() -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    spawn_on(cpu, sibling, task)
        .join()
        .expect("the executor finishes")
}

/// Spawn a future on an executor pinned to a cpu, without waiting for it
///
/// # Arguments
///
/// * `cpu` - The executor's cpu
/// * `sibling` - Where its blocking thread runs, or the same cpu if `None`
/// * `task` - The future's maker
pub fn spawn_on<G, F, T>(cpu: usize, sibling: Option<usize>, task: G) -> glommio::ExecutorJoinHandle<T>
where
    G: FnOnce() -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    // the blocking thread on the sibling, or on the cpu itself
    let blocking = sibling.unwrap_or(cpu);
    let pool = PoolPlacement::Custom(vec![CpuSet::online()
        .expect("the cpus are listed")
        .filter(|location| location.cpu == blocking)]);
    LocalExecutorBuilder::new(Placement::Fixed(cpu))
        .name("device")
        .io_memory(32 << 20)
        .ring_depth(256)
        .blocking_thread_pool_placement(pool)
        .spawn(task)
        .expect("the executor spawns")
}

/// The sides of a cell in the order a round runs them: as declared in odd rounds, reversed in
/// even ones
///
/// # Arguments
///
/// * `sides` - The sides as declared
/// * `round` - The round
#[must_use]
pub fn ordered<T: Clone>(sides: &[T], round: u32) -> Vec<T> {
    let mut order = sides.to_vec();
    if round % 2 == 0 {
        order.reverse();
    }
    order
}

/// Write back everything dirty and let the device settle, before a side's counters are read
///
/// # Arguments
///
/// * `dir` - A directory on the filesystem
pub async fn settle(dir: &Path) {
    let _ = sys::syncfs(dir);
    glommio::timer::sleep(Duration::from_millis(250)).await;
}

/// Write back what a side left dirty, timed, so its bytes count to it: the drain
///
/// # Arguments
///
/// * `dir` - A directory on the filesystem
#[must_use]
pub fn drain(dir: &Path) -> f64 {
    let start = Instant::now();
    let _ = sys::syncfs(dir);
    start.elapsed().as_secs_f64() * 1e3
}

/// A size written the way the tables write it
///
/// # Arguments
///
/// * `bytes` - The size
#[must_use]
pub fn size_name(bytes: u64) -> String {
    if bytes >= 1 << 20 && bytes % (1 << 20) == 0 {
        format!("{}M", bytes >> 20)
    } else {
        format!("{}K", bytes >> 10)
    }
}

/// The value after a flag, if the flag is given
///
/// # Arguments
///
/// * `args` - The arguments
/// * `flag` - The flag
fn value_of(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == flag)
        .and_then(|at| args.get(at + 1))
        .cloned()
}

/// A comma separated list after a flag, if the flag is given
///
/// # Arguments
///
/// * `args` - The arguments
/// * `flag` - The flag
fn list_of<T: std::str::FromStr>(args: &[String], flag: &str) -> Option<Vec<T>> {
    value_of(args, flag).map(|list| {
        list.split(',')
            .map(|item| item.parse().unwrap_or_else(|_| panic!("{flag} takes a list")))
            .collect()
    })
}

/// One core's checksum rate, from X5's figures for the cpus the lab has
///
/// # Arguments
///
/// * `cpu` - The cpu's model
fn crc_rate(cpu: &str) -> f64 {
    // CRC-64/NVME through crc-fast at 64 KiB out of cache, one core (X5)
    if cpu.contains("7945HX") {
        40.5
    } else {
        11.4
    }
}

/// Refuse a scratch directory a run must not write to
///
/// # Arguments
///
/// * `facts` - The filesystem's facts
/// * `allow_root` - Whether the root filesystem was allowed by name
/// * `expect` - The filesystem the caller expects, if named
fn guard(facts: &Facts, allow_root: bool, expect: Option<&str>) {
    assert!(facts.fs != "tmpfs", "a tmpfs scratch directory has no direct I/O");
    assert!(
        allow_root || facts.mount != Path::new("/"),
        "the scratch directory is on the root filesystem; pass --allow-root-fs to measure it anyway"
    );
    if let Some(expect) = expect {
        assert_eq!(facts.fs, expect, "the scratch directory is not on the filesystem expected");
    }
}

/// Raise this process's limit on open files to its hard limit
fn raise_open_files() {
    // SAFETY: a zeroed rlimit is a valid out parameter, filled by the call
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            limit.rlim_cur = limit.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &limit);
        }
    }
}

/// Run X7's cells that ask what the disk's write cache costs, with it off
///
/// The lab script turns the cache off before and on again after; this refuses to run unless the
/// kernel says the disk writes through, so a figure labelled `-wt` never comes from a disk that
/// cached it.
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
fn run_wcoff(ctx: &Ctx, round: u32) {
    // the kernel's view, read now, not when the facts were gathered
    let cache = std::fs::read_to_string(format!("/sys/class/block/{}/queue/write_cache", ctx.facts.devices.disk))
        .map(|text| text.trim().to_string())
        .unwrap_or_default();
    assert_eq!(cache, "write through", "wcoff runs only with the disk's write cache off");
    sync::run(ctx, round, "-wt");
    journal::run_wcoff(ctx, round);
    partial::run_wcoff(ctx, round);
}

/// Run `shoal-spike device` with the arguments after the subcommand
///
/// # Arguments
///
/// * `args` - The arguments after `device`
pub fn main(args: &[String]) {
    let command = args.first().map(String::as_str).unwrap_or("help");
    // the read measurement holds two thousand chunks open at once
    raise_open_files();
    // report and the write cache probe stand apart from the rest
    match command {
        "report" => {
            let quick = args.iter().any(|arg| arg == "--quick");
            let paths: Vec<String> = args[1..].iter().filter(|arg| *arg != "--quick").cloned().collect();
            print!("{}", report::report(&record::read_all(&paths), quick));
            return;
        }
        "help" | "--help" => {
            println!(
                "shoal-spike device <facts|quick|{}|all|slc|clean|report> --dir <scratch> \
                 [--core N] [--slices 1,2,4] [--round R | --rounds N] [--budget-mib M] \
                 [--only a,b] [--expect-fs xfs] [--leg <name>] [--out f.json] \
                 [--keep-populations] [--allow-root-fs] [--crc-gibs X] [--ssd-dir <scratch>] \
                 [--quick] [--side-suffix -wt]\n  measurements: {}\n  only when named: {}",
                ALL.join("|"),
                ALL.join(","),
                EXTRA.join(",")
            );
            return;
        }
        _ => {}
    }
    let dir = PathBuf::from(value_of(args, "--dir").expect("--dir names the scratch directory"));
    std::fs::create_dir_all(&dir).expect("the scratch directory is made");
    if command == "clean" {
        io::wipe(&dir);
        return;
    }
    let facts = Facts::gather(&dir);
    guard(
        &facts,
        args.iter().any(|arg| arg == "--allow-root-fs"),
        value_of(args, "--expect-fs").as_deref(),
    );
    // the cpu, its sibling, and the order slices are added in
    let default_core = if facts.cpu.contains("7945HX") { 8 } else { 2 };
    let core = value_of(args, "--core").map_or(default_core, |core| core.parse().expect("--core takes a cpu"));
    let order = facts::slice_order(core);
    let sibling = facts::sibling(core);
    if command == "slc" {
        slices::slc_probe(&dir, core, sibling, &facts);
        return;
    }
    let quick = command == "quick" || args.iter().any(|arg| arg == "--quick");
    let rounds = match value_of(args, "--round") {
        Some(round) => vec![round.parse().expect("--round takes a number")],
        None => {
            let count: u32 = value_of(args, "--rounds").map_or(1, |n| n.parse().expect("--rounds takes a count"));
            (1..=count).collect()
        }
    };
    let leg = value_of(args, "--leg").unwrap_or_else(|| format!("{} {} {}", facts.host, facts.model, facts.fs));
    let label = format!("{} · leg {leg}", facts.label());
    // the probes, before anything leans on them
    let probes = {
        let (dir, devices, label) = (dir.join("probes"), facts.devices.clone(), label.clone());
        on_core(core, sibling, move || facts::probe(dir, devices, label))
    };
    print!("{}", probes.table);
    // the host's SSD, when a side journals or keeps a slice there
    let ssd = value_of(args, "--ssd-dir").map(|ssd_dir| {
        let dir = PathBuf::from(ssd_dir);
        std::fs::create_dir_all(&dir).expect("the SSD's scratch directory is made");
        let ssd_facts = Facts::gather(&dir);
        assert!(ssd_facts.fs != "tmpfs", "a tmpfs SSD directory has no direct I/O");
        assert!(!ssd_facts.rotational, "the --ssd-dir directory is on a rotational disk");
        Ssd { dir, facts: ssd_facts }
    });
    if let Some(ssd) = &ssd {
        println!("SSD beside it: {}\n", ssd.facts.label());
    }
    // a disk is driven by one slice and two, never more: past two is one arm shared further
    let default_slices: Vec<usize> = if facts.rotational { vec![1, 2] } else { vec![1, 2, 4, 8] };
    let ctx = Ctx {
        dir: dir.clone(),
        core,
        sibling,
        slices: list_of(args, "--slices").unwrap_or_else(|| {
            let cores = order.len();
            default_slices.into_iter().filter(|&n| n <= cores).collect()
        }),
        order,
        rounds,
        quick,
        budget: value_of(args, "--budget-mib").map_or(128, |mib| mib.parse().expect("--budget-mib takes MiB")) << 20,
        crc_gibs: value_of(args, "--crc-gibs").map_or_else(|| crc_rate(&facts.cpu), |rate| rate.parse().expect("--crc-gibs takes a rate")),
        facts,
        probes,
        leg,
        out: value_of(args, "--out").map(PathBuf::from),
        keep: args.iter().any(|arg| arg == "--keep-populations"),
        ssd,
        side_suffix: value_of(args, "--side-suffix").unwrap_or_default(),
    };
    if command == "facts" {
        return;
    }
    // the measurements asked for, every one for `all` and `quick`
    let wanted: Vec<String> = match command {
        "all" | "quick" => list_of(args, "--only").unwrap_or_else(|| ALL.iter().map(|name| (*name).to_string()).collect()),
        one => vec![one.to_string()],
    };
    // a name nobody knows is a mistake, never a run that measures nothing
    for name in &wanted {
        assert!(
            ALL.iter().chain(EXTRA).any(|known| known == name),
            "no measurement is named {name}; the names are {} and {}",
            ALL.join(", "),
            EXTRA.join(", ")
        );
    }
    for round in ctx.rounds.clone() {
        // the probes' figures, a record a round, so a report can read the idle sync, named by
        // the write cache they ran under
        let probe = if ctx.facts.write_cache == "write through" && ctx.rotational() { "probe-wt" } else { "probe" };
        ctx.emit(&[ctx.record("probes", round, SideOut::new("idle", probe, &ctx.probes.figures))]);
        for name in ALL.iter().chain(EXTRA).filter(|name| wanted.iter().any(|want| want == *name)) {
            let started = Instant::now();
            match *name {
                "chunk" => chunk::run(&ctx, round),
                "chunk-recycle" => chunk::run_recycle(&ctx, round),
                "journal" => journal::run(&ctx, round),
                "partial" => partial::run(&ctx, round),
                "remove" => remove::run(&ctx, round),
                "listing" => listing::run(&ctx, round),
                "read" => read::run(&ctx, round),
                "frag" => frag::run(&ctx, round),
                "slices" => slices::run(&ctx, round),
                "seq" => seq::run(&ctx, round),
                "sync" => sync::run(&ctx, round, ""),
                "contend" => contend::run(&ctx, round),
                "scrub" => scrub::run(&ctx, round),
                "shared" => shared::run(&ctx, round),
                "wcoff" => run_wcoff(&ctx, round),
                "listing-1m" => listing::run_million(&ctx, round),
                _ => unreachable!("every name in ALL and EXTRA is matched"),
            }
            eprintln!("device: {name} round {round} took {:.0} s", started.elapsed().as_secs_f64());
        }
    }
    // a quick run leaves nothing behind
    if quick && !ctx.keep {
        io::wipe(&ctx.dir);
    }
}
