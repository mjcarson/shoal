//! X12: a rebuild's rate under a budget, and what the foreground pays for it
//!
//! S10 rebuilds a stripe chunk by reading `k` current chunks, computing the lost one and writing
//! it whole, and holds that work to bytes a second for each device, as a source and as a
//! destination. A device's two roles are measured apart, because they cost a foreground
//! differently and are budgeted apart:
//!
//! - `role=dest`: the device only takes rebuilt chunks. The survivors are already in memory, a
//!   ring of eight stripes standing in for chunks that arrived over the network; each is verified
//!   as a destination verifies what it receives, the lost chunk decoded, checksummed and checked
//!   against the one it replaces, and written whole as S6 writes a chunk. Judged.
//! - `role=local`: one device does the whole of the spike's pipeline, reading `k` chunks of a
//!   stripe, computing one and writing it, for a copy, a 2+1 and a 4+2. Reported: in a pool every
//!   device is a source of some rebuilds and a destination of others, never both of one.
//!
//! The source role is a deep scrub's reads, which `deep` measures. Each side runs one of four
//! paces beside the foreground: none, a fixed budget, idle time (with and without a ceiling), and
//! no bound. `granularity` asks Q17's question beside it: what one missed unit costs to rebuild
//! as a whole chunk, and as the unit alone.

use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use glommio::TaskQueueHandle;

use super::background::{self, DestPool, OutBuffers, Tally};
use super::counters::Devices;
use super::foreground::{self, Fg, Load, Places};
use super::io::{self, HEADER};
use super::paced::{Gauge, Pace, Pacer, Window};
use super::stats::{fmt, Rng, Samples};
use super::stripes::{self, ChunkHeader, Codec, HeldChunk, Layout, Population, Steps, Units, CHUNK, UNIT, UNITS};
use super::table::Table;
use super::{on_core, ordered, Ctx, SideOut};

/// A rebuild's fixed budgets on a disk, MiB a second of device bytes
const DISK_BUDGETS: &[f64] = &[10.0, 20.0, 40.0, 80.0];

/// A rebuild's fixed budgets on an SSD
const SSD_BUDGETS: &[f64] = &[100.0, 200.0, 400.0, 800.0];

/// The ceiling idle pacing is also run under, on a disk
const DISK_CEILING: f64 = 40.0;

/// The ceiling idle pacing is also run under, on an SSD
const SSD_CEILING: f64 = 400.0;

/// Stripes the destination's ring holds in memory
const RING: usize = 8;

/// Objects in the destination's pool
pub const POOL_OBJECTS: usize = 32;

/// Positions an object of the pool holds: the widest layout's
pub const POOL_SLOTS: usize = 6;

/// A piece of a rebuild's I/O
pub const PIECE: u64 = 1 << 20;

/// A device's role in a rebuild
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// It takes rebuilt chunks, the survivors in memory
    Dest,
    /// It reads the survivors, and takes the rebuilt chunk
    Local,
}

impl Role {
    /// Its name in a cell
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Role::Dest => "dest",
            Role::Local => "local",
        }
    }
}

/// One side of the measurement
#[derive(Debug, Clone, Copy)]
struct Side {
    /// The device's role
    role: Role,
    /// The stripes rebuilt
    layout: Layout,
    /// The pace
    pace: Pace,
}

impl Side {
    /// Its cell
    fn cell(&self) -> String {
        format!("role={} layout={} piece={}", self.role.name(), self.layout.name(), super::size_name(PIECE))
    }
}

/// How deep a pace's I/O goes and how many rebuilds run at once
///
/// A fixed budget and idle pacing keep one rebuild and few pieces in flight, so a foreground waits
/// behind little; an SSD takes four pieces at a fixed budget, since one at a time cannot reach its
/// larger budgets. No bound keeps eight pieces of each of two rebuilds on a disk and four on an SSD.
///
/// # Arguments
///
/// * `pace` - The pace
/// * `rotational` - Whether the device spins
#[must_use]
pub fn shape(pace: Pace, rotational: bool) -> (usize, usize) {
    match (pace, rotational) {
        (Pace::Fixed(_), false) => (4, 1),
        (Pace::Fixed(_) | Pace::Idle(_) | Pace::None, _) => (1, 1),
        (Pace::Unbounded, true) => (8, 2),
        (Pace::Unbounded, false) => (8, 4),
    }
}

/// The sides a round runs
///
/// # Arguments
///
/// * `ctx` - The run
fn sides(ctx: &Ctx) -> Vec<Side> {
    let (budgets, ceiling) = if ctx.rotational() { (DISK_BUDGETS, DISK_CEILING) } else { (SSD_BUDGETS, SSD_CEILING) };
    let budgets: Vec<f64> = ctx.x12.rebuild_mib.clone().unwrap_or_else(|| budgets.to_vec());
    let mut sides = Vec::new();
    // the destination at every pace
    let dest = |pace| Side { role: Role::Dest, layout: Layout::Rs42, pace };
    if ctx.quick {
        sides.extend([Pace::None, Pace::Fixed(budgets[0]), Pace::Idle(None), Pace::Unbounded].map(dest));
    } else {
        sides.push(dest(Pace::None));
        sides.extend(budgets.iter().map(|&mib| dest(Pace::Fixed(mib))));
        sides.extend([Pace::Idle(None), Pace::Idle(Some(ceiling)), Pace::Unbounded].map(dest));
    }
    // the whole pipeline on one device, for a copy and for a decode
    for layout in [Layout::Copy, Layout::Rs21, Layout::Rs42] {
        let paces: &[Pace] = if ctx.quick { &[Pace::Unbounded] } else { &[Pace::Idle(None), Pace::Unbounded] };
        sides.extend(paces.iter().map(|&pace| Side { role: Role::Local, layout, pace }));
    }
    // a run that names its paces or its roles keeps those
    sides.retain(|side| {
        ctx.x12.runs(&side.pace.name()) && ctx.x12.roles.as_ref().is_none_or(|roles| roles.iter().any(|role| role == side.role.name()))
    });
    sides
}

/// Everything a round's sides share, held open on the slice's executor
pub struct Rig {
    /// The foreground
    pub fg: Fg,
    /// The 4+2 population, which copies are read from too
    pub rs42: Population,
    /// The 2+1 population
    pub rs21: Population,
    /// The destination's ring: every chunk of eight 4+2 stripes, in memory
    pub ring: Vec<Vec<HeldChunk>>,
    /// The pool rebuilt chunks are written into
    pub pool: DestPool,
    /// A codec for each layout
    pub codecs: Vec<Codec>,
    /// The background's task queue
    pub queue: TaskQueueHandle,
    /// The devices counted
    pub devices: Devices,
    /// Whether the device spins
    pub rotational: bool,
    /// The background queue's latency goal, microseconds
    pub goal_us: u64,
}

impl Rig {
    /// The population a layout is read from
    ///
    /// # Arguments
    ///
    /// * `layout` - The layout
    #[must_use]
    pub fn population(&self, layout: Layout) -> &Population {
        match layout.population() {
            Layout::Rs21 => &self.rs21,
            _ => &self.rs42,
        }
    }

    /// The codec of a layout
    ///
    /// # Arguments
    ///
    /// * `layout` - The layout
    #[must_use]
    pub fn codec(&self, layout: Layout) -> &Codec {
        self.codecs.iter().find(|codec| codec.layout == layout).expect("a codec for every layout")
    }
}

/// What one rebuild stream did
#[derive(Debug, Default)]
pub struct Rebuilt {
    /// Chunks rebuilt whose write ended in the window
    pub chunks: usize,
    /// Chunks whose rebuilt bytes differed from the chunk they replace
    pub mismatches: usize,
    /// Survivor units that failed their checksum, or survivors a source refused
    pub source_failures: usize,
    /// Where the cpu went
    pub steps: Steps,
    /// Each counted chunk's time, from its first read or decode to its directory's sync
    pub took: Samples,
    /// Bytes checksummed: survivors and rebuilt chunks
    pub checked: u64,
    /// Bytes rebuilt, counted or not
    pub made: u64,
}

/// Rebuild one chunk from survivors, check it against the chunk it replaces and write it whole
///
/// # Arguments
///
/// * `rig` - The round's rig
/// * `codec` - The layout's codec
/// * `sources` - The survivors, in the order the codec names them
/// * `lost` - The position lost
/// * `expect` - The lost chunk's checksums
/// * `stripe` - The stripe
/// * `object` - The pool's object it is written into
/// * `out` - The output's buffers
/// * `depth` - Pieces in flight
/// * `pacer` - The side's pacer
/// * `tally` - Where counted bytes go
/// * `window` - The side's window
/// * `seen` - What the stream did
#[allow(clippy::too_many_arguments)]
pub async fn rebuild_one<C: Units>(
    rig: &Rig,
    codec: &Codec,
    sources: &[&C],
    lost: usize,
    expect: &[u64; UNITS],
    stripe: usize,
    object: usize,
    mut out: OutBuffers,
    depth: usize,
    pacer: &mut Pacer,
    tally: &Tally,
    window: Window,
    seen: &mut Rebuilt,
) -> OutBuffers {
    // every survivor verified, as a destination verifies what it receives
    for (source, &position) in sources.iter().zip(&codec.survivors(lost)) {
        let crcs = &rig.population(codec.layout).tables[stripe][position];
        seen.source_failures += stripes::verify(*source, crcs, None, &mut seen.steps).await as usize;
        seen.checked += CHUNK;
    }
    // the lost chunk, then its checksums into its header
    codec.rebuild(sources, lost, &mut out.pieces, out.piece, &mut seen.steps).await;
    let began = Instant::now();
    let mut crcs = [0_u64; UNITS];
    for (unit, crc) in crcs.iter_mut().enumerate() {
        *crc = stripes::crc(out.unit(unit));
        if unit % 8 == 7 {
            glommio::yield_if_needed().await;
        }
    }
    seen.steps.crc_ns += began.elapsed().as_nanos() as u64;
    seen.checked += CHUNK;
    seen.made += CHUNK;
    if &crcs != expect {
        seen.mismatches += 1;
    }
    ChunkHeader { layout: codec.layout, stripe: stripe as u64, position: lost as u8, crcs }.encode(out.header.as_bytes_mut());
    // written whole into the pool, as S6 writes a chunk
    let (header, pieces, piece) = out.lend();
    rig.pool.write(object, lost % POOL_SLOTS, header.clone(), &pieces, depth, pacer, tally, window).await;
    OutBuffers::back(header, pieces, piece)
}

/// One rebuild stream as a destination: survivors from the ring, rebuilt chunks to the pool
///
/// # Arguments
///
/// * `rig` - The round's rig
/// * `pace` - The pace
/// * `stream` - This stream's number
/// * `streams` - Streams running
/// * `window` - The side's window
/// * `tally` - Where counted bytes go
async fn dest_stream(rig: Rc<Rig>, pace: Pace, stream: usize, streams: usize, window: Window, tally: Rc<Tally>) -> Rebuilt {
    let (depth, _) = shape(pace, rig.rotational);
    let mut pacer = Pacer::new(pace, PIECE, rig.fg.gauge.clone());
    let codec = rig.codec(Layout::Rs42);
    let width = Layout::Rs42.width();
    let mut out = OutBuffers::new(PIECE);
    let mut seen = Rebuilt::default();
    super::paced::until(window.start).await;
    let mut nth = stream;
    while Instant::now() < window.end {
        // a ring stripe and a position, every position of every stripe in turn
        let held = &rig.ring[nth % rig.ring.len()];
        let lost = (nth / rig.ring.len()) % width;
        let stripe = held[0].header.stripe as usize;
        let sources: Vec<&HeldChunk> = codec.survivors(lost).iter().map(|&position| &held[position]).collect();
        let began = Instant::now();
        out = rebuild_one(&rig, codec, &sources, lost, &held[lost].header.crcs, stripe, nth % POOL_OBJECTS, out, depth, &mut pacer, &tally, window, &mut seen).await;
        let ended = Instant::now();
        if window.counts(began, ended) {
            seen.chunks += 1;
            seen.took.push(ended - began);
        }
        nth += streams;
    }
    seen
}

/// One rebuild stream on one device: survivors read from the population, rebuilt chunks to the pool
///
/// # Arguments
///
/// * `rig` - The round's rig
/// * `layout` - The stripes rebuilt
/// * `pace` - The pace
/// * `stream` - This stream's number
/// * `streams` - Streams running
/// * `window` - The side's window
/// * `tally` - Where counted bytes go
/// * `start` - The placement group the walk starts at
async fn local_stream(rig: Rc<Rig>, layout: Layout, pace: Pace, stream: usize, streams: usize, window: Window, tally: Rc<Tally>, start: usize) -> Rebuilt {
    let (depth, _) = shape(pace, rig.rotational);
    let mut pacer = Pacer::new(pace, PIECE, rig.fg.gauge.clone());
    let codec = rig.codec(layout);
    let population = rig.population(layout);
    let walk = population.walk(start, false);
    let mut out = OutBuffers::new(PIECE);
    let mut seen = Rebuilt::default();
    super::paced::until(window.start).await;
    let mut nth = stream;
    'walk: while Instant::now() < window.end {
        let stripe = walk[nth % walk.len()];
        // the lost position turns with the stripe; a copy loses a data chunk
        let lost = stripe % if layout == Layout::Copy { Layout::Rs42.k() } else { layout.width() };
        let began = Instant::now();
        // the survivors read whole, one after another, as this device's share of the reads
        let mut sources = Vec::with_capacity(layout.k());
        for position in codec.survivors(lost) {
            let file = &population.files[stripe][position];
            match background::read_chunk(file, PIECE, depth, &mut pacer, &tally, window).await {
                Some(chunk) => sources.push(chunk),
                None => break 'walk,
            }
        }
        let sources: Vec<&stripes::ReadChunk> = sources.iter().collect();
        let expect = population.tables[stripe][lost];
        out = rebuild_one(&rig, codec, &sources, lost, &expect, stripe, nth % POOL_OBJECTS, out, depth, &mut pacer, &tally, window, &mut seen).await;
        let ended = Instant::now();
        if window.counts(began, ended) {
            seen.chunks += 1;
            seen.took.push(ended - began);
        }
        nth += streams;
    }
    seen
}

/// The seed of a side's walk and of the round's arrivals
///
/// # Arguments
///
/// * `round` - The round
/// * `index` - The side's place in the declared order
fn seed(round: u32, index: usize) -> u64 {
    Rng::new(0x12 ^ (u64::from(round) << 32) ^ index as u64).next()
}

/// Run one side
///
/// # Arguments
///
/// * `rig` - The round's rig
/// * `side` - The side
/// * `window` - Its window
/// * `round` - The round
/// * `index` - The side's place in the declared order
async fn side(rig: Rc<Rig>, side: Side, window: Window, round: u32, index: usize) -> SideOut {
    let edges = background::edges(rig.devices.clone(), window);
    // the foreground's arrivals are the round's, the same for every side
    let running = rig.fg.start(window, seed(round, 0));
    let tally = Rc::new(Tally::default());
    let (depth, streams) = shape(side.pace, rig.rotational);
    let start = (seed(round, index) % stripes::PGS as u64) as usize;
    let queue = rig.queue;
    // the rebuild's streams, in the background's queue
    let tasks: Vec<_> = if side.pace == Pace::None {
        Vec::new()
    } else {
        (0..streams)
            .map(|stream| {
                let (rig, tally) = (rig.clone(), tally.clone());
                let work = async move {
                    match side.role {
                        Role::Dest => dest_stream(rig, side.pace, stream, streams, window, tally).await,
                        Role::Local => local_stream(rig, side.layout, side.pace, stream, streams, window, tally, start).await,
                    }
                };
                glommio::spawn_local_into(work, queue).expect("the background's queue")
            })
            .collect()
    };
    let seen = running.finish().await;
    let streams_seen = join_all(tasks).await;
    let (delta, cpu_ns) = edges.await;
    // the streams' figures together
    let mut rebuilt = Rebuilt::default();
    for one in &streams_seen {
        rebuilt.chunks += one.chunks;
        rebuilt.mismatches += one.mismatches;
        rebuilt.source_failures += one.source_failures;
        rebuilt.steps.add(&one.steps);
        rebuilt.took.extend(&one.took);
        rebuilt.checked += one.checked;
        rebuilt.made += one.made;
    }
    let secs = window.secs();
    let mib = |bytes: u64| bytes as f64 / secs / f64::from(1 << 20);
    let background_mib = mib(tally.read.get() + tally.written.get());
    let rebuilt_mib = mib(tally.rebuilt.get());
    // cpu per MiB processed, over every chunk the streams touched, counted or not
    let per_mib = |ns: u64, bytes: u64| if bytes == 0 { 0.0 } else { ns as f64 / 1e3 / (bytes as f64 / f64::from(1 << 20)) };
    let took = rebuilt.took.summary();
    let mut figures: Vec<(String, f64)> = vec![
        ("budget_mib_s".into(), side.pace.budget()),
        ("rebuilt_mib_s".into(), rebuilt_mib),
        ("bg_read_mib_s".into(), mib(tally.read.get())),
        ("bg_write_mib_s".into(), mib(tally.written.get())),
        ("achieved_ratio".into(), if let Pace::Fixed(budget) = side.pace { background_mib / budget } else { 0.0 }),
        ("chunks".into(), rebuilt.chunks as f64),
        ("mismatches".into(), rebuilt.mismatches as f64),
        ("source_failures".into(), rebuilt.source_failures as f64),
        ("crc_us_mib".into(), per_mib(rebuilt.steps.crc_ns, rebuilt.checked)),
        ("decode_us_mib".into(), per_mib(rebuilt.steps.decode_ns, rebuilt.made)),
        ("exec_busy".into(), background::busy(cpu_ns, &window)),
        ("chunk_p50".into(), took.p50),
        ("chunk_p99".into(), took.tail()),
        ("busy".into(), delta.busy()),
        ("flushes_s".into(), delta.flushes as f64 / delta.secs.max(1e-9)),
        ("dev_read_mib_s".into(), delta.read as f64 / delta.secs.max(1e-9) / f64::from(1 << 20)),
        ("dev_write_mib_s".into(), delta.written as f64 / delta.secs.max(1e-9) / f64::from(1 << 20)),
        ("depth".into(), depth as f64),
        ("streams".into(), if side.pace == Pace::None { 0.0 } else { streams as f64 }),
        ("rotational".into(), if rig.rotational { 1.0 } else { 0.0 }),
        ("journal_apart".into(), if rig.fg.journal_apart { 1.0 } else { 0.0 }),
        ("goal_us".into(), rig.goal_us as f64),
    ];
    figures.extend(foreground::figures(&seen));
    let figures: Vec<(&str, f64)> = figures.iter().map(|(name, value)| (name.as_str(), *value)).collect();
    SideOut::new(side.cell(), side.pace.name(), &figures)
}

/// Where a round's populations, pool and foreground are
#[derive(Debug, Clone)]
pub struct Paths {
    /// Every X12 population's directory
    pub base: PathBuf,
    /// The destination's pool
    pub pool: PathBuf,
    /// The foreground's files
    pub places: Places,
    /// Stripes in the 4+2 population
    pub rs42: usize,
    /// Stripes in the 2+1 population
    pub rs21: usize,
}

impl Paths {
    /// A run's paths
    ///
    /// # Arguments
    ///
    /// * `ctx` - The run
    #[must_use]
    pub fn of(ctx: &Ctx) -> Paths {
        Paths {
            base: ctx.sub("x12"),
            pool: ctx.sub("x12-pool"),
            places: Places::of(ctx),
            rs42: Layout::Rs42.stripes(ctx.quick),
            rs21: Layout::Rs21.stripes(ctx.quick),
        }
    }

    /// Make every population that is not there, outside anything timed
    ///
    /// # Arguments
    ///
    /// * `ctx` - The run
    pub fn populate(&self, ctx: &Ctx) {
        self.places.populate(ctx);
        let paths = self.clone();
        on_core(ctx.core, ctx.sibling, move || async move {
            stripes::populate(stripes::population_dir(&paths.base, Layout::Rs42), Layout::Rs42, paths.rs42).await;
            stripes::populate(stripes::population_dir(&paths.base, Layout::Rs21), Layout::Rs21, paths.rs21).await;
        });
    }
}

/// Run the rebuild measurement for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    super::require_write_through(ctx);
    let paths = Paths::of(ctx);
    paths.populate(ctx);
    let sides = sides(ctx);
    let devices = ctx.facts.devices.clone();
    let (rotational, form) = (ctx.rotational(), ctx.dir_sync());
    let goal_us = ctx.x12.goal_us;
    let windows = (ctx.window(Duration::from_secs(3)), ctx.window(ctx.x12.counted()));
    let span_bytes = ctx.facts.fs_bytes;
    let (outs, span, kernels) = on_core(ctx.core, ctx.sibling, move || async move {
        let fg = Fg::open(&paths.places, Load::for_device(rotational)).await;
        let span = fg.arm.span(span_bytes);
        let rs42 = Population::open(&stripes::population_dir(&paths.base, Layout::Rs42), Layout::Rs42, paths.rs42).await;
        let rs21 = Population::open(&stripes::population_dir(&paths.base, Layout::Rs21), Layout::Rs21, paths.rs21).await;
        // the destination's ring: eight clean stripes held whole
        let mut ring = Vec::with_capacity(RING);
        for stripe in rs42.walk(0, false).into_iter().take(RING) {
            let mut held = Vec::with_capacity(Layout::Rs42.width());
            for position in 0..Layout::Rs42.width() {
                held.push(rs42.hold(stripe, position).await);
            }
            ring.push(held);
        }
        let pool = DestPool::make(&paths.pool, POOL_OBJECTS, POOL_SLOTS, form).await;
        let codecs = vec![Codec::new(Layout::Copy), Codec::new(Layout::Rs21), Codec::new(Layout::Rs42)];
        let kernels = codecs[2].kernels();
        let rig = Rc::new(Rig { fg, rs42, rs21, ring, pool, codecs, queue: background::queue(goal_us), devices, rotational, goal_us });
        let mut outs = Vec::new();
        let declared: Vec<(usize, Side)> = sides.into_iter().enumerate().collect();
        for (index, one) in ordered(&declared, round) {
            let window = Window::new(windows.0, windows.1);
            outs.push(side(rig.clone(), one, window, round, index).await);
            super::settle(&paths.pool).await;
        }
        // every file closed before the executor ends
        if let Ok(rig) = Rc::try_unwrap(rig) {
            rig.fg.close().await;
            rig.rs42.close().await;
            rig.rs21.close().await;
        }
        io::wipe(&paths.pool);
        (outs, span, kernels)
    });
    let mut table = Table::new(&[
        "cell", "side", "rebuilt MiB/s", "bg read+write MiB/s", "achieved", "chunk p50 ms", "read p99 ms", "write p99 ms", "stage p99 ms", "lag p99 ms", "mismatches", "disk busy",
    ]);
    let ms = |out: &SideOut, name: &str| fmt(out.get(name) / 1e3);
    let mut records = Vec::new();
    for mut out in outs {
        table.row(vec![
            out.cell.clone(),
            out.side.clone(),
            fmt(out.get("rebuilt_mib_s")),
            fmt(out.get("bg_read_mib_s") + out.get("bg_write_mib_s")),
            fmt(out.get("achieved_ratio")),
            ms(&out, "chunk_p50"),
            ms(&out, "read_p99"),
            ms(&out, "write_p99"),
            ms(&out, "stage_p99"),
            ms(&out, "lag_p99"),
            fmt(out.get("mismatches")),
            fmt(out.get("busy")),
        ]);
        for (name, value) in span.figures() {
            out.metrics.insert(name.to_string(), value);
        }
        records.push(ctx.record("rebuild", round, out));
    }
    print!(
        "{}",
        table.render(
            &format!("X12 · A rebuild beside the foreground, round {round} (4 MiB chunks of 64 KiB units, pieces of 1 MiB; {kernels}; {})", span.show()),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}

/// One unit of a stripe rebuilt alone: the unit read from each survivor, verified, decoded, and
/// written in place into the chunk with its header, then synced, as a partial apply is
///
/// # Arguments
///
/// * `population` - The population
/// * `codec` - The layout's codec
/// * `pool` - The pool whose chunk it is written into
/// * `stripe` - The stripe
/// * `lost` - The position lost
/// * `unit` - The unit
/// * `object` - The pool's object
/// * `buffers` - A unit's and a header's buffers to write from
async fn rebuild_unit(
    population: &Population,
    codec: &Codec,
    pool: &DestPool,
    stripe: usize,
    lost: usize,
    unit: usize,
    object: usize,
    buffers: &mut Option<(glommio::io::DmaBuffer, glommio::io::DmaBuffer)>,
) -> bool {
    let survivors = codec.survivors(lost);
    // the unit from every survivor at once, as k devices would be read
    let reads = join_all(survivors.iter().map(|&position| {
        population.files[stripe][position].read_at_aligned(HEADER + unit as u64 * UNIT, UNIT as usize)
    }))
    .await;
    let reads: Vec<_> = reads.into_iter().map(|read| read.expect("a unit")).collect();
    // verified against the survivors' tables
    let mut sound = true;
    for (read, &position) in reads.iter().zip(&survivors) {
        sound &= stripes::crc(read) == population.tables[stripe][position][unit];
    }
    // decoded into a unit's buffer on the codec's kept plan, or copied for a copy
    let (mut target, header) = buffers.take().expect("buffers lent back");
    let units: Vec<&[u8]> = reads.iter().map(|read| &read[..]).collect();
    codec.decode_unit(&units, &survivors, lost, target.as_bytes_mut());
    let matched = stripes::crc(target.as_bytes()) == population.tables[stripe][lost][unit];
    // written in place with the header, then the chunk synced
    let file = io::open(&pool.current(object, lost % POOL_SLOTS), false).await;
    let (target, header) = (Rc::new(target), Rc::new(header));
    let (one, two) = futures::join!(file.write_rc_at(target.clone(), HEADER + unit as u64 * UNIT), file.write_rc_at(header.clone(), 0));
    one.expect("a unit lands");
    two.expect("a header lands");
    file.fdatasync().await.expect("synced");
    file.close().await.expect("closed");
    *buffers = Some((Rc::try_unwrap(target).ok().expect("written"), Rc::try_unwrap(header).ok().expect("written")));
    sound && matched
}

/// Run Q17's measurement for one round: what one missed unit costs to rebuild as its whole chunk,
/// and as the unit alone, one rebuild at a time and nothing beside it
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run_granularity(ctx: &Ctx, round: u32) {
    super::require_write_through(ctx);
    let paths = Paths::of(ctx);
    paths.populate(ctx);
    let devices = ctx.facts.devices.clone();
    let (rotational, form) = (ctx.rotational(), ctx.dir_sync());
    let windows = (ctx.window(Duration::from_secs(2)), ctx.window(Duration::from_secs(10)));
    let cells: Vec<(Layout, bool)> = [Layout::Rs21, Layout::Rs42].into_iter().flat_map(|layout| [(layout, false), (layout, true)]).collect();
    let outs = on_core(ctx.core, ctx.sibling, move || async move {
        let rs42 = Population::open(&stripes::population_dir(&paths.base, Layout::Rs42), Layout::Rs42, paths.rs42).await;
        let rs21 = Population::open(&stripes::population_dir(&paths.base, Layout::Rs21), Layout::Rs21, paths.rs21).await;
        let pool = DestPool::make(&paths.pool, POOL_OBJECTS, POOL_SLOTS, form).await;
        let gauge = Rc::new(Gauge::default());
        let mut outs = Vec::new();
        for (layout, unit_alone) in ordered(&cells, round) {
            let population = if layout == Layout::Rs21 { &rs21 } else { &rs42 };
            let codec = Codec::new(layout);
            let window = Window::new(windows.0, windows.1);
            let edges = background::edges(devices.clone(), window);
            let walk = population.walk((seed(round, 99) % stripes::PGS as u64) as usize, false);
            let mut took = Samples::default();
            let (mut count, mut wrong) = (0_usize, 0_usize);
            let mut rng = Rng::new(seed(round, usize::from(unit_alone)));
            let mut out = OutBuffers::new(PIECE);
            let mut unit_buffers = Some((glommio::allocate_dma_buffer(UNIT as usize), glommio::allocate_dma_buffer(HEADER as usize)));
            let mut pacer = Pacer::new(Pace::Unbounded, PIECE, gauge.clone());
            let tally = Tally::default();
            let mut steps = Steps::default();
            super::paced::until(window.start).await;
            let mut nth = 0;
            while Instant::now() < window.end {
                let stripe = walk[nth % walk.len()];
                let lost = stripe % layout.width();
                let object = nth % POOL_OBJECTS;
                let began = Instant::now();
                if unit_alone {
                    let unit = rng.below(UNITS as u64) as usize;
                    if !rebuild_unit(population, &codec, &pool, stripe, lost, unit, object, &mut unit_buffers).await {
                        wrong += 1;
                    }
                } else {
                    // the whole chunk: survivors read whole, decoded, written whole
                    let mut sources = Vec::new();
                    for position in codec.survivors(lost) {
                        let read = background::read_chunk(&population.files[stripe][position], PIECE, 8, &mut pacer, &tally, Window { end: window.end + Duration::from_secs(60), ..window }).await;
                        sources.push(read.expect("read whole"));
                    }
                    let refs: Vec<&stripes::ReadChunk> = sources.iter().collect();
                    // verified, as the unit's survivors are
                    for (source, &position) in refs.iter().zip(&codec.survivors(lost)) {
                        if stripes::verify(*source, &population.tables[stripe][position], None, &mut steps).await > 0 {
                            wrong += 1;
                        }
                    }
                    codec.rebuild(&refs, lost, &mut out.pieces, out.piece, &mut steps).await;
                    let crcs: Vec<u64> = (0..UNITS).map(|unit| stripes::crc(out.unit(unit))).collect();
                    if crcs.as_slice() != population.tables[stripe][lost].as_slice() {
                        wrong += 1;
                    }
                    let (header, pieces, piece) = out.lend();
                    pool.write(object, lost % POOL_SLOTS, header.clone(), &pieces, 8, &mut pacer, &tally, window).await;
                    out = OutBuffers::back(header, pieces, piece);
                }
                let ended = Instant::now();
                if window.counts(began, ended) {
                    count += 1;
                    took.push(ended - began);
                }
                nth += 1;
            }
            let (delta, _) = edges.await;
            let summary = took.summary();
            let figures = [
                ("rebuilds_s", count as f64 / window.secs()),
                ("us_p50", summary.p50),
                ("us_p99", summary.tail()),
                ("dev_kib", delta.kib_per(count)),
                ("dev_read_kib", delta.read as f64 / 1024.0 / count.max(1) as f64),
                ("flushes", delta.flushes_per(count)),
                ("wrong", wrong as f64),
                ("rotational", if rotational { 1.0 } else { 0.0 }),
            ];
            let cell = format!("layout={}", layout.name());
            outs.push(SideOut::new(cell, if unit_alone { "unit" } else { "chunk" }, &figures));
            super::settle(&paths.pool).await;
        }
        rs42.close().await;
        rs21.close().await;
        io::wipe(&paths.pool);
        outs
    });
    let mut table = Table::new(&["cell", "side", "rebuilds/s", "p50 ms", "p99 ms", "device KiB each", "device read KiB each", "flushes each", "wrong"]);
    let mut records = Vec::new();
    for out in outs {
        table.row(vec![
            out.cell.clone(),
            out.side.clone(),
            fmt(out.get("rebuilds_s")),
            fmt(out.get("us_p50") / 1e3),
            fmt(out.get("us_p99") / 1e3),
            fmt(out.get("dev_kib")),
            fmt(out.get("dev_read_kib")),
            fmt(out.get("flushes")),
            fmt(out.get("wrong")),
        ]);
        records.push(ctx.record("granularity", round, out));
    }
    print!(
        "{}",
        table.render(&format!("X12 · Q17: one missed unit rebuilt as its chunk and alone, round {round} (one at a time, nothing beside it)"), &ctx.label())
    );
    ctx.emit(&records);
}
