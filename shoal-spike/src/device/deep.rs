//! X12: a deep scrub's rate under a budget, and what the foreground pays for it
//!
//! S11's deep scrub reads every unit of every chunk on the slice that holds it and verifies its
//! checksum, and X14 found Ceph's checks nothing between an overwritable pool's chunks, so S11
//! adds a check of its own: each holder folds its chunk's units into one unit's worth of summary,
//! and the data chunks' summaries, encoded, must equal the parity chunks'. This runs both on the
//! 4+2 population, a stripe at a time, placement group by placement group, beside the foreground,
//! at the same four paces as a rebuild. Its reads are also a rebuild's source role.
//!
//! Every planted fault the walk reaches must be found and nothing else may be: a planted parity
//! fault by the summary check alone, a planted data fault by its unit's checksum. A side counts
//! what it found, what it missed and what it reported that was not planted. On an SSD a `cpu`
//! side runs the same verification over stripes held in memory beside the foreground, so what the
//! foreground pays for a scrub's cpu is told apart from what it pays for its reads. Idle pacing is
//! also run with pieces of 256 KiB and 4 MiB, since a disk's foreground waits behind at most one.

use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use glommio::TaskQueueHandle;

use super::background::{self, Tally};
use super::counters::Devices;
use super::foreground::{self, Fg, Load};
use super::paced::{Pace, Pacer, Window};
use super::rebuild::{shape, Paths};
use super::stats::{fmt, Rng};
use super::stripes::{self, planted, ChunkHeader, Codec, Fault, HeldChunk, Layout, Population, Steps, Units, CHUNK, UNIT};
use super::table::Table;
use super::{on_core, ordered, Ctx, SideOut};

/// A deep scrub's fixed budgets on a disk, MiB a second
const DISK_BUDGETS: &[f64] = &[5.0, 10.0, 20.0, 40.0];

/// A deep scrub's fixed budgets on an SSD
const SSD_BUDGETS: &[f64] = &[50.0, 100.0, 200.0, 400.0];

/// The ceiling idle pacing is also run under, on a disk
const DISK_CEILING: f64 = 20.0;

/// The ceiling idle pacing is also run under, on an SSD
const SSD_CEILING: f64 = 200.0;

/// The piece a scrub reads in, unless a side says otherwise
const PIECE: u64 = 1 << 20;

/// The pieces idle pacing is also run with, KiB
const PIECES_KIB: &[u64] = &[256, 4096];

/// Stripes the `cpu` side holds in memory
const HELD: usize = 8;

/// The layout scrubbed
const LAYOUT: Layout = Layout::Rs42;

/// One side of the measurement
#[derive(Debug, Clone, Copy)]
struct Side {
    /// The pace, ignored by the `cpu` side
    pace: Pace,
    /// Bytes a piece
    piece: u64,
    /// Whether it scrubs stripes held in memory, with no reads
    cpu: bool,
}

impl Side {
    /// Its cell
    fn cell(&self) -> String {
        format!("layout={} piece={}", LAYOUT.name(), super::size_name(self.piece))
    }

    /// Its name
    fn name(&self) -> String {
        if self.cpu {
            "cpu".to_string()
        } else {
            self.pace.name()
        }
    }
}

/// The sides a round runs
///
/// # Arguments
///
/// * `ctx` - The run
fn sides(ctx: &Ctx) -> Vec<Side> {
    let (budgets, ceiling) = if ctx.rotational() { (DISK_BUDGETS, DISK_CEILING) } else { (SSD_BUDGETS, SSD_CEILING) };
    let budgets: Vec<f64> = ctx.x12.scrub_mib.clone().unwrap_or_else(|| budgets.to_vec());
    let pieces: Vec<u64> = ctx.x12.pieces_kib.clone().unwrap_or_else(|| PIECES_KIB.to_vec());
    let at = |pace| Side { pace, piece: PIECE, cpu: false };
    let mut sides = Vec::new();
    if ctx.quick {
        sides.extend([Pace::None, Pace::Fixed(budgets[0]), Pace::Idle(None), Pace::Unbounded].map(at));
        sides.push(Side { pace: Pace::Idle(None), piece: pieces[0] << 10, cpu: false });
    } else {
        sides.push(at(Pace::None));
        sides.extend(budgets.iter().map(|&mib| at(Pace::Fixed(mib))));
        sides.extend([Pace::Idle(None), Pace::Idle(Some(ceiling)), Pace::Unbounded].map(at));
        sides.extend(pieces.iter().map(|&kib| Side { pace: Pace::Idle(None), piece: kib << 10, cpu: false }));
    }
    // the scrub's cpu alone beside the foreground, where an SSD's reads are short enough for the
    // cpu to be what a foreground waits behind
    if !ctx.rotational() {
        sides.push(Side { pace: Pace::Unbounded, piece: PIECE, cpu: true });
    }
    // a run that names its paces keeps those, in pieces of 1 MiB
    if ctx.x12.paces.is_some() {
        sides.retain(|side| !side.cpu && side.piece == PIECE && ctx.x12.runs(&side.pace.name()));
    }
    sides
}

/// Everything a round's sides share, held open on the slice's executor
struct Rig {
    /// The foreground
    fg: Fg,
    /// The 4+2 population
    population: Population,
    /// Stripes held in memory for the `cpu` side, the planted two among them
    held: Vec<Vec<HeldChunk>>,
    /// The stripes held, by number
    held_stripes: Vec<usize>,
    /// The codec
    codec: Codec,
    /// The background's task queue
    queue: TaskQueueHandle,
    /// The devices counted
    devices: Devices,
    /// Whether the device spins
    rotational: bool,
    /// The background queue's latency goal, microseconds
    goal_us: u64,
}

/// What a scrub found and did
#[derive(Debug, Default)]
struct Scrubbed {
    /// Stripes whose every chunk was read and checked, counted when the last ended in the window
    stripes: usize,
    /// Units that failed their checksum
    crc_failures: usize,
    /// Stripes whose summaries did not match
    parity_mismatches: usize,
    /// Headers that were not the chunk's
    bad_headers: usize,
    /// Planted faults the scrub reached
    planted_checked: usize,
    /// Of those, found
    planted_found: usize,
    /// Of those, missed
    undetected: usize,
    /// Findings on stripes with nothing planted
    false_alarms: usize,
    /// Where the cpu went
    steps: Steps,
    /// Bytes verified, counted or not
    verified: u64,
    /// Stripes summary-checked, counted or not
    summaries: usize,
}

/// Judge one stripe's findings against what was planted in it
///
/// # Arguments
///
/// * `seen` - The scrub's tally
/// * `fault` - What was planted, if anything
/// * `failed` - Units that failed their checksum, headers included
/// * `matched` - Whether the summaries matched, checked only when every unit passed
fn judge(seen: &mut Scrubbed, fault: Option<Fault>, failed: usize, matched: bool) {
    seen.crc_failures += failed;
    if !matched {
        seen.parity_mismatches += 1;
    }
    match fault {
        // a data fault is the checksum's to find
        Some(Fault::Data) => {
            seen.planted_checked += 1;
            if failed > 0 {
                seen.planted_found += 1;
            } else {
                seen.undetected += 1;
            }
        }
        // a parity fault passes every checksum and is the summary's to find
        Some(Fault::Parity) => {
            seen.planted_checked += 1;
            if failed == 0 && !matched {
                seen.planted_found += 1;
            } else {
                seen.undetected += 1;
            }
            if failed > 0 {
                seen.false_alarms += 1;
            }
        }
        None => seen.false_alarms += usize::from(failed > 0) + usize::from(!matched),
    }
}

/// Check one stripe's chunks, read or held: every unit against its header, every chunk folded,
/// and the summaries checked when every unit passed
///
/// # Arguments
///
/// * `codec` - The codec
/// * `chunks` - The stripe's chunks, each with the header it was read with
/// * `stripe` - The stripe
/// * `stripes` - Stripes in the population, which says where the faults were planted
/// * `seen` - The scrub's tally
async fn check_stripe<C: Units>(codec: &Codec, chunks: &[(&C, Option<ChunkHeader>)], stripe: usize, stripes: usize, seen: &mut Scrubbed) -> (usize, bool) {
    let mut failed = 0;
    let mut summaries = vec![vec![0_u8; UNIT as usize]; chunks.len()];
    for (position, (chunk, header)) in chunks.iter().enumerate() {
        // a header that is not this chunk's fails every unit it describes
        let Some(header) = header.as_ref().filter(|header| header.stripe == stripe as u64 && usize::from(header.position) == position) else {
            seen.bad_headers += 1;
            failed += stripes::UNITS;
            continue;
        };
        failed += stripes::verify(*chunk, &header.crcs, Some(&mut summaries[position]), &mut seen.steps).await as usize;
        seen.verified += CHUNK;
    }
    // the summary check only among chunks that verified: a failed unit is already a finding
    let matched = if failed == 0 {
        let mut scratch = vec![vec![0_u8; UNIT as usize]; LAYOUT.m()];
        seen.summaries += 1;
        codec.summaries_match(&summaries, &mut scratch, &mut seen.steps)
    } else {
        true
    };
    judge(seen, planted(LAYOUT, stripes, stripe), failed, matched);
    (failed, matched)
}

/// A deep scrub reading from the device, a stripe at a time, until the window ends
///
/// # Arguments
///
/// * `rig` - The round's rig
/// * `side` - The side
/// * `window` - The side's window
/// * `tally` - Where counted bytes go
/// * `start` - The placement group the walk starts at
async fn scrub_device(rig: Rc<Rig>, side: Side, window: Window, tally: Rc<Tally>, start: usize) -> Scrubbed {
    let (depth, _) = shape(side.pace, rig.rotational);
    let mut pacer = Pacer::new(side.pace, side.piece, rig.fg.gauge.clone());
    let population = &rig.population;
    let walk = population.walk(start, true);
    let mut seen = Scrubbed::default();
    super::paced::until(window.start).await;
    let mut nth = 0;
    'walk: while Instant::now() < window.end {
        let stripe = walk[nth % walk.len()];
        // every chunk of the stripe read whole, as each holder reads its own
        let mut read = Vec::with_capacity(LAYOUT.width());
        for position in 0..LAYOUT.width() {
            match background::read_chunk(&population.files[stripe][position], side.piece, depth, &mut pacer, &tally, window).await {
                Some(chunk) => read.push(chunk),
                None => break 'walk,
            }
        }
        let chunks: Vec<(&stripes::ReadChunk, Option<ChunkHeader>)> = read.iter().map(|chunk| (chunk, ChunkHeader::decode(&chunk.header))).collect();
        check_stripe(&rig.codec, &chunks, stripe, population.stripes(), &mut seen).await;
        if Instant::now() <= window.end {
            seen.stripes += 1;
        }
        nth += 1;
    }
    seen
}

/// A deep scrub's verification over stripes held in memory, with no reads, until the window ends
///
/// # Arguments
///
/// * `rig` - The round's rig
/// * `window` - The side's window
/// * `tally` - Where counted bytes go
async fn scrub_held(rig: Rc<Rig>, window: Window, tally: Rc<Tally>) -> Scrubbed {
    let mut seen = Scrubbed::default();
    super::paced::until(window.start).await;
    let mut nth = 0;
    while Instant::now() < window.end {
        let at = nth % rig.held.len();
        let chunks: Vec<(&HeldChunk, Option<ChunkHeader>)> = rig.held[at].iter().map(|chunk| (chunk, Some(chunk.header.clone()))).collect();
        let began = Instant::now();
        check_stripe(&rig.codec, &chunks, rig.held_stripes[at], rig.population.stripes(), &mut seen).await;
        // counted as the bytes a device scrub would have read
        if window.counts(began, Instant::now()) {
            seen.stripes += 1;
            tally.read.set(tally.read.get() + CHUNK * LAYOUT.width() as u64);
        }
        nth += 1;
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
    Rng::new(0xdee9 ^ (u64::from(round) << 32) ^ index as u64).next()
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
    let running = rig.fg.start(window, seed(round, 0));
    let tally = Rc::new(Tally::default());
    let start = (seed(round, index) % stripes::PGS as u64) as usize;
    let queue = rig.queue;
    // the scrub, in the background's queue
    let task = (side.pace != Pace::None).then(|| {
        let (rig, tally) = (rig.clone(), tally.clone());
        let work = async move {
            if side.cpu {
                scrub_held(rig, window, tally).await
            } else {
                scrub_device(rig, side, window, tally, start).await
            }
        };
        glommio::spawn_local_into(work, queue).expect("the background's queue")
    });
    let seen = running.finish().await;
    let scrubbed = match task {
        Some(task) => join_all([task]).await.pop().expect("one"),
        None => Scrubbed::default(),
    };
    let (delta, cpu_ns) = edges.await;
    let secs = window.secs();
    let scrub_mib = tally.read.get() as f64 / secs / f64::from(1 << 20);
    let per_mib = |ns: u64| if scrubbed.verified == 0 { 0.0 } else { ns as f64 / 1e3 / (scrubbed.verified as f64 / f64::from(1 << 20)) };
    let (depth, _) = shape(side.pace, rig.rotational);
    let mut figures: Vec<(String, f64)> = vec![
        ("budget_mib_s".into(), side.pace.budget()),
        ("scrub_mib_s".into(), scrub_mib),
        ("achieved_ratio".into(), if let Pace::Fixed(budget) = side.pace { scrub_mib / budget } else { 0.0 }),
        ("stripes_checked".into(), scrubbed.stripes as f64),
        ("crc_failures".into(), scrubbed.crc_failures as f64),
        ("parity_mismatches".into(), scrubbed.parity_mismatches as f64),
        ("bad_headers".into(), scrubbed.bad_headers as f64),
        ("planted_checked".into(), scrubbed.planted_checked as f64),
        ("planted_found".into(), scrubbed.planted_found as f64),
        ("undetected".into(), scrubbed.undetected as f64),
        ("false_alarms".into(), scrubbed.false_alarms as f64),
        ("crc_us_mib".into(), per_mib(scrubbed.steps.crc_ns)),
        ("fold_us_mib".into(), per_mib(scrubbed.steps.fold_ns)),
        ("summary_us_stripe".into(), scrubbed.steps.summary_ns as f64 / 1e3 / scrubbed.summaries.max(1) as f64),
        ("exec_busy".into(), background::busy(cpu_ns, &window)),
        ("busy".into(), delta.busy()),
        ("flushes_s".into(), delta.flushes as f64 / delta.secs.max(1e-9)),
        ("dev_read_mib_s".into(), delta.read as f64 / delta.secs.max(1e-9) / f64::from(1 << 20)),
        ("piece_kib".into(), (side.piece >> 10) as f64),
        ("depth".into(), if side.pace == Pace::None { 0.0 } else { depth as f64 }),
        ("rotational".into(), if rig.rotational { 1.0 } else { 0.0 }),
        ("journal_apart".into(), if rig.fg.journal_apart { 1.0 } else { 0.0 }),
        ("goal_us".into(), rig.goal_us as f64),
    ];
    figures.extend(foreground::figures(&seen));
    let figures: Vec<(&str, f64)> = figures.iter().map(|(name, value)| (name.as_str(), *value)).collect();
    SideOut::new(side.cell(), side.name(), &figures)
}

/// Run the deep scrub measurement for one round
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
    let rotational = ctx.rotational();
    let goal_us = ctx.x12.goal_us;
    let windows = (ctx.window(Duration::from_secs(3)), ctx.window(ctx.x12.counted()));
    let span_bytes = ctx.facts.fs_bytes;
    let (outs, span, kernels) = on_core(ctx.core, ctx.sibling, move || async move {
        let fg = Fg::open(&paths.places, Load::for_device(rotational)).await;
        let span = fg.arm.span(span_bytes);
        let population = Population::open(&stripes::population_dir(&paths.base, LAYOUT), LAYOUT, paths.rs42).await;
        // the stripes the cpu side holds: the planted two first, then clean ones
        let stripes = population.stripes();
        let mut held_stripes: Vec<usize> = (0..stripes).filter(|&stripe| planted(LAYOUT, stripes, stripe).is_some()).collect();
        held_stripes.extend(population.walk(0, false).into_iter().take(HELD - held_stripes.len()));
        let mut held = Vec::new();
        if !rotational {
            for &stripe in &held_stripes {
                let mut chunks = Vec::with_capacity(LAYOUT.width());
                for position in 0..LAYOUT.width() {
                    chunks.push(population.hold(stripe, position).await);
                }
                held.push(chunks);
            }
        }
        let codec = Codec::new(LAYOUT);
        let kernels = codec.kernels();
        let rig = Rc::new(Rig { fg, population, held, held_stripes, codec, queue: background::queue(goal_us), devices, rotational, goal_us });
        let mut outs = Vec::new();
        let declared: Vec<(usize, Side)> = sides.into_iter().enumerate().collect();
        for (index, one) in ordered(&declared, round) {
            let window = Window::new(windows.0, windows.1);
            outs.push(side(rig.clone(), one, window, round, index).await);
            super::settle(&paths.base).await;
        }
        if let Ok(rig) = Rc::try_unwrap(rig) {
            rig.fg.close().await;
            rig.population.close().await;
        }
        (outs, span, kernels)
    });
    let mut table = Table::new(&[
        "cell", "side", "scrub MiB/s", "achieved", "stripes", "planted found/checked", "false alarms", "read p99 ms", "write p99 ms", "lag p99 ms", "crc µs/MiB", "fold µs/MiB", "disk busy",
    ]);
    let ms = |out: &SideOut, name: &str| fmt(out.get(name) / 1e3);
    let mut records = Vec::new();
    for mut out in outs {
        table.row(vec![
            out.cell.clone(),
            out.side.clone(),
            fmt(out.get("scrub_mib_s")),
            fmt(out.get("achieved_ratio")),
            fmt(out.get("stripes_checked")),
            format!("{}/{}", out.get("planted_found"), out.get("planted_checked")),
            fmt(out.get("false_alarms")),
            ms(&out, "read_p99"),
            ms(&out, "write_p99"),
            ms(&out, "lag_p99"),
            fmt(out.get("crc_us_mib")),
            fmt(out.get("fold_us_mib")),
            fmt(out.get("busy")),
        ]);
        for (name, value) in span.figures() {
            out.metrics.insert(name.to_string(), value);
        }
        records.push(ctx.record("deep", round, out));
    }
    print!(
        "{}",
        table.render(
            &format!("X12 · A deep scrub beside the foreground, round {round} (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by {kernels}; {})", span.show()),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}
