//! Sections 1 and 3a: making bytes on one core, and on several at once with no wire
//!
//! Each generator fills units of 4 KiB, 64 KiB and 1 MiB, alone, then with the unit's CRC-64/NVME
//! taken as a driver takes it before a unit goes on the wire, then checking a unit that came back
//! by making it again and comparing. *Cold* takes units in turn from an arena of 256 MiB, larger
//! than any cache the lab has; *hot* takes one unit over and over, which is what a driver's frame
//! buffer is. A CRC alone and a copy are measured beside them, the two costs every generator is
//! read against. The timing is X5's: one warm-up call, then whole batches between readings of the
//! clock until the budget is spent, three runs with every side of a cell interleaved.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use crc_fast::CrcAlgorithm;

use super::generate::Generator;
use super::Ctx;
use crate::device::sys::Aligned;
use crate::device::{ordered, size_name, SideOut};

/// The arena units are taken from in turn when cold
pub const ARENA: usize = 256 << 20;

/// The units every generator is measured at
pub const UNITS: &[usize] = &[4096, 64 << 10, 1 << 20];

/// What one measurement found
struct Timed {
    /// Calls made
    calls: u64,
    /// The time they took
    elapsed: Duration,
}

/// Call `f` on rows in turn, after one warm-up call, until the budget has passed
///
/// The clock is read once a batch, so that reading it is not a tenth of a 4 KiB call.
///
/// # Arguments
///
/// * `rows` - How many rows there are to take in turn
/// * `batch` - Calls between two readings of the clock
/// * `budget` - The least time to run for
/// * `f` - One call on one row
fn time<F: FnMut(usize)>(rows: usize, batch: u64, budget: Duration, mut f: F) -> Timed {
    // one call first, so a table built on first use is not timed
    f(0);
    let mut row = 1 % rows;
    let mut calls = 0;
    let start = Instant::now();
    // whole batches until the budget is spent
    loop {
        for _ in 0..batch {
            f(row);
            row += 1;
            if row == rows {
                row = 0;
            }
        }
        calls += batch;
        if start.elapsed() >= budget {
            break;
        }
    }
    Timed {
        calls,
        elapsed: start.elapsed(),
    }
}

/// The CRC-64/NVME of a unit, as the driver takes it before the unit goes on the wire
///
/// # Arguments
///
/// * `bytes` - The unit
#[must_use]
pub fn crc(bytes: &[u8]) -> u64 {
    crc_fast::checksum(CrcAlgorithm::Crc64Nvme, bytes)
}

/// What a side does to one unit
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    /// Make it
    Fill,
    /// Make it and take its CRC
    FillCrc,
    /// Make it again and compare it with what came back
    Verify,
}

impl Op {
    /// The operation's name in a table
    fn name(self) -> &'static str {
        match self {
            Op::Fill => "fill",
            Op::FillCrc => "fill+crc",
            Op::Verify => "verify",
        }
    }
}

/// The memory a round's measurements share
struct Buffers {
    /// Where cold units are made
    arena: Aligned,
    /// Object zero of each generator over the arena's length, which a cold verify compares with
    correct: Vec<Aligned>,
    /// Where hot units are made
    hot: Aligned,
    /// Object zero's first unit of each generator, which a hot verify compares with
    hot_correct: Vec<Aligned>,
}

/// Run section one: every generator at every unit, cold and hot, then the references
///
/// # Arguments
///
/// * `ctx` - What the run is run with
/// * `generators` - The generators
/// * `round` - The round, which orders the sides
#[must_use]
pub fn section(ctx: &Ctx, generators: &[Arc<dyn Generator>], round: u32) -> Vec<SideOut> {
    // every buffer made once, each generator's correct bytes among them
    let largest = *UNITS.last().expect("a unit");
    let mut buffers = Buffers {
        arena: Aligned::new(ARENA),
        correct: Vec::new(),
        hot: Aligned::new(largest),
        hot_correct: Vec::new(),
    };
    for generator in generators {
        let mut correct = Aligned::new(ARENA);
        generator.fill(0, 0, correct.as_mut_slice());
        buffers.correct.push(correct);
        let mut hot = Aligned::new(largest);
        generator.fill(0, 0, hot.as_mut_slice());
        buffers.hot_correct.push(hot);
    }
    let mut outs = Vec::new();
    for &unit in UNITS {
        for cold in [true, false] {
            // each operation a cell, its sides the generators
            for op in [Op::Fill, Op::FillCrc, Op::Verify] {
                let cell = format!("{} {} {}", op.name(), size_name(unit as u64), temp(cold));
                let sides: Vec<usize> = (0..generators.len()).collect();
                let mut runs: Vec<Vec<f64>> = vec![Vec::new(); generators.len()];
                for _ in 0..ctx.runs() {
                    for &at in &ordered(&sides, round) {
                        let gib = measure(ctx, &mut buffers, generators, at, op, unit, cold);
                        runs[at].push(gib);
                    }
                }
                for (at, generator) in generators.iter().enumerate() {
                    outs.push(side_out(&cell, generator.name(), unit, &runs[at]));
                }
            }
            // the two costs every generator is read against
            outs.extend(references(ctx, &mut buffers, unit, cold, round));
        }
    }
    outs
}

/// "cold" or "hot"
///
/// # Arguments
///
/// * `cold` - Whether units are taken in turn from the arena
fn temp(cold: bool) -> &'static str {
    if cold {
        "cold"
    } else {
        "hot"
    }
}

/// A side's figures from its runs: the median rate, its range, and the time of one call
///
/// # Arguments
///
/// * `cell` - The cell
/// * `side` - The side
/// * `unit` - The unit, for the time of a call
/// * `runs` - GiB a second, one a run
fn side_out(cell: &str, side: &str, unit: usize, runs: &[f64]) -> SideOut {
    let mut sorted = runs.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = sorted[sorted.len() / 2];
    let us = unit as f64 / (median * f64::from(1u32 << 30)) * 1e6;
    SideOut::new(
        cell,
        side,
        &[
            ("gib_s", median),
            ("gib_s_min", sorted[0]),
            ("gib_s_max", sorted[sorted.len() - 1]),
            ("us_call", us),
        ],
    )
}

/// Time one generator at one operation, unit and temperature, in GiB a second
///
/// # Arguments
///
/// * `ctx` - What the run is run with
/// * `buffers` - The round's memory
/// * `generators` - The generators
/// * `at` - Which generator
/// * `op` - What each call does
/// * `unit` - The unit
/// * `cold` - Whether units are taken in turn from the arena
fn measure(
    ctx: &Ctx,
    buffers: &mut Buffers,
    generators: &[Arc<dyn Generator>],
    at: usize,
    op: Op,
    unit: usize,
    cold: bool,
) -> f64 {
    let generator = &generators[at];
    let rows = if cold { ARENA / unit } else { 1 };
    let batch = ((256 << 10) / unit).max(1) as u64;
    let mut sink = 0u64;
    let mut mismatches = 0u64;
    let timed = {
        let Buffers {
            arena,
            correct,
            hot,
            hot_correct,
        } = buffers;
        let (arena, hot) = (arena.as_mut_slice(), &mut hot.as_mut_slice()[..unit]);
        let (correct, hot_correct) = (correct[at].as_mut_slice(), &hot_correct[at].as_mut_slice()[..unit]);
        time(rows, batch, ctx.budget(), |row| {
            // the unit this call makes: a row of the arena, or the one hot unit
            let offset = (row * unit) as u64;
            match (op, cold) {
                (Op::Fill, true) => generator.fill(0, offset, &mut arena[row * unit..(row + 1) * unit]),
                (Op::Fill, false) => generator.fill(0, 0, hot),
                (Op::FillCrc, true) => {
                    let out = &mut arena[row * unit..(row + 1) * unit];
                    generator.fill(0, offset, out);
                    sink ^= crc(out);
                }
                (Op::FillCrc, false) => {
                    generator.fill(0, 0, hot);
                    sink ^= crc(hot);
                }
                // made into the hot unit, compared with the bytes that came back
                (Op::Verify, true) => {
                    generator.fill(0, offset, hot);
                    mismatches += u64::from(hot[..] != correct[row * unit..(row + 1) * unit]);
                }
                (Op::Verify, false) => {
                    generator.fill(0, 0, hot);
                    mismatches += u64::from(hot[..] != hot_correct[..]);
                }
            }
        })
    };
    black_box(sink);
    assert_eq!(mismatches, 0, "{} made other bytes the second time", generator.name());
    unit as f64 * timed.calls as f64 / timed.elapsed.as_secs_f64() / f64::from(1u32 << 30)
}

/// The two references at one unit and temperature: a CRC alone, and a copy
///
/// # Arguments
///
/// * `ctx` - What the run is run with
/// * `buffers` - The round's memory
/// * `unit` - The unit
/// * `cold` - Whether units are taken in turn from the arena
/// * `round` - The round, which orders the sides
fn references(ctx: &Ctx, buffers: &mut Buffers, unit: usize, cold: bool, round: u32) -> Vec<SideOut> {
    let rows = if cold { ARENA / unit } else { 1 };
    let batch = ((256 << 10) / unit).max(1) as u64;
    let mut crc_runs = Vec::new();
    let mut copy_runs = Vec::new();
    for _ in 0..ctx.runs() {
        for side in ordered(&["crc", "copy"], round) {
            let Buffers {
                arena,
                correct,
                hot,
                ..
            } = &mut *buffers;
            let (arena, source, hot) = (arena.as_mut_slice(), correct[0].as_mut_slice(), &mut hot.as_mut_slice()[..unit]);
            let mut sink = 0u64;
            let timed = if side == "crc" {
                // the checksum of bytes already made
                time(rows, batch, ctx.budget(), |row| {
                    let bytes = if cold { &arena[row * unit..(row + 1) * unit] } else { &hot[..] };
                    sink ^= crc(bytes);
                })
            } else {
                // a unit copied from bytes already made, which is all `stamped` does
                time(rows, batch, ctx.budget(), |row| {
                    let from = &source[row * unit..(row + 1) * unit];
                    if cold {
                        arena[row * unit..(row + 1) * unit].copy_from_slice(from);
                    } else {
                        hot.copy_from_slice(&source[..unit]);
                    }
                })
            };
            black_box(sink);
            let gib = unit as f64 * timed.calls as f64 / timed.elapsed.as_secs_f64() / f64::from(1u32 << 30);
            if side == "crc" {
                crc_runs.push(gib);
            } else {
                copy_runs.push(gib);
            }
        }
    }
    let size = size_name(unit as u64);
    vec![
        side_out(&format!("crc {size} {}", temp(cold)), "crc64nvme", unit, &crc_runs),
        side_out(&format!("copy {size} {}", temp(cold)), "memcpy", unit, &copy_runs),
    ]
}

/// Run section 3a: several cores making and checksumming at once, with no wire
///
/// Each thread is pinned to a core of its own and takes 1 MiB units in turn from an arena of its
/// own, so the cores share nothing but memory. What the total does as cores are added is what a
/// driver of several cores can make, before any socket.
///
/// # Arguments
///
/// * `ctx` - What the run is run with
/// * `generators` - The generators
/// * `round` - The round, which orders the sides
#[must_use]
pub fn cores_section(ctx: &Ctx, generators: &[Arc<dyn Generator>], round: u32) -> Vec<SideOut> {
    let mut outs = Vec::new();
    for &count in &ctx.make_counts {
        let cell = format!("fill+crc 1M cold x{count}");
        for at in ordered(&(0..generators.len()).collect::<Vec<_>>(), round) {
            let (total, least) = together(ctx, &generators[at], count);
            outs.push(SideOut::new(
                cell.clone(),
                generators[at].name(),
                &[("gib_s_total", total), ("gib_s_core_min", least), ("cores", count as f64)],
            ));
        }
    }
    outs
}

/// Run one generator on several pinned threads at once, for the cores section's window
///
/// # Arguments
///
/// * `ctx` - What the run is run with
/// * `generator` - The generator
/// * `count` - How many cores
fn together(ctx: &Ctx, generator: &Arc<dyn Generator>, count: usize) -> (f64, f64) {
    let unit = 1 << 20;
    let start = Arc::new(Barrier::new(count + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let threads: Vec<_> = ctx.make_cores[..count]
        .iter()
        .map(|&cpu| {
            let (generator, start, stop) = (generator.clone(), start.clone(), stop.clone());
            std::thread::spawn(move || {
                // pinned, its arena made and written once, then the window
                crate::placement::timing::pin(cpu);
                let mut arena = Aligned::new(ARENA);
                let arena = arena.as_mut_slice();
                generator.fill(0, 0, arena);
                let made = AtomicU64::new(0);
                let mut sink = 0u64;
                start.wait();
                let begun = Instant::now();
                let mut row = 0;
                while !stop.load(Ordering::Relaxed) {
                    let out = &mut arena[row * unit..(row + 1) * unit];
                    generator.fill(cpu as u64, (row * unit) as u64, out);
                    sink ^= crc(out);
                    made.fetch_add(unit as u64, Ordering::Relaxed);
                    row = (row + 1) % (ARENA / unit);
                }
                black_box(sink);
                made.load(Ordering::Relaxed) as f64 / begun.elapsed().as_secs_f64() / f64::from(1u32 << 30)
            })
        })
        .collect();
    // every thread ready, then the window, then the stop
    start.wait();
    std::thread::sleep(ctx.window());
    stop.store(true, Ordering::Relaxed);
    let rates: Vec<f64> = threads
        .into_iter()
        .map(|thread| thread.join().expect("a making thread finishes"))
        .collect();
    let least = rates.iter().copied().fold(f64::INFINITY, f64::min);
    (rates.iter().sum(), least)
}
