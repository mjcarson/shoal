//! Speed for one core: every candidate at every unit, cold and hot, one call over a unit and the
//! unit fed in pieces of 4 KiB, measured `runs` times with the candidates interleaved inside each
//! cell so that drift falls on all of them. Then what one combine costs

use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use crate::buffers::{AlignedBuf, SplitMix64};
use crate::record::{CombineCell, SpeedCell};
use crate::sums::{self, CrcMath, Digest, Splits, Sum};

/// What a speed pass measures
pub struct Plan {
    /// The units
    pub units: Vec<usize>,
    /// Measurements a cell
    pub runs: usize,
    /// The least time one measurement runs for
    pub budget: Duration,
    /// The lengths of a second part a combine is timed at
    pub combine_lens: Vec<u64>,
}

impl Plan {
    /// The full plan X5 asks for, or the quick one that proves every adapter runs
    ///
    /// # Arguments
    ///
    /// * `quick` - Whether this is the quick pass
    pub fn new(quick: bool) -> Self {
        // the quick pass runs two units once, briefly
        if quick {
            return Plan {
                units: vec![4096, 64 * 1024],
                runs: 1,
                budget: Duration::from_millis(30),
                combine_lens: vec![64 * 1024],
            };
        }
        // X5's units, 4 KiB to 1 MiB, each cell three times for at least 200 ms
        Plan {
            units: vec![4096, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024],
            runs: 3,
            budget: Duration::from_millis(200),
            combine_lens: vec![4096, 64 * 1024, 1024 * 1024],
        }
    }
}

/// What one measurement found
struct Timed {
    /// Calls made
    calls: u64,
    /// Time they took
    elapsed: Duration,
}

/// Call `f` on units in turn, after one warm-up call, until the budget has passed
///
/// The clock is read once a batch, so that reading it is not a tenth of a 4 KiB call.
///
/// # Arguments
///
/// * `rows` - How many units there are to take in turn
/// * `batch` - Calls between two readings of the clock
/// * `budget` - The least time to run for
/// * `f` - One call on one unit
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

/// The cells measured so far, by candidate, unit, operation and temperature
#[derive(Default)]
struct Cells {
    /// The cells in the order first measured
    cells: Vec<SpeedCell>,
    /// Where each key is in `cells`
    index: HashMap<(String, usize, String, bool), usize>,
}

impl Cells {
    /// Record one measurement, counted by `unit` bytes a call
    ///
    /// # Arguments
    ///
    /// * `sum` - The candidate
    /// * `unit` - The unit
    /// * `op` - The operation
    /// * `hot` - Whether one unit was used for every call
    /// * `timed` - The measurement
    fn record(&mut self, sum: &str, unit: usize, op: &str, hot: bool, timed: Timed) {
        // find the cell, or make it
        let key = (sum.to_string(), unit, op.to_string(), hot);
        let next = self.cells.len();
        let at = *self.index.entry(key).or_insert(next);
        if at == next {
            self.cells.push(SpeedCell {
                sum: sum.to_string(),
                unit,
                op: op.to_string(),
                hot,
                ..SpeedCell::default()
            });
        }
        let cell = &mut self.cells[at];
        // bytes a second over the whole measurement, and the time of one call
        let secs = timed.elapsed.as_secs_f64();
        cell.gib_per_sec
            .push(unit as f64 * timed.calls as f64 / secs / (1u64 << 30) as f64);
        cell.us_per_call.push(secs * 1e6 / timed.calls as f64);
        cell.calls.push(timed.calls);
    }
}

/// Measure one candidate at one unit: one call over a unit, and the unit fed in pieces of 4 KiB
///
/// # Arguments
///
/// * `sum` - The candidate
/// * `unit` - The unit
/// * `arena` - The cold arena units are taken from in turn
/// * `budget` - The least time a measurement runs for
/// * `hot` - Whether to use one unit for every call, so the data stays in cache
/// * `cells` - Where the results go
fn measure(
    sum: &dyn Sum,
    unit: usize,
    arena: &AlignedBuf,
    budget: Duration,
    hot: bool,
    cells: &mut Cells,
) {
    // cold takes every unit the arena holds in turn; hot is the first one over and over
    let rows = if hot { 1 } else { arena.len() / unit };
    // the clock is read about every 256 KiB of work
    let batch = (256 * 1024 / unit).max(1) as u64;
    let at = |row: usize| &arena[row * unit..(row + 1) * unit];
    // one call over the whole unit
    let timed = time(rows, batch, budget, |row| {
        black_box(sum.one_shot(black_box(at(row))));
    });
    cells.record(sum.name(), unit, "one-shot", hot, timed);
    // the same unit through the incremental interface, as it would arrive in pieces
    let splits = Splits::Every(4096);
    if sum.stream(at(0), &splits).is_some() {
        let timed = time(rows, batch, budget, |row| {
            black_box(sum.stream(black_box(at(row)), &splits));
        });
        cells.record(sum.name(), unit, "stream-4k", hot, timed);
    }
}

/// Run the plan, printing a line as each cell finishes
///
/// # Arguments
///
/// * `plan` - What to measure
/// * `arena` - The cold arena, larger than any cache
pub fn run(plan: &Plan, arena: &AlignedBuf) -> Vec<SpeedCell> {
    let mut cells = Cells::default();
    let sums = sums::all();
    for run in 0..plan.runs {
        // cold first, then hot, at every unit
        for hot in [false, true] {
            for &unit in &plan.units {
                let started = Instant::now();
                // the candidates interleaved inside the cell
                for sum in &sums {
                    measure(sum.as_ref(), unit, arena, plan.budget, hot, &mut cells);
                }
                eprintln!(
                    "speed run {}/{} {} {:>5} KiB in {:.1}s",
                    run + 1,
                    plan.runs,
                    if hot { "hot " } else { "cold" },
                    unit / 1024,
                    started.elapsed().as_secs_f64()
                );
            }
        }
    }
    cells.cells
}

/// What one combine costs, for every candidate that combines, at every length of a second part
///
/// # Arguments
///
/// * `plan` - The runs, budget and lengths to use
pub fn combine(plan: &Plan) -> Vec<CombineCell> {
    let mut out: Vec<CombineCell> = Vec::new();
    let sums = sums::all();
    // a ring of seeded digests to combine, so no two calls see the same inputs
    let mut rng = SplitMix64::new(0xc0b1);
    let ring: Vec<[u8; 8]> = (0..1024).map(|_| rng.next_u64().to_le_bytes()).collect();
    for _ in 0..plan.runs {
        for &len_b in &plan.combine_lens {
            for sum in &sums {
                // a candidate that does not combine is left out
                let width = (sum.bits() / 8) as usize;
                let digest = |i: usize| Digest::from_bytes(&ring[i % ring.len()][..width.min(8)]);
                if width > 8 || sum.combine(digest(0), digest(1), len_b).is_none() {
                    continue;
                }
                // a chain of combines, each fed the last one's output
                let mut acc = digest(0);
                let timed = time(ring.len(), 1024, plan.budget, |i| {
                    acc = sum
                        .combine(black_box(acc), digest(i), black_box(len_b))
                        .expect("it combined once");
                });
                black_box(acc);
                // nanoseconds a combine
                let ns = timed.elapsed.as_secs_f64() * 1e9 / timed.calls as f64;
                push(&mut out, sum.name(), len_b, ns);
            }
            // the harness's combine for both CRCs: the multiplier made each call, and made once
            for (name, math) in [
                ("crc32c", CrcMath::crc32c()),
                ("crc64nvme", CrcMath::crc64nvme()),
            ] {
                let mut acc = ring[0].len() as u64;
                let timed = time(ring.len(), 1024, plan.budget, |i| {
                    let b = u64::from_le_bytes(ring[i % ring.len()]);
                    acc = math.combine(black_box(acc), b, black_box(len_b));
                });
                black_box(acc);
                let ns = timed.elapsed.as_secs_f64() * 1e9 / timed.calls as f64;
                push(&mut out, &format!("harness {name}"), len_b, ns);
                let op = math.operator(len_b);
                let timed = time(ring.len(), 1024, plan.budget, |i| {
                    let b = u64::from_le_bytes(ring[i % ring.len()]);
                    acc = math.combine_op(black_box(acc), b, black_box(op));
                });
                black_box(acc);
                let ns = timed.elapsed.as_secs_f64() * 1e9 / timed.calls as f64;
                push(&mut out, &format!("harness {name}, fixed length"), len_b, ns);
            }
        }
    }
    out
}

/// Add one measurement of a combine to its cell, made if it is new
///
/// # Arguments
///
/// * `out` - The cells
/// * `sum` - The candidate, or the harness's combine
/// * `len_b` - The second part's length
/// * `ns` - Nanoseconds a combine
fn push(out: &mut Vec<CombineCell>, sum: &str, len_b: u64, ns: f64) {
    match out
        .iter_mut()
        .find(|cell| cell.sum == sum && cell.len_b == len_b)
    {
        Some(cell) => cell.ns_per_combine.push(ns),
        None => out.push(CombineCell {
            sum: sum.to_string(),
            len_b,
            ns_per_combine: vec![ns],
        }),
    }
}
