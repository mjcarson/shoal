//! Speed for one core: every candidate at every layout and unit, every operation, measured `runs`
//! times with the candidates interleaved inside each cell so that drift falls on all of them

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::buffers::{Arenas, Cut};
use crate::codes::{self, Code, Layout};
use crate::record::SpeedCell;

/// What a speed pass measures
pub struct Plan {
    /// The layouts every candidate runs at
    pub layouts: Vec<Layout>,
    /// The layouts every candidate also runs at hot, with one row in cache
    pub hot_layouts: Vec<Layout>,
    /// Layouts only the named candidates run at: the k+1 reference rows
    pub reference: Vec<(Layout, Vec<&'static str>)>,
    /// The units
    pub units: Vec<usize>,
    /// Measurements a cell
    pub runs: usize,
    /// The least time one measurement runs for
    pub budget: Duration,
}

impl Plan {
    /// The full plan X4 asks for, or the quick one that proves every adapter runs
    ///
    /// # Arguments
    ///
    /// * `quick` - Whether this is the quick pass
    pub fn new(quick: bool) -> Self {
        if quick {
            return Plan {
                layouts: vec![Layout::new(2, 1), Layout::new(4, 2)],
                hot_layouts: vec![Layout::new(4, 2)],
                reference: vec![],
                units: vec![4096, 64 * 1024],
                runs: 1,
                budget: Duration::from_millis(30),
            };
        }
        // X4's five layouts and five units; XOR and its nearest field code at k+1 beside them
        let reference = [4, 6, 8, 10]
            .into_iter()
            .map(|k| (Layout::new(k, 1), vec!["xor", "rusty_erasure"]))
            .collect();
        Plan {
            layouts: crate::check::x4_layouts(),
            hot_layouts: vec![Layout::new(4, 2), Layout::new(10, 4)],
            reference,
            units: vec![4096, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024],
            runs: 3,
            budget: Duration::from_millis(200),
        }
    }
}

/// What one measurement found
struct Timed {
    /// Calls made
    calls: u64,
    /// Time they took
    elapsed: Duration,
    /// Calls that could not decode
    failures: u64,
}

/// Call `f` on rows in turn, after one warm-up call, until the budget and three calls have passed
///
/// # Arguments
///
/// * `rows` - How many rows there are
/// * `budget` - The least time to run for
/// * `f` - One call on one row; `Ok(false)` is a set that could not decode
fn time<F: FnMut(usize) -> Result<bool, String>>(
    rows: usize,
    budget: Duration,
    mut f: F,
) -> Result<Timed, String> {
    // one call first, so that a plan or table built on first use is not timed
    f(0)?;
    let mut row = 1 % rows;
    let mut calls = 0;
    let mut failures = 0;
    let start = Instant::now();
    // whole calls until both the budget and three calls are spent
    while calls < 3 || start.elapsed() < budget {
        if !f(row)? {
            failures += 1;
        }
        calls += 1;
        row = (row + 1) % rows;
    }
    Ok(Timed {
        calls,
        elapsed: start.elapsed(),
        failures,
    })
}

/// The cells measured so far, by candidate, layout, unit and operation
#[derive(Default)]
struct Cells {
    /// The cells in the order first measured
    cells: Vec<SpeedCell>,
    /// Where each key is in `cells`
    index: HashMap<(String, String, usize, String, bool), usize>,
}

impl Cells {
    /// The cell for a key, made if it is new
    ///
    /// # Arguments
    ///
    /// * `code` - The candidate
    /// * `layout` - The layout
    /// * `unit` - The unit
    /// * `op` - The operation
    /// * `hot` - Whether one row was used for every call
    fn get(
        &mut self,
        code: &str,
        layout: Layout,
        unit: usize,
        op: &str,
        hot: bool,
    ) -> &mut SpeedCell {
        let key = (
            code.to_string(),
            layout.to_string(),
            unit,
            op.to_string(),
            hot,
        );
        let next = self.cells.len();
        let at = *self.index.entry(key).or_insert(next);
        if at == next {
            self.cells.push(SpeedCell {
                code: code.to_string(),
                layout: layout.to_string(),
                unit,
                op: op.to_string(),
                hot,
                ..SpeedCell::default()
            });
        }
        &mut self.cells[at]
    }

    /// Record one measurement, counted by `bytes` a call
    ///
    /// # Arguments
    ///
    /// * `code` - The candidate
    /// * `layout` - The layout
    /// * `unit` - The unit
    /// * `op` - The operation
    /// * `hot` - Whether one row was used for every call
    /// * `bytes` - The bytes one call is counted by
    /// * `timed` - The measurement, or why there is none
    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        code: &str,
        layout: Layout,
        unit: usize,
        op: &str,
        hot: bool,
        bytes: usize,
        timed: Result<Timed, String>,
    ) {
        let cell = self.get(code, layout, unit, op, hot);
        match timed {
            Ok(timed) => {
                // bytes a second over the whole measurement, and the time of one call
                let secs = timed.elapsed.as_secs_f64();
                cell.gib_per_sec
                    .push(bytes as f64 * timed.calls as f64 / secs / (1u64 << 30) as f64);
                cell.us_per_call.push(secs * 1e6 / timed.calls as f64);
                cell.calls.push(timed.calls);
                cell.failures += timed.failures;
            }
            Err(note) => cell.note = Some(note),
        }
    }
}

/// Measure every operation of one candidate in one cell
///
/// # Arguments
///
/// * `code` - The candidate
/// * `layout` - The layout
/// * `unit` - The unit
/// * `arenas` - The arenas to cut rows from
/// * `budget` - The least time a measurement runs for
/// * `hot` - Whether to use one row for every call, so the data stays in cache
/// * `cells` - Where the results go
fn measure(
    code: &mut dyn Code,
    layout: Layout,
    unit: usize,
    arenas: &mut Arenas,
    budget: Duration,
    hot: bool,
    cells: &mut Cells,
) {
    let name = code.name();
    // a layout the candidate cannot run is noted against its encode, once
    if let Err(note) = code.supports(layout, unit) {
        cells.get(name, layout, unit, "encode", hot).note = Some(note);
        return;
    }
    let shape = match code.prepare(layout, unit) {
        Ok(shape) => shape,
        Err(note) => {
            cells.get(name, layout, unit, "encode", hot).note = Some(note);
            return;
        }
    };
    let mut cut: Cut = arenas.cut(layout.k, unit, shape.count, shape.len);
    // hot is the first row over and over
    if hot {
        cut.rows = 1;
    }
    let Arenas { data, stored, new } = arenas;
    // every row encoded once, untimed, so every row read later holds valid chunks
    for row in 0..cut.rows {
        let (units, mut chunks) = cut.row(data, stored, row);
        let units: Vec<&[u8]> = units.iter().map(|unit| &**unit).collect();
        if let Err(note) = code.encode(&units, &mut chunks) {
            cells.get(name, layout, unit, "encode", hot).note = Some(note);
            return;
        }
    }
    // encode, counted by the stripe data it covers
    let timed = time(cut.rows, budget, |row| {
        let (units, mut chunks) = cut.row(data, stored, row);
        let units: Vec<&[u8]> = units.iter().map(|unit| &**unit).collect();
        code.encode(&units, &mut chunks).map(|()| true)
    });
    cells.record(name, layout, unit, "encode", hot, layout.k * unit, timed);
    // decode with j chunks lost: data units first, the worst case for a systematic code. A code
    // that is not systematic decodes on every read, so it is measured with nothing lost too
    let first = if code.systematic() { 1 } else { 0 };
    for j in first..=layout.m {
        let lost: Vec<usize> = (0..j).collect();
        let timed = time(cut.rows, budget, |row| {
            let (mut units, mut chunks) = cut.row(data, stored, row);
            code.decode(&mut units, &mut chunks, &lost)
        });
        cells.record(
            name,
            layout,
            unit,
            &format!("decode-{j}"),
            hot,
            layout.k * unit,
            timed,
        );
    }
    // rebuild chunk zero from k others, counted by the chunk rebuilt
    let timed = time(cut.rows, budget, |row| {
        let (mut units, mut chunks) = cut.row(data, stored, row);
        code.rebuild(&mut units, &mut chunks, 0)
    });
    cells.record(name, layout, unit, "rebuild", hot, unit, timed);
    // update the whole of data unit zero, counted by the bytes changed; last, since it leaves
    // parity that no longer matches the data
    let new = &new[..unit];
    let timed = time(cut.rows, budget, |row| {
        let (units, mut chunks) = cut.row(data, stored, row);
        code.update(0, &units[0][..], new, &mut chunks, 0..unit)
            .map(|()| true)
    });
    cells.record(name, layout, unit, "update", hot, unit, timed);
}

/// The kernels pass: `rusty_erasure` forced to each kernel set this cpu has, so that what an
/// instruction set is worth is read on one host and not across two microarchitectures
///
/// # Arguments
///
/// * `plan` - The runs, units and budget to use; its layouts are replaced by 4+2 and 10+4
/// * `arenas` - The arenas to cut rows from
pub fn kernels(plan: &Plan, arenas: &mut Arenas) -> Vec<SpeedCell> {
    let mut cells = Cells::default();
    // every kernel set this cpu has
    let mut codes: Vec<codes::rusty::Rusty> = codes::rusty::KERNEL_SETS
        .iter()
        .filter_map(|&set| codes::rusty::Rusty::forced(set))
        .collect();
    for run in 0..plan.runs {
        // cold and hot, at both layouts
        for hot in [false, true] {
            for layout in [Layout::new(4, 2), Layout::new(10, 4)] {
                for &unit in &plan.units {
                    let started = Instant::now();
                    // the kernel sets interleaved inside the cell
                    for code in codes.iter_mut() {
                        measure(code, layout, unit, arenas, plan.budget, hot, &mut cells);
                    }
                    eprintln!(
                        "kernels run {}/{} {} {:>5} {:>5} KiB in {:.1}s",
                        run + 1,
                        plan.runs,
                        if hot { "hot " } else { "cold" },
                        layout.to_string(),
                        unit / 1024,
                        started.elapsed().as_secs_f64()
                    );
                }
            }
        }
    }
    cells.cells
}

/// Run the plan, printing a line as each cell finishes
///
/// # Arguments
///
/// * `plan` - What to measure
/// * `arenas` - The arenas to cut rows from
pub fn run(plan: &Plan, arenas: &mut Arenas) -> Vec<SpeedCell> {
    let mut cells = Cells::default();
    let mut codes = codes::all();
    // every layout with the candidates that run it, cold; then the hot layouts with all of them
    let mut layouts: Vec<(Layout, Option<Vec<&'static str>>, bool)> = plan
        .layouts
        .iter()
        .map(|&layout| (layout, None, false))
        .collect();
    layouts.extend(
        plan.reference
            .iter()
            .map(|(layout, only)| (*layout, Some(only.clone()), false)),
    );
    layouts.extend(plan.hot_layouts.iter().map(|&layout| (layout, None, true)));
    for run in 0..plan.runs {
        for (layout, only, hot) in &layouts {
            for &unit in &plan.units {
                let started = Instant::now();
                // the candidates interleaved inside the cell
                for code in codes.iter_mut() {
                    if only
                        .as_ref()
                        .is_some_and(|only| !only.contains(&code.name()))
                    {
                        continue;
                    }
                    measure(
                        code.as_mut(),
                        *layout,
                        unit,
                        arenas,
                        plan.budget,
                        *hot,
                        &mut cells,
                    );
                }
                eprintln!(
                    "speed run {}/{} {} {:>5} {:>5} KiB in {:.1}s",
                    run + 1,
                    plan.runs,
                    if *hot { "hot " } else { "cold" },
                    layout.to_string(),
                    unit / 1024,
                    started.elapsed().as_secs_f64()
                );
            }
        }
    }
    cells.cells
}
