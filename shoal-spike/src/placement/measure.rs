//! What X2 records of each candidate over each shape: fill, movement, feasibility, exceptions
//!
//! Everything here is a function of the seeded shapes and placement groups, so it prints the
//! same tables on every host. Placement of a pool's groups is split across threads; nothing is
//! timed.
//!
//! - **Fill**: each device's chunks over its weight, the fullest over the mean, and the
//!   coefficient of variation across the pool's devices. One consumer, every placement group
//!   holding the same bytes, which is a large bucket's case; a small one is the next table
//! - **A small bucket**: the same fill when each placement group holds a Poisson number of
//!   stripes, so the imbalance a bucket of few stripes brings is told apart from placement's
//! - **Movement**: for each change, the chunks whose slice changed (a changed position counts,
//!   for an erasure coded pool, and only a changed set for a replicated one), over the least that
//!   change could move: the chunks the departing devices held, or the chunks a new device ends
//!   up with, or the chunks a reweighted device gave up. Beside the candidates, `rendezvous,
//!   positions kept` is rendezvous's set with positions held as state, the way a tablet group
//!   would hold them beside its generation: a member that stays keeps its position, so only the
//!   set's changes move
//! - **Feasibility**: answers that break the domain rule, which only the window gives, and the
//!   by-position candidates' fallbacks
//! - **Exceptions**: what the rendezvous answer needs to be within 5%, 2% and 1%
//! - **Fitted weights**: the bias weighted rendezvous has when it picks several devices of
//!   unequal weight is systematic, so it can be corrected in the map with one number a device,
//!   as Ceph's balancer does in its `crush-compat` mode. The weights are fitted to one consumer
//!   and judged on another, so what they correct is the bias and not the first consumer's luck
//! - **The logarithm**: how many choices libm's `ln` would make differently from the table's

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use super::candidates::{pgs, Candidate, Pg, Scratch, View, MAX_WIDTH};
use super::score::SplitMix;
use super::shape::{Change, Pool, Shape, MAX_SLICES};
use super::table::Assignment;

/// The consumer every simulated placement group belongs to
const CONSUMER: u64 = 0x5eed_0001;

/// The margins the exceptions are counted to, loosest first
const MARGINS: [f64; 3] = [0.05, 0.02, 0.01];

/// The most exception moves tried for one pool before the balancer gives up
const EXCEPTION_CAP: u64 = 400_000;

/// Placement groups a tablet the movement tables are taken at
const MOVEMENT_PER_TABLET: u32 = 4;

/// A second consumer, whose groups judge weights fitted to the first's
const HOLDOUT: u64 = 0x5eed_0002;

/// Placement groups a tablet weights are fitted at
const FIT_PER_TABLET: u32 = 16;

/// Rounds of fitting
const FIT_ROUNDS: usize = 24;

/// Consumers in the sample the planner fits weights to
const FIT_CONSUMERS: u64 = 8;

/// What placing every group of a pool found
#[derive(Debug, Default, Clone, Copy)]
pub struct Totals {
    /// Answers that put two positions in one domain
    pub violations: u64,
    /// By-position answers that ran out of rounds
    pub fallbacks: u64,
    /// Rounds drawn, summed over the groups
    pub rounds: u64,
}

/// Every placement group's answer, as slice ids, `width` a group
pub struct Placed {
    /// The answers
    pub ids: Vec<u32>,
    /// What the lookups found
    pub totals: Totals,
}

/// Place every group with one candidate, across threads, checking every answer
///
/// # Arguments
///
/// * `view` - The pool over the map
/// * `candidate` - The candidate
/// * `pgs` - The placement groups
/// * `threads` - How many threads to split them over
///
/// # Panics
///
/// If any candidate but the window answers with two positions in one domain.
#[must_use]
pub fn place_all(view: &View, candidate: Candidate, pgs: &[Pg], threads: usize) -> Placed {
    let width = view.width;
    let mut ids = vec![0u32; pgs.len() * width];
    let index = view.index();
    let per_thread = pgs.len().div_ceil(threads.max(1));
    // each thread takes a run of groups and the run of answers that goes with it
    let totals: Vec<Totals> = std::thread::scope(|scope| {
        let handles: Vec<_> = ids
            .chunks_mut(per_thread * width)
            .zip(pgs.chunks(per_thread))
            .map(|(out, groups)| {
                let index = &index;
                scope.spawn(move || {
                    let mut scratch = Scratch::default();
                    let mut totals = Totals::default();
                    let mut domains = [0u32; MAX_WIDTH];
                    for (pg, answer) in groups.iter().zip(out.chunks_mut(width)) {
                        let flags = view.place(candidate, pg, &mut scratch, answer);
                        totals.fallbacks += u64::from(flags.fallback);
                        totals.rounds += u64::from(flags.rounds);
                        // read every answer back: distinct domains, or a counted violation
                        for (pos, id) in answer.iter().enumerate() {
                            let slice = index[id] as usize;
                            domains[pos] = view.slices[slice].domain;
                        }
                        let distinct = (1..width).all(|pos| !domains[..pos].contains(&domains[pos]));
                        if !distinct {
                            assert_eq!(
                                candidate,
                                Candidate::Window,
                                "{} broke the domain rule",
                                candidate.label()
                            );
                            totals.violations += 1;
                        }
                    }
                    totals
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("a placement thread finishes"))
            .collect()
    });
    // the threads' totals summed
    let totals = totals.iter().fold(Totals::default(), |sum, part| Totals {
        violations: sum.violations + part.violations,
        fallbacks: sum.fallbacks + part.fallbacks,
        rounds: sum.rounds + part.rounds,
    });
    Placed { ids, totals }
}

/// Chunks on each of a view's devices, from answers as slice ids
///
/// # Arguments
///
/// * `view` - The view
/// * `ids` - The answers
/// * `per_pg` - How many stripes each group holds, or `None` for one each
#[must_use]
pub fn device_loads(view: &View, ids: &[u32], per_pg: Option<&[u64]>) -> Vec<f64> {
    let index = view.index();
    let mut loads = vec![0.0; view.devices.len()];
    for (slot, id) in ids.iter().enumerate() {
        // a group's chunk carries as many stripes as the group holds
        let weight = per_pg.map_or(1.0, |stripes| stripes[slot / view.width] as f64);
        let device = view.slices[index[id] as usize].device as usize;
        loads[device] += weight;
    }
    loads
}

/// The fullest device over the mean, and the coefficient of variation, of loads over weights
///
/// # Arguments
///
/// * `view` - The view
/// * `loads` - Each device's load
#[must_use]
pub fn fill(view: &View, loads: &[f64]) -> (f64, f64) {
    // the pool's mean is its whole load over its whole weight
    let total: f64 = loads.iter().sum();
    let weight: f64 = view.devices.iter().map(|device| device.weight).sum();
    let mean = total / weight;
    let utils: Vec<f64> = loads
        .iter()
        .zip(&view.devices)
        .map(|(load, device)| load / device.weight / mean)
        .collect();
    let worst = utils.iter().copied().fold(0.0, f64::max);
    // the spread across devices, each counted once whatever its size
    let average = utils.iter().sum::<f64>() / utils.len() as f64;
    let variance = utils.iter().map(|u| (u - average).powi(2)).sum::<f64>() / utils.len() as f64;
    (worst, variance.sqrt())
}

/// Format a fill pair as the tables print it: the fullest over the mean, then the spread
///
/// # Arguments
///
/// * `worst` - The fullest over the mean
/// * `cv` - The coefficient of variation
fn fill_cell(worst: f64, cv: f64) -> String {
    format!("+{:.1}% / {:.1}%", (worst - 1.0) * 100.0, cv * 100.0)
}

/// The chunks a change moved: changed positions for an erasure coded pool, changed sets otherwise
///
/// # Arguments
///
/// * `old` - The answers before
/// * `new` - The answers after
/// * `width` - Positions a group
/// * `positional` - Whether a position change is a move
#[must_use]
pub fn moved(old: &[u32], new: &[u32], width: usize, positional: bool) -> u64 {
    if positional {
        return old.iter().zip(new).filter(|(a, b)| a != b).count() as u64;
    }
    // a replica that is in both sets moved nowhere, whatever position it is listed at
    old.chunks(width)
        .zip(new.chunks(width))
        .map(|(a, b)| b.iter().filter(|id| !a.contains(id)).count() as u64)
        .sum()
}

/// The least a change could move, read from what each side holds
///
/// # Arguments
///
/// * `before` - The view before
/// * `after` - The view after
/// * `old` - The answers before
/// * `new` - The answers after
#[must_use]
pub fn least(before: &View, after: &View, old: &[u32], new: &[u32]) -> u64 {
    let uids_before: HashMap<u32, f64> =
        before.devices.iter().map(|device| (device.uid, device.weight)).collect();
    let uids_after: HashMap<u32, f64> =
        after.devices.iter().map(|device| (device.uid, device.weight)).collect();
    let uid = |id: &u32| id / MAX_SLICES;
    // a device gone: every chunk it held has to be placed again
    let gone: HashSet<u32> = uids_before
        .keys()
        .filter(|u| !uids_after.contains_key(u))
        .copied()
        .collect();
    if !gone.is_empty() {
        return old.iter().filter(|id| gone.contains(&uid(id))).count() as u64;
    }
    // a device come: every chunk it ends up with was moved onto it
    let came: HashSet<u32> = uids_after
        .keys()
        .filter(|u| !uids_before.contains_key(u))
        .copied()
        .collect();
    if !came.is_empty() {
        return new.iter().filter(|id| came.contains(&uid(id))).count() as u64;
    }
    // a device reweighted: what it gave up or took on
    let changed: Vec<u32> = uids_after
        .iter()
        .filter(|(u, weight)| uids_before.get(u).is_some_and(|w| w != *weight))
        .map(|(u, _)| *u)
        .collect();
    changed
        .iter()
        .map(|device| {
            let held = old.iter().filter(|id| uid(id) == *device).count() as i64;
            let holds = new.iter().filter(|id| uid(id) == *device).count() as i64;
            (held - holds).unsigned_abs()
        })
        .sum()
}

/// A Poisson sample, by Knuth's method below thirty and the normal approximation above
///
/// # Arguments
///
/// * `rng` - The generator
/// * `lambda` - The mean
fn poisson(rng: &mut SplitMix, lambda: f64) -> u64 {
    if lambda < 30.0 {
        // multiply uniforms until the product falls below e^-lambda
        let limit = (-lambda).exp();
        let mut product = rng.next_f64();
        let mut count = 0;
        while product > limit {
            product *= rng.next_f64();
            count += 1;
        }
        return count;
    }
    // Box-Muller for one standard normal, then scaled
    let u1 = rng.next_f64().max(f64::MIN_POSITIVE);
    let u2 = rng.next_f64();
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    (lambda + lambda.sqrt() * z).round().max(0.0) as u64
}

/// Options for one simulation
pub struct Options {
    /// Placement groups a tablet the fill tables sweep
    pub per_tablet: Vec<u32>,
    /// Threads to place over
    pub threads: usize,
    /// Only the shapes whose names contain this
    pub only: Option<String>,
}

/// Run the simulation and return its tables as markdown
///
/// # Arguments
///
/// * `options` - What to run
#[must_use]
pub fn simulate(options: &Options) -> String {
    let mut out = String::new();
    let shapes: Vec<Shape> = Shape::all()
        .into_iter()
        .filter(|shape| options.only.as_ref().is_none_or(|only| shape.name.contains(only.as_str())))
        .collect();
    let _ = writeln!(out, "# X2 placement simulation\n");
    let _ = writeln!(
        out,
        "One consumer. Placement groups a tablet {:?}; 4096 tablets. Seeded: these tables are the \
         same on every host and every run.\n",
        options.per_tablet
    );
    out.push_str(&shapes_table(&shapes));
    out.push_str(&fill_tables(&shapes, options));
    out.push_str(&exception_tables(&shapes, options));
    out.push_str(&fitted_tables(&shapes, options));
    out.push_str(&movement_tables(&shapes, options));
    out.push_str(&feasibility_table(&shapes, options));
    out.push_str(&libm_table(&shapes, options));
    out.push_str(&small_bucket_table(&shapes, options));
    out
}

/// The shapes, what each stands for and how many domains each pool has
///
/// # Arguments
///
/// * `shapes` - The shapes
fn shapes_table(shapes: &[Shape]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## Shapes\n");
    let _ = writeln!(out, "| shape | hosts | devices | slices | pools (domains) | what it stands for |");
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- |");
    for shape in shapes {
        let slices: u32 = shape.devices.iter().map(|device| u32::from(device.slices)).sum();
        let pools: Vec<String> = shape
            .pools
            .iter()
            .map(|pool| format!("{} {} ({})", pool.name, pool.label(), shape.domains_of(pool)))
            .collect();
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} |",
            shape.name,
            shape.hosts.len(),
            shape.devices.len(),
            slices,
            pools.join(", "),
            shape.about
        );
    }
    out.push('\n');
    out
}

/// The fill tables: every candidate at every number of groups a tablet
///
/// # Arguments
///
/// * `shapes` - The shapes
/// * `options` - What to run
fn fill_tables(shapes: &[Shape], options: &Options) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## Fill\n");
    let _ = writeln!(
        out,
        "Each cell is the fullest device over the mean, then the coefficient of variation across \
         the pool's devices, of chunks over weight. `table` is the planner's assignment, the best \
         the shape allows. A window cell that breaks the domain rule says how many groups it broke \
         it for.\n"
    );
    for shape in shapes {
        let _ = writeln!(out, "### {}: {}\n", shape.name, shape.about);
        let _ = writeln!(
            out,
            "| pool | groups a tablet | chunks a device | window | rendezvous | by position | by domain | by domain, by position | table |"
        );
        let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
        for pool in &shape.pools {
            let view = View::new(shape, pool);
            if !view.feasible() {
                let _ = writeln!(out, "| {} {} | - | - | infeasible: {} domains for a width of {} | | | | | |", pool.name, pool.label(), view.domains.len(), view.width);
                continue;
            }
            for per_tablet in &options.per_tablet {
                let groups = pgs(CONSUMER, *per_tablet);
                let chunks = (groups.len() * view.width) as f64 / view.devices.len() as f64;
                let mut cells = Vec::new();
                for candidate in Candidate::FILLED {
                    let placed = place_all(&view, candidate, &groups, options.threads);
                    let (worst, cv) = fill(&view, &device_loads(&view, &placed.ids, None));
                    let mut cell = fill_cell(worst, cv);
                    if placed.totals.violations > 0 {
                        let _ = write!(cell, ", breaks the rule for {}", placed.totals.violations);
                    }
                    if placed.totals.fallbacks > 0 {
                        let _ = write!(cell, ", {} fell back", placed.totals.fallbacks);
                    }
                    cells.push(cell);
                }
                // the planner's table, built greedily
                let table = Assignment::table(&view, groups.len());
                let loads: Vec<f64> = table.device_load.iter().map(|load| *load as f64).collect();
                let (worst, cv) = fill(&view, &loads);
                cells.push(fill_cell(worst, cv));
                let _ = writeln!(
                    out,
                    "| {} {} | {} | {:.0} | {} |",
                    pool.name,
                    pool.label(),
                    per_tablet,
                    chunks,
                    cells.join(" | ")
                );
            }
        }
        out.push('\n');
    }
    out
}

/// The exception tables: what rendezvous needs on the map to be within each margin
///
/// # Arguments
///
/// * `shapes` - The shapes
/// * `options` - What to run
fn exception_tables(shapes: &[Shape], options: &Options) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## Exceptions to rendezvous\n");
    let _ = writeln!(
        out,
        "Starting from the rendezvous answer, one chunk at a time is moved off the fullest device \
         onto the emptiest the domain rule allows. A cell is the exceptions on the map once the \
         fullest device is within the margin, and the placement groups they touch, as a share of \
         the pool's; `stuck` is a balancer that found no chunk on the fullest device it could \
         move anywhere emptier.\n"
    );
    let _ = writeln!(
        out,
        "| shape | pool | groups a tablet | fullest before | within 5% | within 2% | within 1% |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- |");
    for shape in shapes {
        for pool in &shape.pools {
            let view = View::new(shape, pool);
            if !view.feasible() {
                continue;
            }
            for per_tablet in &options.per_tablet {
                let groups = pgs(CONSUMER, *per_tablet);
                let placed = place_all(&view, Candidate::Rendezvous, &groups, options.threads);
                let (mut assignment, orphans) = Assignment::from_ids(&view, &placed.ids);
                assert!(orphans.is_empty(), "a rule's answer is in its own view");
                let before = assignment.worst();
                let reached = assignment.exceptions(&MARGINS, EXCEPTION_CAP);
                let cells: Vec<String> = reached
                    .iter()
                    .map(|result| match result {
                        Some((entries, touched)) => format!(
                            "{entries} ({:.2}% of groups)",
                            *touched as f64 * 100.0 / groups.len() as f64
                        ),
                        None => "stuck".to_string(),
                    })
                    .collect();
                let _ = writeln!(
                    out,
                    "| {} | {} {} | {} | +{:.1}% | {} |",
                    shape.name,
                    pool.name,
                    pool.label(),
                    per_tablet,
                    (before - 1.0) * 100.0,
                    cells.join(" | ")
                );
            }
        }
    }
    out.push('\n');
    out
}

/// A shape whose devices carry placement weights in place of their own
///
/// # Arguments
///
/// * `shape` - The shape
/// * `weights` - A placement weight for each device uid
fn weighted(shape: &Shape, weights: &HashMap<u32, f64>) -> Shape {
    let mut shape = shape.clone();
    for device in &mut shape.devices {
        if let Some(weight) = weights.get(&device.uid) {
            device.weight = *weight;
        }
    }
    shape
}

/// Fit placement weights so rendezvous fills a pool's devices by their capacity weights
///
/// Each round places every group under the current weights and scales each device's weight by
/// the inverse square root of its fill over the mean, which damps the step so it does not
/// oscillate. Returns the weights of the round whose fullest device was lowest.
///
/// # Arguments
///
/// * `shape` - The shape, whose device weights are capacity
/// * `pool` - The pool
/// * `groups` - The groups fitted to
/// * `start` - Placement weights to start from, a device's capacity where it has none
/// * `threads` - Threads to place over
fn fit(
    shape: &Shape,
    pool: &Pool,
    groups: &[Pg],
    start: &HashMap<u32, f64>,
    threads: usize,
) -> HashMap<u32, f64> {
    let capacity = View::new(shape, pool);
    let mut weights: HashMap<u32, f64> = capacity
        .devices
        .iter()
        .map(|device| (device.uid, *start.get(&device.uid).unwrap_or(&device.weight)))
        .collect();
    let mut best = (f64::INFINITY, weights.clone());
    for _ in 0..FIT_ROUNDS {
        // place under the current weights and read each device's fill against its capacity
        let placing = View::new(&weighted(shape, &weights), pool);
        let placed = place_all(&placing, Candidate::Rendezvous, groups, threads);
        let loads = device_loads(&placing, &placed.ids, None);
        let (worst, _) = fill(&capacity, &loads);
        if worst < best.0 {
            best = (worst, weights.clone());
        }
        // the devices are in the same order in both views, so loads index the capacity view too
        let total: f64 = loads.iter().sum();
        let weight: f64 = capacity.devices.iter().map(|device| device.weight).sum();
        let mean = total / weight;
        for (device, load) in capacity.devices.iter().zip(&loads) {
            let util = (load / device.weight / mean).max(1e-6);
            if let Some(placing_weight) = weights.get_mut(&device.uid) {
                *placing_weight /= util.sqrt();
            }
        }
    }
    best.1
}

/// The fitted weight tables: the bias corrected in the map, judged on a consumer not fitted to
///
/// # Arguments
///
/// * `shapes` - The shapes
/// * `options` - What to run
fn fitted_tables(shapes: &[Shape], options: &Options) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## Fitted weights\n");
    let _ = writeln!(
        out,
        "{FIT_PER_TABLET} groups a tablet. Placement weights, one a device, fitted over \
         {FIT_ROUNDS} rounds, and judged on a consumer they were not fitted to. They are fitted \
         once to one consumer's groups, and once to a sample of {FIT_CONSUMERS} other consumers' \
         groups, which is what a planner would fit to. Each fill cell is the fullest device over \
         the mean. The exceptions are what the unfitted consumer still needs to be within 2%, \
         before and after the sample's fit. The movement is what adding a device moves under \
         `rendezvous, positions kept` once the sample's weights are fitted again from where they \
         were, over the least that change could move: the cost of keeping the fit.\n"
    );
    let _ = writeln!(
        out,
        "| shape | pool | rendezvous | fitted to one consumer, on another | fitted to the sample, on a consumer in it | fitted to the sample, on another | exceptions to 2%, before → after | add a device, fitted again |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- |");
    let holdout = pgs(HOLDOUT, FIT_PER_TABLET);
    // the planner fits to a sample of several consumers' groups, none of them the holdout's,
    // so the fit sees the rule's bias and not one consumer's luck
    let sample: Vec<Pg> = (0..FIT_CONSUMERS)
        .flat_map(|consumer| pgs(CONSUMER + 0x100 * (consumer + 1), FIT_PER_TABLET))
        .collect();
    let fitted_on = pgs(CONSUMER, FIT_PER_TABLET);
    // one of the sample's own consumers, to read the fit where it was made
    let in_sample = pgs(CONSUMER + 0x100, FIT_PER_TABLET);
    for shape in shapes.iter().filter(|shape| shape.name != "lab-1") {
        for pool in &shape.pools {
            let capacity = View::new(shape, pool);
            if !capacity.feasible() {
                continue;
            }
            // the bias before, on the second consumer
            let plain = place_all(&capacity, Candidate::Rendezvous, &holdout, options.threads);
            let (before, _) = fill(&capacity, &device_loads(&capacity, &plain.ids, None));
            let exceptions_before = Assignment::from_ids(&capacity, &plain.ids)
                .0
                .exceptions(&MARGINS[..2], EXCEPTION_CAP)[1];
            // fitted to one consumer, and to the sample, each judged on the holdout
            let single = fit(shape, pool, &fitted_on, &HashMap::new(), options.threads);
            let single_view = View::new(&weighted(shape, &single), pool);
            let single_holdout = place_all(&single_view, Candidate::Rendezvous, &holdout, options.threads);
            let (single_fill, _) = fill(&capacity, &device_loads(&single_view, &single_holdout.ids, None));
            let weights = fit(shape, pool, &sample, &HashMap::new(), options.threads);
            let placing = View::new(&weighted(shape, &weights), pool);
            let on_sample = place_all(&placing, Candidate::Rendezvous, &in_sample, options.threads);
            let (fitted_fill, _) = fill(&capacity, &device_loads(&placing, &on_sample.ids, None));
            let on_fitted = place_all(&placing, Candidate::Rendezvous, &fitted_on, options.threads);
            let on_holdout = place_all(&placing, Candidate::Rendezvous, &holdout, options.threads);
            let (holdout_fill, _) = fill(&capacity, &device_loads(&placing, &on_holdout.ids, None));
            // the holdout's answer held against the capacity view for its exceptions
            let exceptions_after = {
                let (assignment, _) = Assignment::from_ids(&capacity, &on_holdout.ids);
                let mut assignment = assignment;
                assignment.exceptions(&MARGINS[..2], EXCEPTION_CAP)[1]
            };
            // a device added, and the weights fitted again from the old ones
            let changed = shape.changed(Change::Add, pool.class);
            let refit = fit(&changed, pool, &sample, &weights, options.threads);
            let after = View::new(&weighted(&changed, &refit), pool);
            let moved_after = place_all(&after, Candidate::Rendezvous, &fitted_on, options.threads);
            let set_moves = moved(&on_fitted.ids, &moved_after.ids, capacity.width, false);
            let least = least(&placing, &after, &on_fitted.ids, &moved_after.ids);
            let exceptions = |result: Option<(u64, u64)>| {
                result.map_or_else(|| "stuck".to_string(), |(entries, _)| entries.to_string())
            };
            let _ = writeln!(
                out,
                "| {} | {} {} | +{:.1}% | +{:.1}% | +{:.1}% | +{:.1}% | {} → {} | {} |",
                shape.name,
                pool.name,
                pool.label(),
                (before - 1.0) * 100.0,
                (single_fill - 1.0) * 100.0,
                (fitted_fill - 1.0) * 100.0,
                (holdout_fill - 1.0) * 100.0,
                exceptions(exceptions_before),
                exceptions(exceptions_after),
                movement_cell(set_moves, least, on_fitted.ids.len() as f64)
            );
        }
    }
    out.push('\n');
    out
}

/// The movement tables: every change, every candidate, at one number of groups a tablet
///
/// # Arguments
///
/// * `shapes` - The shapes
/// * `options` - What to run
fn movement_tables(shapes: &[Shape], options: &Options) -> String {
    let mut out = String::new();
    let groups = pgs(CONSUMER, MOVEMENT_PER_TABLET);
    let _ = writeln!(out, "## Movement\n");
    let _ = writeln!(
        out,
        "{MOVEMENT_PER_TABLET} placement groups a tablet. Each change is made to host zero's first \
         device of the pool's class. `least` is the chunks the change had to move: what the \
         departed devices held, or what a new device ends up holding, or what a reweighted device \
         gave up, under `rendezvous`. A cell is the chunks moved over that least, and the share of \
         all the pool's chunks moved. An erasure coded pool counts a chunk that changed position \
         as moved; a replicated pool counts only a changed set.\n"
    );
    for shape in shapes {
        let _ = writeln!(out, "### {}\n", shape.name);
        let _ = writeln!(
            out,
            "| pool | change | least | window | rendezvous | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | rendezvous, positions kept | table |"
        );
        let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |");
        for pool in &shape.pools {
            let before = View::new(shape, pool);
            if !before.feasible() {
                continue;
            }
            let total = (groups.len() * before.width) as f64;
            // the answers before, once a candidate, and the table once
            let olds: Vec<Placed> = Candidate::SIMULATED
                .iter()
                .map(|candidate| place_all(&before, *candidate, &groups, options.threads))
                .collect();
            let table = Assignment::table(&before, groups.len());
            for change in Change::ALL {
                let changed = shape.changed(change, pool.class);
                let after = View::new(&changed, pool);
                if !after.feasible() {
                    let _ = writeln!(
                        out,
                        "| {} {} | {} | - | infeasible: {} domains for a width of {} | | | | | | | | |",
                        pool.name,
                        pool.label(),
                        change.label(),
                        after.domains.len(),
                        after.width
                    );
                    continue;
                }
                let mut cells = Vec::new();
                let mut floor = 0;
                let mut kept = String::new();
                for (candidate, old) in Candidate::SIMULATED.iter().zip(&olds) {
                    let new = place_all(&after, *candidate, &groups, options.threads);
                    let moves = moved(&old.ids, &new.ids, before.width, before.positional);
                    let least = least(&before, &after, &old.ids, &new.ids);
                    if *candidate == Candidate::Rendezvous {
                        floor = least;
                        // positions kept by the group: a member that stays keeps its position
                        // and one that arrives takes a vacated one, so only the set moves
                        let set_moves = moved(&old.ids, &new.ids, before.width, false);
                        kept = movement_cell(set_moves, least, total);
                    }
                    cells.push(movement_cell(moves, least, total));
                }
                cells.push(kept);
                // the table carried across the change by the planner
                let (carried, moves) = Assignment::carry(&table, &after);
                let least = least(&before, &after, &table.ids(), &carried.ids());
                cells.push(movement_cell(moves, least, total));
                let _ = writeln!(
                    out,
                    "| {} {} | {} | {} | {} |",
                    pool.name,
                    pool.label(),
                    change.label(),
                    floor,
                    cells.join(" | ")
                );
            }
        }
        out.push('\n');
    }
    out
}

/// One movement cell: moved over the least, and the share of every chunk
///
/// # Arguments
///
/// * `moves` - Chunks moved
/// * `least` - The least the change could move
/// * `total` - Every chunk of the pool
fn movement_cell(moves: u64, least: u64, total: f64) -> String {
    if least == 0 {
        return format!("{moves} moved, none needed");
    }
    format!(
        "{:.2}× ({:.2}%)",
        moves as f64 / least as f64,
        moves as f64 * 100.0 / total
    )
}

/// The feasibility table: window violations and by-position rounds, at every count a tablet
///
/// # Arguments
///
/// * `shapes` - The shapes
/// * `options` - What to run
fn feasibility_table(shapes: &[Shape], options: &Options) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## Feasibility\n");
    let _ = writeln!(
        out,
        "Groups whose answer breaks the domain rule, which only the window can give; and for the \
         two by-position candidates the mean rounds drawn and the groups that ran out of rounds \
         and fell back.\n"
    );
    let _ = writeln!(
        out,
        "| shape | pool | groups a tablet | window breaks the rule | by position: rounds, fell back | by domain, by position: rounds, fell back |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- |");
    for shape in shapes {
        for pool in &shape.pools {
            let view = View::new(shape, pool);
            if !view.feasible() {
                continue;
            }
            for per_tablet in &options.per_tablet {
                let groups = pgs(CONSUMER, *per_tablet);
                let window = place_all(&view, Candidate::Window, &groups, options.threads);
                let by_position = place_all(&view, Candidate::ByPosition, &groups, options.threads);
                let by_domain = place_all(&view, Candidate::ByDomainByPosition, &groups, options.threads);
                let share = |count: u64| count as f64 * 100.0 / groups.len() as f64;
                let _ = writeln!(
                    out,
                    "| {} | {} {} | {} | {} ({:.1}%) | {:.2}, {} | {:.2}, {} |",
                    shape.name,
                    pool.name,
                    pool.label(),
                    per_tablet,
                    window.totals.violations,
                    share(window.totals.violations),
                    by_position.totals.rounds as f64 / groups.len() as f64,
                    by_position.totals.fallbacks,
                    by_domain.totals.rounds as f64 / groups.len() as f64,
                    by_domain.totals.fallbacks
                );
            }
        }
    }
    out.push('\n');
    out
}

/// The logarithm table: how many of rendezvous's choices libm's `ln` would make differently
///
/// # Arguments
///
/// * `shapes` - The shapes
/// * `options` - What to run
fn libm_table(shapes: &[Shape], options: &Options) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## The table's logarithm against libm's\n");
    let _ = writeln!(
        out,
        "Every chunk of every group at the largest count a tablet, placed by `rendezvous` with the \
         fixed-point table and again with `f64::ln`.\n"
    );
    let _ = writeln!(out, "| shape | pool | chunks | chosen differently |");
    let _ = writeln!(out, "| --- | --- | --- | --- |");
    let per_tablet = options.per_tablet.iter().copied().max().unwrap_or(1);
    let groups = pgs(CONSUMER, per_tablet);
    for shape in shapes {
        for pool in &shape.pools {
            let view = View::new(shape, pool);
            if !view.feasible() {
                continue;
            }
            let table = place_all(&view, Candidate::Rendezvous, &groups, options.threads);
            let libm = place_all(&view, Candidate::RendezvousLibm, &groups, options.threads);
            let differ = table.ids.iter().zip(&libm.ids).filter(|(a, b)| a != b).count();
            let _ = writeln!(
                out,
                "| {} | {} {} | {} | {} |",
                shape.name,
                pool.name,
                pool.label(),
                table.ids.len(),
                differ
            );
        }
    }
    out.push('\n');
    out
}

/// The small bucket table: fill when each group holds a Poisson number of stripes
///
/// # Arguments
///
/// * `shapes` - The shapes
/// * `options` - What to run
fn small_bucket_table(shapes: &[Shape], options: &Options) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## A bucket of few stripes\n");
    let _ = writeln!(
        out,
        "`rendezvous` on lab-2, each placement group holding a Poisson number of stripes with the \
         mean the bucket's stripes over its groups. The last column is the placement alone, every \
         group holding the same.\n"
    );
    let _ = writeln!(
        out,
        "| pool | groups a tablet | 10⁴ stripes | 10⁵ | 10⁶ | 10⁷ | placement alone |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- |");
    let Some(shape) = shapes.iter().find(|shape| shape.name == "lab-2") else {
        return String::new();
    };
    for pool in &shape.pools {
        let view = View::new(shape, pool);
        for per_tablet in &options.per_tablet {
            let groups = pgs(CONSUMER, *per_tablet);
            let placed = place_all(&view, Candidate::Rendezvous, &groups, options.threads);
            let mut cells = Vec::new();
            for stripes in [1e4, 1e5, 1e6, 1e7] {
                // one draw of the bucket, seeded by its size
                let mut rng = SplitMix::new(stripes as u64 ^ u64::from(*per_tablet));
                let lambda = stripes / groups.len() as f64;
                let per_pg: Vec<u64> = (0..groups.len()).map(|_| poisson(&mut rng, lambda)).collect();
                let (worst, _) = fill(&view, &device_loads(&view, &placed.ids, Some(&per_pg)));
                cells.push(format!("+{:.1}%", (worst - 1.0) * 100.0));
            }
            let (worst, _) = fill(&view, &device_loads(&view, &placed.ids, None));
            cells.push(format!("+{:.1}%", (worst - 1.0) * 100.0));
            let _ = writeln!(
                out,
                "| {} {} | {} | {} |",
                pool.name,
                pool.label(),
                per_tablet,
                cells.join(" | ")
            );
        }
    }
    out.push('\n');
    out
}

/// The placement groups rendezvous moves when a device is added: what a pending list would hold
///
/// The pool map's alternative to keeping a generation is to list every group between two of
/// them; this is how long that list is for one consumer.
///
/// # Arguments
///
/// * `shape` - The shape
/// * `pool` - The pool
/// * `per_tablet` - Placement groups a tablet
/// * `threads` - Threads to place over
#[must_use]
pub fn pending_after_add(shape: &Shape, pool: &Pool, per_tablet: u32, threads: usize) -> u64 {
    // the answers before and after a device like host zero's first is added
    let groups = pgs(CONSUMER, per_tablet);
    let before = View::new(shape, pool);
    let after = View::new(&shape.changed(Change::Add, pool.class), pool);
    let old = place_all(&before, Candidate::Rendezvous, &groups, threads);
    let new = place_all(&after, Candidate::Rendezvous, &groups, threads);
    // the groups with any chunk moving
    old.ids
        .chunks(before.width)
        .zip(new.ids.chunks(after.width))
        .filter(|(a, b)| a != b)
        .count() as u64
}
