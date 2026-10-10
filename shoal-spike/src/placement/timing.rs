//! What one lookup costs, a placement group at a time, on one pinned core
//!
//! A placement function is evaluated wherever a stripe is staged or read (S5, "What it costs"),
//! so its cost a lookup is one of X2's numbers. This times each candidate over the shapes that
//! bracket the question, the lab's and fifty hosts of twenty-four, and the cost of building the
//! lookup's view from a map, which every node pays on every committed change. These figures are
//! the only ones in X2 that depend on the host, and every table says which it ran on.

use std::fmt::Write as _;
use std::hint::black_box;
use std::time::{Duration, Instant};

use super::candidates::{pgs, Candidate, Scratch, View, MAX_WIDTH};
use super::measure::place_all;
use super::shape::Shape;

/// Placement groups a tablet the lookups are timed at
const PER_TABLET: u32 = 4;

/// How long one timed run lasts at least
const RUN: Duration = Duration::from_millis(200);

/// Timed runs a cell, of which the median is printed
const RUNS: usize = 5;

/// The candidates timed, the table's read last
const TIMED: [Candidate; 8] = [
    Candidate::Window,
    Candidate::Rendezvous,
    Candidate::RendezvousLibm,
    Candidate::ByPosition,
    Candidate::ByDomain,
    Candidate::ByDomainByPosition,
    Candidate::Matched,
    Candidate::ByDomainMatched,
];

/// Pin this thread to one cpu
///
/// # Arguments
///
/// * `core` - The cpu
///
/// # Panics
///
/// If the kernel refuses the affinity.
pub fn pin(core: usize) {
    // a set holding the one cpu, applied to the calling thread
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        let result = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        assert_eq!(result, 0, "pinning to cpu {core} failed");
    }
}

/// The cpu's model, as `/proc/cpuinfo` names it
#[must_use]
pub fn cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|info| {
            info.lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split(':').nth(1))
                .map(|name| name.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// The median of several timed runs of a closure, in nanoseconds a call
///
/// # Arguments
///
/// * `f` - One call
fn ns_per_call<F: FnMut()>(mut f: F) -> f64 {
    let mut medians = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        // whole batches of calls until the run has lasted long enough
        let start = Instant::now();
        let mut calls = 0u64;
        while start.elapsed() < RUN {
            for _ in 0..64 {
                f();
            }
            calls += 64;
        }
        medians.push(start.elapsed().as_nanos() as f64 / calls as f64);
    }
    medians.sort_by(f64::total_cmp);
    medians[RUNS / 2]
}

/// Time every candidate's lookup over the timed shapes
///
/// # Arguments
///
/// * `core` - The cpu to pin to, if any
/// * `label` - What build this is, for the header
#[must_use]
pub fn lookups(core: Option<usize>, label: &str) -> String {
    if let Some(core) = core {
        pin(core);
    }
    let mut out = String::new();
    let _ = writeln!(out, "# X2 lookups\n");
    let _ = writeln!(
        out,
        "host {} · {} · governor {} · build {} · cpu {} · {} placement groups a tablet · median of {} runs of at least {} ms\n",
        crate::hostname(),
        cpu_model(),
        crate::governor(),
        label,
        core.map_or_else(|| "unpinned".to_string(), |core| core.to_string()),
        PER_TABLET,
        RUNS,
        RUN.as_millis()
    );
    let groups = pgs(0x5eed_0001, PER_TABLET);
    let _ = writeln!(
        out,
        "| shape | pool | slices | domains | build the view µs | window | rendezvous | rendezvous, libm ln | by position | by domain | by domain, by position | rendezvous, matched | by domain, matched | a table's read |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for shape in Shape::timed() {
        for pool in &shape.pools {
            let view = View::new(&shape, pool);
            if !view.feasible() {
                continue;
            }
            // building the view, which a node does once a committed change
            let build = ns_per_call(|| {
                black_box(View::new(black_box(&shape), pool));
            }) / 1000.0;
            let mut cells = Vec::new();
            for candidate in TIMED {
                // groups in turn, wrapping, so no one group's answer is cached by the cpu
                let mut scratch = Scratch::default();
                let mut out = [0u32; MAX_WIDTH];
                let mut next = 0;
                let ns = ns_per_call(|| {
                    let pg = &groups[next];
                    next = (next + 1) % groups.len();
                    black_box(view.place(candidate, pg, &mut scratch, &mut out));
                    black_box(&out);
                });
                cells.push(format!("{ns:.0}"));
            }
            // a table's lookup is a read of the group's row, timed over every group's answer
            let ids = place_all(&view, Candidate::Rendezvous, &groups, 1).ids;
            let width = view.width;
            let mut next = 0;
            let read = ns_per_call(|| {
                let row = &ids[next * width..(next + 1) * width];
                next = (next + 1) % groups.len();
                black_box(row);
            });
            cells.push(format!("{read:.1}"));
            let _ = writeln!(
                out,
                "| {} | {} {} | {} | {} | {:.1} | {} |",
                shape.name,
                pool.name,
                pool.label(),
                view.slices.len(),
                view.domains.len(),
                build,
                cells.join(" | ")
            );
        }
    }
    out.push('\n');
    let _ = writeln!(out, "Every lookup cell is nanoseconds for one placement group's whole answer.");
    out
}
