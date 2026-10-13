//! X7: a foreground write's tail while a deep scrub reads, at a byte budget
//!
//! S11 bounds a scrub by bytes a second for each device, shared with recovery, and S10 says a
//! budget sized for an SSD saturates a disk. This measures what a budget leaves the foreground on
//! a disk. A deep scrub reads every chunk whole, in the order the population was written, taking
//! its bytes from a bucket that fills at the budget. Beside it on the same executor, open loops
//! issue small writes the way S6 makes them, a journal record on the disk then the unit and
//! header in place and the chunk synced, and 64 KiB reads, each counted from its slot. H4 reads
//! the writes' tail at each budget against none.

use std::rc::Rc;
use std::time::Duration;

use super::arm::{self, Arm, CHUNK};
use super::contend::{paced_figures, read_one, Journal};
use super::counters::Devices;
use super::io::{self, Payloads, HEADER};
use super::paced::{open_loop, until, Bucket, Window};
use super::stats::{fmt, Rng};
use super::table::Table;
use super::{on_core, ordered, Ctx, SideOut};

/// The budgets, MiB a second; `None` is a scrub with no bound
const BUDGETS: &[Option<f64>] = &[Some(0.0), Some(10.0), Some(20.0), Some(40.0), Some(60.0), None];

/// Chunks in the population
const POPULATION: usize = 1024;

/// A foreground write's unit
const WRITE: u64 = 4 << 10;

/// Foreground writes a second
const WRITE_RATE: f64 = 10.0;

/// Foreground reads a second
const READ_RATE: f64 = 20.0;

/// The journal's ring
const RING: u64 = 256 << 20;

/// The budget's name in a cell
///
/// # Arguments
///
/// * `budget` - The budget
fn budget_name(budget: Option<f64>) -> String {
    match budget {
        Some(mib) => format!("budget={mib:.0}"),
        None => "budget=unbounded".to_string(),
    }
}

/// One small write as S6 makes it: staged, then applied in place and synced
///
/// # Arguments
///
/// * `arm` - The population
/// * `journal` - The disk's journal
/// * `payloads` - The bytes
/// * `nth` - The write's number, which seeds its place
async fn write_one(arm: Rc<Arm>, journal: Rc<Journal>, payloads: Rc<Payloads>, nth: u64) {
    let mut rng = Rng::new(nth ^ 0x5c2b_0000);
    let chunk = rng.below(arm.files.len() as u64) as usize;
    let offset = HEADER + rng.below(CHUNK / WRITE) * WRITE;
    // the stage, durable through the group commit
    journal.stage(&payloads).await;
    // the unit and header in place, then the chunk's sync
    let file = &arm.files[chunk];
    futures::join!(
        io::write_body(file, &payloads, WRITE, offset),
        io::write_body(file, &payloads, HEADER, 0)
    );
    file.fdatasync().await.expect("applied");
}

/// Read every chunk whole, in order and round again, at a budget, until the window ends
///
/// # Arguments
///
/// * `arm` - The population
/// * `budget` - MiB a second, or `None` for no bound
/// * `window` - The cell's window
async fn scrub(arm: Rc<Arm>, budget: Option<f64>, window: Window) -> u64 {
    let mut bucket = Bucket::new(budget, window.start);
    let mut counted = 0;
    let mut chunk = 0;
    until(window.start).await;
    loop {
        // a chunk's bytes from the bucket, then the chunk read whole with its header
        bucket.take(CHUNK + HEADER).await;
        if std::time::Instant::now() >= window.end {
            break;
        }
        let begun = std::time::Instant::now();
        arm.files[chunk].read_at_aligned(0, (CHUNK + HEADER) as usize).await.expect("scrubbed");
        if window.counts(begun, std::time::Instant::now()) {
            counted += CHUNK + HEADER;
        }
        chunk = (chunk + 1) % arm.files.len();
    }
    counted
}

/// Run one budget's side
///
/// # Arguments
///
/// * `arm` - The population
/// * `journal` - The disk's journal
/// * `payloads` - The bytes
/// * `budget` - The scrub's budget, zero for no scrub
/// * `window` - The side's window
/// * `devices` - The disk's devices
async fn side(arm: Rc<Arm>, journal: Rc<Journal>, payloads: Rc<Payloads>, budget: Option<f64>, window: Window, devices: Devices) -> SideOut {
    let edges = glommio::spawn_local(async move {
        until(window.warm).await;
        let before = devices.snap();
        until(window.end).await;
        before.delta(&devices.snap())
    });
    // no scrub at a budget of zero
    let scrubber = (budget != Some(0.0)).then(|| glommio::spawn_local(scrub(arm.clone(), budget, window)));
    let writes = {
        let (arm, journal, payloads) = (arm.clone(), journal.clone(), payloads.clone());
        glommio::spawn_local(open_loop(WRITE_RATE, window, move |nth| {
            write_one(arm.clone(), journal.clone(), payloads.clone(), nth)
        }))
    };
    let reads = {
        let arm = arm.clone();
        glommio::spawn_local(open_loop(READ_RATE, window, move |nth| read_one(arm.clone(), nth)))
    };
    let (writes, reads) = (writes.await, reads.await);
    let scrubbed = match scrubber {
        Some(task) => task.await,
        None => 0,
    };
    let delta = edges.await;
    let mut figures: Vec<(String, f64)> = vec![
        ("budget_mib_s".into(), budget.unwrap_or(-1.0)),
        ("scrub_mib_s".into(), scrubbed as f64 / window.secs() / f64::from(1 << 20)),
        ("busy".into(), delta.busy()),
        ("flushes_s".into(), delta.flushes as f64 / delta.secs),
    ];
    figures.extend(paced_figures("write", &writes));
    figures.extend(paced_figures("read", &reads));
    let figures: Vec<(&str, f64)> = figures.iter().map(|(name, value)| (name.as_str(), *value)).collect();
    SideOut::new(budget_name(budget), "scrub", &figures)
}

/// Run the scrub measurement for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let root = ctx.sub("arm");
    let count = ctx.count(POPULATION, 64);
    {
        let root = root.clone();
        on_core(ctx.core, ctx.sibling, move || arm::populate(root, count));
    }
    let dir = ctx.sub("scrub");
    let devices = ctx.facts.devices.clone();
    let device_bytes = ctx.facts.fs_bytes;
    let quick = ctx.quick;
    let windows = (ctx.window(Duration::from_secs(3)), ctx.window(Duration::from_secs(20)));
    let budgets: Vec<Option<f64>> = if quick { vec![Some(0.0), Some(10.0), None] } else { BUDGETS.to_vec() };
    let (outs, span) = on_core(ctx.core, ctx.sibling, move || async move {
        io::wipe(&dir);
        let arm = Rc::new(Arm::open(&root, count).await);
        let span = arm.span(device_bytes);
        let journal = Rc::new(Journal::make(&dir, if quick { 16 << 20 } else { RING }).await);
        let payloads = Rc::new(Payloads::new(0x5c2b));
        let _ = (payloads.get(WRITE), payloads.get(HEADER), payloads.get(HEADER + (4 << 10)));
        let mut outs = Vec::new();
        for budget in ordered(&budgets, round) {
            let window = Window::new(windows.0, windows.1);
            outs.push(side(arm.clone(), journal.clone(), payloads.clone(), budget, window, devices.clone()).await);
        }
        if let Ok(journal) = Rc::try_unwrap(journal) {
            journal.close().await;
        }
        if let Ok(arm) = Rc::try_unwrap(arm) {
            arm.close().await;
        }
        io::wipe(&dir);
        (outs, span)
    });
    let mut table = Table::new(&[
        "budget MiB/s", "scrub MiB/s", "write p50 ms", "write p99 ms", "write max ms", "read p50 ms",
        "read p99 ms", "disk busy", "flushes/s",
    ]);
    let ms = |out: &SideOut, name: &str| fmt(out.get(name) / 1e3);
    let mut records = Vec::new();
    // by budget, the unbounded scrub last
    let key = |out: &SideOut| match out.get("budget_mib_s") {
        budget if budget < 0.0 => f64::MAX,
        budget => budget,
    };
    let mut outs = outs;
    outs.sort_by(|a, b| key(a).total_cmp(&key(b)));
    for mut out in outs {
        // the supplement's sides carry the write cache they ran under
        out.side.push_str(&ctx.side_suffix);
        table.row(vec![
            out.cell.trim_start_matches("budget=").to_string(),
            fmt(out.get("scrub_mib_s")),
            ms(&out, "write_p50"),
            ms(&out, "write_p99"),
            ms(&out, "write_max"),
            ms(&out, "read_p50"),
            ms(&out, "read_p99"),
            fmt(out.get("busy")),
            fmt(out.get("flushes_s")),
        ]);
        for (name, value) in span.figures() {
            out.metrics.insert(name.to_string(), value);
        }
        records.push(ctx.record("scrub", round, out));
    }
    print!(
        "{}",
        table.render(
            &format!(
                "X7 · A foreground under a deep scrub's budget, round {round} (4 KiB writes, staged then applied, at {WRITE_RATE}/s; 64 KiB reads at {READ_RATE}/s; open loop; {})",
                span.show()
            ),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}
