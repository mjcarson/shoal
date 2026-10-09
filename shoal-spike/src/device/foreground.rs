//! X12: the foreground a rebuild and a scrub are judged beside
//!
//! It is X7's scrub foreground with what Q23 decided for a disk: small writes made as S6 makes
//! them, a 4 KiB record staged on a journal and then the unit and the header written in place and
//! the chunk synced, and 64 KiB reads of a random unit. On a rotational disk the journal is on the
//! host's SSD, as a rotational device's must be, and the rates are X7's; on an SSD the journal is
//! on the device itself and the rates are those X7's `shared` gave an SSD's slice. Every operation
//! enters the slice's gauge from its slot to its end, which is what a background paced by idle time
//! waits on, and arrives at a time drawn from a seeded Poisson process, the same for every side of
//! a round. A write's latency is to durable in place; its stage, the acknowledgement a client
//! sees, is timed apart.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Instant;

use glommio::Task;

use super::arm::{self, Arm, CHUNK};
use super::contend::{paced_figures, read_one, Journal};
use super::io::{self, Payloads, HEADER};
use super::paced::{open_loop_with, Arrivals, Gauge, Paced, Window};
use super::stats::{Rng, Samples};
use super::Ctx;

/// A foreground write's unit
const WRITE: u64 = 4 << 10;

/// The journal's ring
const RING: u64 = 256 << 20;

/// Chunks in the foreground's population, which is X7's
const POPULATION: usize = 1024;

/// What the foreground offers
#[derive(Debug, Clone, Copy)]
pub struct Load {
    /// Reads a second
    pub read_rate: f64,
    /// Writes a second
    pub write_rate: f64,
}

impl Load {
    /// The load for a device: X7's on a disk, X7's SSD slice's on an SSD
    ///
    /// # Arguments
    ///
    /// * `rotational` - Whether the device spins
    #[must_use]
    pub fn for_device(rotational: bool) -> Load {
        if rotational {
            Load { read_rate: 20.0, write_rate: 10.0 }
        } else {
            Load { read_rate: 1000.0, write_rate: 200.0 }
        }
    }
}

/// Where the foreground's files are
#[derive(Debug, Clone)]
pub struct Places {
    /// Its population, on the device measured
    pub arm: PathBuf,
    /// Its journal's directory: the host's SSD beside a disk, else the device measured
    pub journal: PathBuf,
    /// Chunks in its population
    pub count: usize,
    /// The ring's length
    pub ring: u64,
    /// Whether the journal is on another device than the one measured
    pub journal_apart: bool,
}

impl Places {
    /// Where the foreground goes for a run
    ///
    /// # Arguments
    ///
    /// * `ctx` - The run
    #[must_use]
    pub fn of(ctx: &Ctx) -> Places {
        // a disk's journal on its node's SSD, as Q23 decided, when one was named
        let ssd_journal = ctx.rotational().then(|| ctx.ssd_sub("x12-journal")).flatten();
        Places {
            arm: ctx.sub("arm"),
            journal_apart: ssd_journal.is_some(),
            journal: ssd_journal.unwrap_or_else(|| ctx.sub("x12-journal")),
            count: ctx.count(POPULATION, 64),
            ring: if ctx.quick { 16 << 20 } else { RING },
        }
    }

    /// Make the population if it is not there, outside anything timed
    ///
    /// # Arguments
    ///
    /// * `ctx` - The run
    pub fn populate(&self, ctx: &Ctx) {
        let (root, count) = (self.arm.clone(), self.count);
        super::on_core(ctx.core, ctx.sibling, move || arm::populate(root, count));
    }
}

/// The foreground's files, held open for a measurement
pub struct Fg {
    /// The population
    pub arm: Rc<Arm>,
    /// The journal
    pub journal: Rc<Journal>,
    /// The bytes
    pub payloads: Rc<Payloads>,
    /// The slice's gauge, which every operation enters
    pub gauge: Rc<Gauge>,
    /// What it offers
    pub load: Load,
    /// Whether its journal is on another device than the one measured
    pub journal_apart: bool,
}

/// What one side's foreground saw
pub struct Seen {
    /// Its reads
    pub reads: Paced,
    /// Its writes, to durable in place
    pub writes: Paced,
    /// Its writes' stages, from the slot
    pub stages: Samples,
}

impl Fg {
    /// Open the foreground's files on the slice's executor
    ///
    /// # Arguments
    ///
    /// * `places` - Where they are
    /// * `load` - What it offers
    pub async fn open(places: &Places, load: Load) -> Fg {
        let arm = Rc::new(Arm::open(&places.arm, places.count).await);
        let journal = Rc::new(Journal::make(&places.journal, places.ring).await);
        let payloads = Rc::new(Payloads::new(0x12f9));
        // every buffer filled now, outside the window
        let _ = (payloads.get(WRITE), payloads.get(HEADER), payloads.get(HEADER + WRITE));
        Fg { arm, journal, payloads, gauge: Rc::new(Gauge::default()), load, journal_apart: places.journal_apart }
    }

    /// Start the side's reads and writes; `none` of them is offered when the load is idle
    ///
    /// # Arguments
    ///
    /// * `window` - The side's window
    /// * `seed` - The seed of the arrivals, the same for every side of a round
    #[must_use]
    pub fn start(&self, window: Window, seed: u64) -> Running {
        let stages = Rc::new(RefCell::new(Samples::default()));
        let reads = {
            let arm = self.arm.clone();
            glommio::spawn_local(open_loop_with(
                self.load.read_rate,
                window,
                Arrivals::Poisson(seed ^ 0x7ead),
                Some(self.gauge.clone()),
                move |nth, _| read_one(arm.clone(), nth),
            ))
        };
        let writes = {
            let (arm, journal, payloads, stages) = (self.arm.clone(), self.journal.clone(), self.payloads.clone(), stages.clone());
            glommio::spawn_local(open_loop_with(
                self.load.write_rate,
                window,
                Arrivals::Poisson(seed ^ 0x3417e),
                Some(self.gauge.clone()),
                move |nth, slot| write_one(arm.clone(), journal.clone(), payloads.clone(), stages.clone(), window, nth, slot),
            ))
        };
        Running { reads, writes, stages }
    }

    /// Close every file
    pub async fn close(self) {
        if let Ok(journal) = Rc::try_unwrap(self.journal) {
            journal.close().await;
        }
        if let Ok(arm) = Rc::try_unwrap(self.arm) {
            arm.close().await;
        }
    }
}

/// A side's foreground in flight
pub struct Running {
    /// Its reads' loop
    reads: Task<Paced>,
    /// Its writes' loop
    writes: Task<Paced>,
    /// Its writes' stages
    stages: Rc<RefCell<Samples>>,
}

impl Running {
    /// Wait for every operation the side issued
    pub async fn finish(self) -> Seen {
        let (reads, writes) = (self.reads.await, self.writes.await);
        Seen { reads, writes, stages: self.stages.take() }
    }
}

/// One small write as S6 makes it: staged, then applied in place and synced
///
/// # Arguments
///
/// * `arm` - The population
/// * `journal` - The journal
/// * `payloads` - The bytes
/// * `stages` - Where the stage's latency from the slot is kept
/// * `window` - The side's window, so only counted writes keep a stage
/// * `nth` - The write's number, which seeds its place
/// * `slot` - When it was due
async fn write_one(
    arm: Rc<Arm>,
    journal: Rc<Journal>,
    payloads: Rc<Payloads>,
    stages: Rc<RefCell<Samples>>,
    window: Window,
    nth: u64,
    slot: Instant,
) {
    let mut rng = Rng::new(nth ^ 0x12_5c2b);
    let chunk = rng.below(arm.files.len() as u64) as usize;
    let offset = HEADER + rng.below(CHUNK / WRITE) * WRITE;
    // the stage, durable through the group commit: what a client is acknowledged after
    journal.stage(&payloads).await;
    if slot >= window.warm {
        stages.borrow_mut().push(slot.elapsed());
    }
    // the unit and header in place, then the chunk's sync
    let file = &arm.files[chunk];
    futures::join!(io::write_body(file, &payloads, WRITE, offset), io::write_body(file, &payloads, HEADER, 0));
    file.fdatasync().await.expect("applied");
}

/// The foreground's figures for a side's record
///
/// # Arguments
///
/// * `seen` - What it saw
#[must_use]
pub fn figures(seen: &Seen) -> Vec<(String, f64)> {
    let mut figures = paced_figures("read", &seen.reads);
    figures.extend(paced_figures("write", &seen.writes));
    let stages = seen.stages.summary();
    figures.push(("stage_p50".to_string(), stages.p50));
    figures.push(("stage_p99".to_string(), stages.tail()));
    // how late an operation started: the time the executor was held by the background's cpu
    let mut lag = seen.reads.lag.clone();
    lag.extend(&seen.writes.lag);
    let lag = lag.summary();
    figures.push(("lag_p50".to_string(), lag.p50));
    figures.push(("lag_p99".to_string(), lag.tail()));
    figures.push(("lag_max".to_string(), lag.max));
    figures
}
