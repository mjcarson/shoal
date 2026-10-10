//! X9's object-shaped work beside the tables
//!
//! Spike X9 asks whether object work can share an executor with tables, and what a stager's work
//! costs the shard it runs on (`docs/src/object-storage/spikes.md#x9-table-latency-beside-object-work`).
//! This module is the work it runs beside the workload grid's reference cell: a task that takes
//! stripes of four data units at a set rate and does to each what a stager does to the bytes a
//! client sent it - copies each unit in, checksums it with CRC-64/NVME (X5's choice), encodes two
//! parity units with `rusty_erasure` on ISA-L's Cauchy matrix (X4's), checksums those, and writes
//! all six with direct I/O to a slice, synced as a group every ten milliseconds.
//!
//! It runs in one of two places, asked for by `SHOAL_X9_PLACE`:
//!
//! - `shards`: on every table shard, in a third task queue below the two a shard has
//!   (`Shard::new`), at the node's rate divided among them. The queue is created here, so a run
//!   that asks for nothing has exactly the shards a build without the feature has.
//! - `core:<cpu>`: on an executor of its own pinned to that cpu, at the whole rate, started once
//!   the shards are answering.
//!
//! Work is cut into steps of one chunk unit's worth of input, and between steps the task offers
//! the executor back with `yield_if_needed`, which is S13's "a yield between units". Whether the
//! offer is taken depends on the queue's latency goal, so the report counts both, and times each
//! step and each hold: the time the task kept the executor between two real suspensions, which is
//! what a table's query waits behind.
//!
//! Configured by environment, as the stage profile's sampling is, because a configuration field
//! would be a permanent knob for something only a spike's build reads. Built by `--features x9`
//! and by nothing else, and thrown away when M14 builds object work for real.

use std::cell::Cell;
use std::ops::Range;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use glommio::io::{DmaBuffer, DmaFile, OpenOptions};
use glommio::{Latency, LocalExecutorBuilder, Placement, Shares, Task};
use rusty_erasure::{Coder, Matrix};
use serde::Serialize;
use tracing::{event, instrument, Level};

use crate::server::errors::ShoalError;
use crate::server::ServerError;

/// Where the work runs: `off`, `shards` or `core:<cpu>`
pub const PLACE_ENV: &str = "SHOAL_X9_PLACE";
/// The node's rate of object work, in MiB of data a second
pub const RATE_ENV: &str = "SHOAL_X9_RATE_MIB";
/// The chunk unit, in bytes
pub const UNIT_ENV: &str = "SHOAL_X9_UNIT";
/// The pool's directory, or a block device node to write straight to
pub const DIR_ENV: &str = "SHOAL_X9_DIR";
/// Where the report is written when the pool exits
pub const REPORT_ENV: &str = "SHOAL_X9_REPORT";
/// The latency goal of the third queue in microseconds; unset is `Latency::NotImportant`
pub const LATENCY_ENV: &str = "SHOAL_X9_LATENCY_US";
/// The size of each runner's ring of slots on the pool, in MiB
pub const RING_ENV: &str = "SHOAL_X9_RING_MIB";
/// The input a step reads, in KiB; unset is a whole unit, the plan's "between chunk units"
pub const STEP_ENV: &str = "SHOAL_X9_STEP_KIB";

/// The data units a stripe holds, as the reference layout 4+2 has it
pub const DATA_UNITS: usize = 4;
/// The parity units a stripe holds
pub const PARITY_UNITS: usize = 2;
/// The third queue's shares, a fifth of the medium queue's and a tenth of the high one's
pub const LOW_PRIORITY_SHARES: usize = 100;
/// How often what was written is synced, as a slice's journal commits a group
pub const SYNC_INTERVAL: Duration = Duration::from_millis(10);
/// The seeded bytes a runner copies its units in from, rotated so no stripe is served from cache
pub const SOURCE_BYTES: usize = 32 << 20;
/// The ring each runner writes into when none is asked for
pub const DEFAULT_RING_MIB: u64 = 256;
/// The longest a runner sleeps before it looks at its stop flag again
const SLEEP_SLICE: Duration = Duration::from_millis(20);
/// The furthest a runner's schedule falls behind before the time past it is dropped
///
/// A runner starved while the workload seeds would otherwise carry its backlog into the measured
/// phase and run it there above its rate. A stager whose clients' bytes go unread holds them back
/// through their connections; it does not owe the time it lost.
pub const MAX_BACKLOG: Duration = Duration::from_millis(50);

/// Where the object work runs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskPlacement {
    /// On every table shard, in a third task queue
    TableShards,
    /// On an executor of its own, pinned to this cpu
    OwnCore(usize),
}

/// What a run of object work was asked to do
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectWorkPlan {
    /// Where it runs
    pub placement: TaskPlacement,
    /// The node's rate, in data bytes a second
    pub rate_bytes: u64,
    /// The chunk unit in bytes
    pub unit: usize,
    /// The input bytes a step reads
    pub step_bytes: usize,
    /// The pool's directory or block device
    pub dir: PathBuf,
    /// Where to write the report
    pub report: Option<PathBuf>,
    /// The third queue's latency goal, if it has one
    pub queue_latency: Option<Duration>,
    /// Each runner's ring, in bytes
    pub ring_bytes: u64,
}

impl ObjectWorkPlan {
    /// Read the plan from this process's environment
    ///
    /// Returns `None` when no work was asked for.
    ///
    /// # Errors
    ///
    /// A variable that cannot be read as what it names, or a plan that cannot be run.
    pub fn from_env() -> Result<Option<Self>, ServerError> {
        // read through the one parser the tests drive
        Self::parse(|key| std::env::var(key).ok())
            .map_err(|error| ServerError::Shoal(ShoalError::InvalidConfig(error)))
    }

    /// Read a plan from variables looked up by `read`
    ///
    /// # Arguments
    ///
    /// * `read` - Looks a variable up by name
    fn parse(read: impl Fn(&str) -> Option<String>) -> Result<Option<Self>, String> {
        // nothing asked for is nothing to run
        let place = match read(PLACE_ENV) {
            None => return Ok(None),
            Some(place) => place,
        };
        // where it runs
        let placement = match place.as_str() {
            "" | "off" => return Ok(None),
            "shards" => TaskPlacement::TableShards,
            other => match other.strip_prefix("core:").map(str::parse::<usize>) {
                Some(Ok(cpu)) => TaskPlacement::OwnCore(cpu),
                _ => {
                    return Err(format!(
                        "{PLACE_ENV}={other} is not off, shards or core:<cpu>"
                    ))
                }
            },
        };
        // a number read from a variable, refused by name
        let number = |key: &str| -> Result<Option<u64>, String> {
            read(key)
                .map(|value| {
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("{key}={value} is not a whole number"))
                })
                .transpose()
        };
        // the rate and the unit have no defaults: a run names both
        let rate_mib = number(RATE_ENV)?.ok_or_else(|| format!("{RATE_ENV} is not set"))?;
        let unit = number(UNIT_ENV)?.ok_or_else(|| format!("{UNIT_ENV} is not set"))? as usize;
        let dir = read(DIR_ENV)
            .map(PathBuf::from)
            .ok_or_else(|| format!("{DIR_ENV} is not set"))?;
        // a unit direct I/O can write, which the encode cuts into equal column slices
        if rate_mib == 0 {
            return Err(format!("{RATE_ENV} is zero; unset {PLACE_ENV} for no work"));
        }
        if unit == 0 || unit % 4096 != 0 || unit % DATA_UNITS != 0 {
            return Err(format!(
                "{UNIT_ENV}={unit} is not a non-zero multiple of 4096"
            ));
        }
        // a ring holds whole stripes, and at least one
        let ring_bytes = number(RING_ENV)?.unwrap_or(DEFAULT_RING_MIB) << 20;
        let stripe_bytes = (unit * (DATA_UNITS + PARITY_UNITS)) as u64;
        if ring_bytes < stripe_bytes || ring_bytes % unit as u64 != 0 {
            return Err(format!(
                "{RING_ENV} gives a ring of {ring_bytes} bytes, which holds no whole stripe of {unit} byte units"
            ));
        }
        // a step of a whole unit, or a piece that divides it
        let step_bytes = number(STEP_ENV)?.map_or(unit, |kib| (kib as usize) << 10);
        if step_bytes == 0 || step_bytes > unit || unit % step_bytes != 0 || step_bytes % 4096 != 0
        {
            return Err(format!("{STEP_ENV} gives a step of {step_bytes} bytes, which does not divide a unit of {unit}"));
        }
        // a latency goal for the third queue, or none
        let queue_latency = number(LATENCY_ENV)?.map(Duration::from_micros);
        Ok(Some(ObjectWorkPlan {
            placement,
            rate_bytes: rate_mib << 20,
            unit,
            step_bytes,
            dir,
            report: read(REPORT_ENV).map(PathBuf::from),
            queue_latency,
            ring_bytes,
        }))
    }

    /// The rate one runner of `runners` takes, the node's divided evenly
    ///
    /// # Arguments
    ///
    /// * `runners` - How many runners share the node's rate
    #[must_use]
    pub fn runner_rate(&self, runners: usize) -> u64 {
        self.rate_bytes / runners.max(1) as u64
    }

    /// The data bytes one stripe carries
    #[must_use]
    pub fn stripe_data_bytes(&self) -> u64 {
        (self.unit * DATA_UNITS) as u64
    }
}

/// When each stripe is due, from a runner's origin
///
/// A token schedule: stripe `n` is due once `n` stripes' bytes have been earned at the rate, so a
/// runner that falls behind catches up rather than losing the stripes it missed.
#[derive(Debug, Clone, Copy)]
pub struct PacingSchedule {
    /// The data bytes a stripe carries
    stripe_bytes: u64,
    /// The rate, in data bytes a second
    rate_bytes: u64,
}

impl PacingSchedule {
    /// A schedule of stripes of `stripe_bytes` at `rate_bytes` a second
    ///
    /// # Arguments
    ///
    /// * `stripe_bytes` - The data bytes a stripe carries
    /// * `rate_bytes` - The rate, in data bytes a second
    #[must_use]
    pub fn new(stripe_bytes: u64, rate_bytes: u64) -> Self {
        PacingSchedule {
            stripe_bytes,
            rate_bytes: rate_bytes.max(1),
        }
    }

    /// When stripe `stripe` is due, after the origin
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe's number, from zero
    #[must_use]
    pub fn due(&self, stripe: u64) -> Duration {
        // in nanoseconds, wide enough that no run overflows it
        let nanos = u128::from(stripe) * u128::from(self.stripe_bytes) * 1_000_000_000
            / u128::from(self.rate_bytes);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

/// One step of a stripe's work, a step's worth of input bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StripeStep {
    /// Copy a piece of a data unit in, as a socket's receive would
    Receive {
        /// The data unit
        unit: usize,
        /// The piece of it
        bytes: Range<usize>,
    },
    /// Checksum a piece of a data unit
    ChecksumData {
        /// The data unit
        unit: usize,
        /// The piece of it
        bytes: Range<usize>,
    },
    /// Encode the parity of one slice of columns across every data unit
    EncodeColumns(Range<usize>),
    /// Checksum a piece of a parity unit
    ChecksumParity {
        /// The parity unit
        unit: usize,
        /// The piece of it
        bytes: Range<usize>,
    },
}

impl StripeStep {
    /// The kind of step, as the report groups them
    #[must_use]
    pub fn kind(&self) -> StepKind {
        match self {
            StripeStep::Receive { .. } => StepKind::Receive,
            StripeStep::ChecksumData { .. } | StripeStep::ChecksumParity { .. } => {
                StepKind::Checksum
            }
            StripeStep::EncodeColumns(_) => StepKind::Encode,
        }
    }
}

/// The kinds of step a stripe takes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    /// A data unit copied in
    Receive,
    /// A unit checksummed
    Checksum,
    /// A slice of columns encoded
    Encode,
}

/// Every step of a stripe of units of `unit` bytes, `step` bytes of input each, in the order a
/// stager takes them
///
/// The encode reads every data unit at once, so a whole stripe's encode is `DATA_UNITS` units of
/// input in one call: half a millisecond at 1 MiB on a Zen1 core (X4). Cut into slices of
/// `step / DATA_UNITS` columns, each call reads a step's worth, as every other step does. With
/// `step` the unit, each step is one chunk unit's worth, which is the plan's "yielding between
/// chunk units"; a smaller step puts a yield inside a unit, which a CRC allows since it keeps
/// eight bytes of state between pieces (S13).
///
/// # Arguments
///
/// * `unit` - The chunk unit in bytes
/// * `step` - The input bytes a step reads, which divides the unit
#[must_use]
pub fn stripe_steps(unit: usize, step: usize) -> Vec<StripeStep> {
    // the pieces of a unit, and the columns a slice of the encode covers
    let pieces: Vec<Range<usize>> = (0..unit)
        .step_by(step)
        .map(|start| start..(start + step).min(unit))
        .collect();
    let slice = step / DATA_UNITS;
    let mut steps = Vec::with_capacity(pieces.len() * (2 * DATA_UNITS + PARITY_UNITS + DATA_UNITS));
    // the bytes arrive and are checked a piece at a time
    for unit_index in 0..DATA_UNITS {
        for bytes in &pieces {
            steps.push(StripeStep::Receive {
                unit: unit_index,
                bytes: bytes.clone(),
            });
            steps.push(StripeStep::ChecksumData {
                unit: unit_index,
                bytes: bytes.clone(),
            });
        }
    }
    // then the parity, a step's worth of input at a time
    for start in (0..unit).step_by(slice) {
        steps.push(StripeStep::EncodeColumns(start..(start + slice).min(unit)));
    }
    // and its checksums
    for parity in 0..PARITY_UNITS {
        for bytes in &pieces {
            steps.push(StripeStep::ChecksumParity {
                unit: parity,
                bytes: bytes.clone(),
            });
        }
    }
    steps
}

/// The encode and the checksums a stripe's steps run
pub struct StripeCoder {
    /// Reed-Solomon over ISA-L's Cauchy matrix, with the best kernels this cpu has
    coder: Coder,
    /// Each unit's checksum so far, for a unit fed in pieces, data then parity
    digests: Vec<crc_fast::Digest>,
    /// The last checksum of each unit, data then parity
    sums: [u64; DATA_UNITS + PARITY_UNITS],
}

impl StripeCoder {
    /// A coder for 4+2
    ///
    /// # Errors
    ///
    /// The matrix or the coder cannot be built, which no 4+2 does.
    pub fn new() -> Result<Self, ServerError> {
        // the matrix X4 chose, and the kernels a node would run
        let matrix = Matrix::cauchy(DATA_UNITS, PARITY_UNITS)
            .map_err(|error| std::io::Error::other(format!("x9 matrix: {error:?}")))?;
        let coder = rusty_erasure::coder(matrix)
            .map_err(|error| std::io::Error::other(format!("x9 coder: {error:?}")))?;
        Ok(StripeCoder {
            coder,
            digests: (0..DATA_UNITS + PARITY_UNITS)
                .map(|_| crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme))
                .collect(),
            sums: [0; DATA_UNITS + PARITY_UNITS],
        })
    }

    /// The kernel set the coder dispatched to, for the report
    #[must_use]
    pub fn kernels(&self) -> &'static str {
        self.coder.kernels().name
    }

    /// Checksum a piece of unit `index`, keeping the unit's checksum once its last piece is in
    ///
    /// A whole unit is checksummed in one call, as X5 measured it; a piece feeds the unit's digest.
    ///
    /// # Arguments
    ///
    /// * `index` - The unit, data then parity
    /// * `unit` - The unit's bytes
    /// * `bytes` - The piece
    fn checksum(&mut self, index: usize, unit: &[u8], bytes: &Range<usize>) {
        // a whole unit at once
        if bytes.start == 0 && bytes.end == unit.len() {
            self.sums[index] =
                std::hint::black_box(crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, unit));
            return;
        }
        // a piece, and the unit's checksum once it is the last
        self.digests[index].update(&unit[bytes.clone()]);
        if bytes.end == unit.len() {
            self.sums[index] = std::hint::black_box(self.digests[index].finalize_reset());
        }
    }

    /// Run one checksum or encode step over a stripe's units
    ///
    /// A receive is the caller's, since it writes into a buffer this does not own.
    ///
    /// # Arguments
    ///
    /// * `step` - The step to run
    /// * `data` - The stripe's data units
    /// * `parity` - The stripe's parity units
    pub fn run(&mut self, step: &StripeStep, data: &[&[u8]], parity: &mut [&mut [u8]]) {
        match step {
            // copied in by the caller
            StripeStep::Receive { .. } => {}
            // a piece of a data unit's checksum
            StripeStep::ChecksumData { unit, bytes } => self.checksum(*unit, data[*unit], bytes),
            // one slice of columns across every unit, into the same slice of each parity unit
            StripeStep::EncodeColumns(columns) => {
                let inputs: Vec<&[u8]> = data.iter().map(|unit| &unit[columns.clone()]).collect();
                let mut outputs: Vec<&mut [u8]> = parity
                    .iter_mut()
                    .map(|unit| &mut unit[columns.clone()])
                    .collect();
                // equal slices of a coder's own layout cannot be refused
                self.coder
                    .encode(&inputs, &mut outputs)
                    .expect("equal column slices of a 4+2 stripe encode");
            }
            // a piece of a parity unit's checksum
            StripeStep::ChecksumParity { unit, bytes } => {
                self.checksum(DATA_UNITS + *unit, parity[*unit], bytes);
            }
        }
    }

    /// The checksums of the last stripe, data then parity
    #[must_use]
    pub fn sums(&self) -> [u64; DATA_UNITS + PARITY_UNITS] {
        self.sums
    }
}

/// Fill `bytes` from SplitMix64 at `seed`, eight bytes a step, as X13's generator makes them
///
/// # Arguments
///
/// * `bytes` - What to fill
/// * `seed` - Where the sequence starts
pub fn fill_seeded(bytes: &mut [u8], seed: u64) {
    // the generator's state
    let mut state = seed;
    for chunk in bytes.chunks_mut(8) {
        // one SplitMix64 output
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        // as many of its bytes as fit
        chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
    }
}

/// A distribution of times, summarised
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Spread {
    /// How many samples
    pub n: usize,
    /// The median, in microseconds
    pub p50_us: f64,
    /// The 99th percentile, in microseconds
    pub p99_us: f64,
    /// The 99.9th percentile, in microseconds
    pub p999_us: f64,
    /// The largest, in microseconds
    pub max_us: f64,
}

/// Summarise samples in nanoseconds by nearest rank
///
/// # Arguments
///
/// * `samples` - The samples, in nanoseconds
#[must_use]
pub fn spread(samples: &[u32]) -> Spread {
    // nothing to summarise
    if samples.is_empty() {
        return Spread::default();
    }
    // in order, so a rank is an index
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = |q: f64| {
        let index = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
        f64::from(sorted[index]) / 1000.0
    };
    Spread {
        n: sorted.len(),
        p50_us: rank(0.50),
        p99_us: rank(0.99),
        p999_us: rank(0.999),
        max_us: f64::from(*sorted.last().expect("not empty")) / 1000.0,
    }
}

/// What one runner has done, kept in the order it happened
#[derive(Debug, Default)]
pub struct RunnerCounters {
    /// Stripes finished
    pub stripes: u64,
    /// Stripes started after they were due
    pub late: u64,
    /// Time dropped from the schedule past `MAX_BACKLOG`, in microseconds
    pub dropped_us: u64,
    /// Data bytes taken in
    pub data_bytes: u64,
    /// Bytes written to the pool, data and parity
    pub written_bytes: u64,
    /// Times the task offered the executor back between steps
    pub yields_offered: u64,
    /// Offers the executor took
    pub yields_taken: u64,
    /// Each receive step, in nanoseconds
    pub receive_ns: Vec<u32>,
    /// Each checksum step, in nanoseconds
    pub checksum_ns: Vec<u32>,
    /// Each encode step, in nanoseconds
    pub encode_ns: Vec<u32>,
    /// Each hold: the time the task kept the executor between real suspensions, in nanoseconds
    pub holds_ns: Vec<u32>,
    /// Each group sync, in nanoseconds
    pub syncs_ns: Vec<u32>,
    /// How late each stripe started, in microseconds
    pub lag_us: Vec<u32>,
}

/// Where a runner's counters stood at a mark
#[derive(Debug, Clone, Copy, Default)]
pub struct CounterMark {
    /// Stripes finished
    stripes: u64,
    /// Stripes started late
    late: u64,
    /// Time dropped from the schedule
    dropped_us: u64,
    /// Data bytes taken in
    data_bytes: u64,
    /// Bytes written
    written_bytes: u64,
    /// Yields offered
    yields_offered: u64,
    /// Yields taken
    yields_taken: u64,
    /// Receive samples so far
    receive: usize,
    /// Checksum samples so far
    checksum: usize,
    /// Encode samples so far
    encode: usize,
    /// Holds so far
    holds: usize,
    /// Syncs so far
    syncs: usize,
    /// Lag samples so far
    lags: usize,
}

/// What a runner did between two marks
#[derive(Debug, Clone, Default, Serialize)]
pub struct WindowFigures {
    /// The window's length in seconds
    pub secs: f64,
    /// Stripes finished
    pub stripes: u64,
    /// Stripes started late
    pub late: u64,
    /// Time dropped from the schedule past `MAX_BACKLOG`, in milliseconds
    pub dropped_ms: f64,
    /// Data bytes taken in
    pub data_bytes: u64,
    /// Data MiB a second
    pub data_mib_per_sec: f64,
    /// Bytes written, data and parity
    pub written_bytes: u64,
    /// Group syncs
    pub syncs: usize,
    /// Yields offered between steps
    pub yields_offered: u64,
    /// Yields taken
    pub yields_taken: u64,
    /// The latest a stripe started, in microseconds
    pub max_lag_us: u32,
    /// Every step together
    pub step: Spread,
    /// The receive steps
    pub receive: Spread,
    /// The checksum steps
    pub checksum: Spread,
    /// The encode steps
    pub encode: Spread,
    /// The holds between real suspensions
    pub hold: Spread,
    /// The group syncs
    pub sync: Spread,
}

impl RunnerCounters {
    /// Where the counters stand now
    #[must_use]
    pub fn mark(&self) -> CounterMark {
        CounterMark {
            stripes: self.stripes,
            late: self.late,
            dropped_us: self.dropped_us,
            data_bytes: self.data_bytes,
            written_bytes: self.written_bytes,
            yields_offered: self.yields_offered,
            yields_taken: self.yields_taken,
            receive: self.receive_ns.len(),
            checksum: self.checksum_ns.len(),
            encode: self.encode_ns.len(),
            holds: self.holds_ns.len(),
            syncs: self.syncs_ns.len(),
            lags: self.lag_us.len(),
        }
    }

    /// What happened between `from` and `to`, or from `from` to now
    ///
    /// # Arguments
    ///
    /// * `from` - Where the window starts
    /// * `to` - Where it ends, or now
    /// * `secs` - How long it lasted
    #[must_use]
    pub fn window(&self, from: &CounterMark, to: Option<&CounterMark>, secs: f64) -> WindowFigures {
        // the end, now when none is given
        let to = to.copied().unwrap_or_else(|| self.mark());
        // the samples that fell in the window
        let receive = &self.receive_ns[from.receive..to.receive];
        let checksum = &self.checksum_ns[from.checksum..to.checksum];
        let encode = &self.encode_ns[from.encode..to.encode];
        let mut every = Vec::with_capacity(receive.len() + checksum.len() + encode.len());
        every.extend_from_slice(receive);
        every.extend_from_slice(checksum);
        every.extend_from_slice(encode);
        let data_bytes = to.data_bytes - from.data_bytes;
        WindowFigures {
            secs,
            stripes: to.stripes - from.stripes,
            late: to.late - from.late,
            dropped_ms: (to.dropped_us - from.dropped_us) as f64 / 1000.0,
            data_bytes,
            data_mib_per_sec: if secs > 0.0 {
                data_bytes as f64 / f64::from(1 << 20) / secs
            } else {
                0.0
            },
            written_bytes: to.written_bytes - from.written_bytes,
            syncs: to.syncs - from.syncs,
            yields_offered: to.yields_offered - from.yields_offered,
            yields_taken: to.yields_taken - from.yields_taken,
            max_lag_us: self.lag_us[from.lags..to.lags]
                .iter()
                .copied()
                .max()
                .unwrap_or(0),
            step: spread(&every),
            receive: spread(receive),
            checksum: spread(checksum),
            encode: spread(encode),
            hold: spread(&self.holds_ns[from.holds..to.holds]),
            sync: spread(&self.syncs_ns[from.syncs..to.syncs]),
        }
    }
}

/// One runner of a run, shared between its executor and the pool
struct RunnerEntry {
    /// Its name in the report: `shard-<n>` or `core-<cpu>`
    label: String,
    /// Its rate, in data bytes a second
    rate_bytes: u64,
    /// The cpu it found itself on
    cpu: Mutex<Option<usize>>,
    /// When its schedule began
    origin: Mutex<Option<Instant>>,
    /// What it has done
    counters: Mutex<RunnerCounters>,
}

/// The edges of a workload's measured phase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Edge {
    /// The timed phase began
    MeasuredStart,
    /// The timed phase ended
    MeasuredEnd,
}

/// The run this process is doing, between `begin` and `write_report`
struct ActiveRun {
    /// What was asked for
    plan: ObjectWorkPlan,
    /// The cpus the shards run on
    shard_cpus: Vec<usize>,
    /// When the pool began the run
    started: Instant,
    /// The same moment on the wall clock
    started_at: SystemTime,
    /// The kernel set the coder dispatched to
    kernels: &'static str,
    /// Where crc-fast dispatched CRC-64/NVME
    crc_target: String,
    /// Every runner, in the order they registered
    runners: Vec<Arc<RunnerEntry>>,
    /// The measured phase's edges, with every runner's counters as they stood
    marks: Vec<(Edge, Instant, Vec<CounterMark>)>,
}

/// The run this process is doing, if any
static ACTIVE: Mutex<Option<ActiveRun>> = Mutex::new(None);

/// Lock the active run, whatever a panicking runner left it as
fn active() -> std::sync::MutexGuard<'static, Option<ActiveRun>> {
    ACTIVE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Begin a run, if this process's environment asks for one
///
/// Called by `ShoalPool::start` before any executor exists, so a plan that cannot run refuses
/// the pool before anything has started.
///
/// # Arguments
///
/// * `shard_cpus` - The cpus the shards will run on
///
/// # Errors
///
/// The environment names a plan that cannot run: a malformed variable, an own core that a shard
/// holds, or a pool on tmpfs, where glommio gives up direct I/O.
#[instrument(name = "x9::begin", skip_all, err(Debug))]
pub fn begin(shard_cpus: &[usize]) -> Result<(), ServerError> {
    // a pool started again in this process starts a new run
    let mut guard = active();
    *guard = None;
    // nothing asked for is nothing to do
    let Some(plan) = ObjectWorkPlan::from_env()? else {
        return Ok(());
    };
    // an own core cannot be one a shard runs on
    if let TaskPlacement::OwnCore(cpu) = plan.placement {
        if shard_cpus.contains(&cpu) {
            return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
                "{PLACE_ENV}=core:{cpu} names a cpu a shard runs on ({shard_cpus:?})"
            ))));
        }
    }
    // a pool where direct I/O is silently given up is a different pool
    refuse_tmpfs(&plan.dir)?;
    // what the coder and the checksum dispatch to on this cpu
    let kernels = StripeCoder::new()?.kernels();
    let crc_target = crc_fast::get_calculator_target(crc_fast::CrcAlgorithm::Crc64Nvme);
    event!(Level::WARN, msg = "x9 object work is on", placement = ?plan.placement,
        rate_mib = plan.rate_bytes >> 20, unit = plan.unit, kernels, crc_target);
    *guard = Some(ActiveRun {
        plan,
        shard_cpus: shard_cpus.to_vec(),
        started: Instant::now(),
        started_at: SystemTime::now(),
        kernels,
        crc_target,
        runners: Vec::new(),
        marks: Vec::new(),
    });
    Ok(())
}

/// Refuse a pool whose path, or its parent, is on tmpfs or devtmpfs
///
/// glommio opens a file there without `O_DIRECT` and says nothing, and a block device's node
/// under `/dev` is on devtmpfs, so it has to be made with `mknod` on a real filesystem.
///
/// # Arguments
///
/// * `dir` - The pool's directory or device node
fn refuse_tmpfs(dir: &Path) -> Result<(), ServerError> {
    // a device node is judged by the directory it sits in
    let judged = if std::fs::metadata(dir).is_ok_and(|meta| meta.file_type().is_block_device()) {
        dir.parent().unwrap_or(dir).to_path_buf()
    } else {
        dir.to_path_buf()
    };
    // the filesystem's magic
    let path = std::ffi::CString::new(judged.as_os_str().as_encoded_bytes())
        .map_err(|error| std::io::Error::other(format!("x9 pool path: {error}")))?;
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `path` is a NUL terminated string and `stat` a statfs this call fills
    if unsafe { libc::statfs(path.as_ptr(), &mut stat) } != 0 {
        return Err(ServerError::IO(std::io::Error::last_os_error()));
    }
    // tmpfs and devtmpfs share a magic
    if stat.f_type as i64 == libc::TMPFS_MAGIC as i64 {
        return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "{DIR_ENV}={} is on tmpfs, where glommio drops O_DIRECT; mknod a device node on a real filesystem",
            dir.display()
        ))));
    }
    Ok(())
}

/// Register a runner with the active run
///
/// # Arguments
///
/// * `label` - Its name in the report
/// * `rate_bytes` - Its rate
fn register(label: String, rate_bytes: u64) -> Option<(ObjectWorkPlan, Arc<RunnerEntry>, usize)> {
    // only while a run is active
    let mut guard = active();
    let run = guard.as_mut()?;
    let entry = Arc::new(RunnerEntry {
        label,
        rate_bytes,
        cpu: Mutex::new(None),
        origin: Mutex::new(None),
        counters: Mutex::new(RunnerCounters::default()),
    });
    run.runners.push(entry.clone());
    Some((run.plan.clone(), entry, run.runners.len() - 1))
}

/// Mark an edge of the measured phase, keeping where every runner stood
///
/// A no-op when no run is active.
///
/// # Arguments
///
/// * `edge` - Which edge
pub fn mark(edge: Edge) {
    // the moment, before any lock is waited on
    let now = Instant::now();
    let mut guard = active();
    if let Some(run) = guard.as_mut() {
        // every runner's counters as they stand
        let marks = run
            .runners
            .iter()
            .map(|runner| {
                runner
                    .counters
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .mark()
            })
            .collect();
        run.marks.push((edge, now, marks));
    }
}

/// Spawn this shard's runner in a third task queue, when the run asks for the table shards
///
/// # Arguments
///
/// * `shard_name` - The shard's name, for its queue's
/// * `shard_id` - The shard's number
/// * `shards` - How many shards share the node's rate
/// * `stop` - Set when the pool is shutting down
///
/// # Errors
///
/// The queue or the task cannot be made.
#[instrument(name = "x9::spawn_on_shard", skip_all, err(Debug))]
pub fn spawn_on_shard(
    shard_name: &str,
    shard_id: usize,
    shards: usize,
    stop: Arc<AtomicBool>,
) -> Result<Option<Task<Result<(), ServerError>>>, ServerError> {
    // only a run that asked for the table shards
    let wants = active().as_ref().map(|run| {
        (
            run.plan.placement,
            run.plan.rate_bytes,
            run.plan.queue_latency,
        )
    });
    let Some((TaskPlacement::TableShards, rate_bytes, queue_latency)) = wants else {
        return Ok(None);
    };
    // this shard's share of the node's rate
    let Some((plan, entry, index)) = register(
        format!("shard-{shard_id}"),
        rate_bytes / shards.max(1) as u64,
    ) else {
        return Ok(None);
    };
    // the third queue, below the high one's 1000 shares and the medium one's 500
    let queue = glommio::executor().create_task_queue(
        Shares::Static(LOW_PRIORITY_SHARES),
        queue_latency.map_or(Latency::NotImportant, Latency::Matters),
        &format!("LowPriority:{shard_name}"),
    );
    // the runner, in it
    let task = glommio::spawn_local_into(run_runner(plan, entry, index, stop), queue)?;
    Ok(Some(task))
}

/// The executor of a run on a core of its own
pub struct OwnCore {
    /// Its thread
    thread: glommio::ExecutorJoinHandle<Result<(), ServerError>>,
}

impl OwnCore {
    /// Wait for the executor to finish once its stop flag is set
    ///
    /// # Errors
    ///
    /// The runner failed, or its thread could not be joined.
    pub fn join(self) -> Result<(), ServerError> {
        // the thread's own result, and the runner's inside it
        self.thread
            .join()
            .map_err(|error| std::io::Error::other(format!("x9 executor: {error}")))?
    }
}

/// Start the run's own executor, when the run asks for a core of its own
///
/// Called once the shards are answering, so its rings never take a locked memory budget a shard
/// was owed.
///
/// # Arguments
///
/// * `stop` - Set when the pool is shutting down
///
/// # Errors
///
/// The executor cannot be built.
#[instrument(name = "x9::start_own_core", skip_all, err(Debug))]
pub fn start_own_core(stop: Arc<AtomicBool>) -> Result<Option<OwnCore>, ServerError> {
    // only a run that asked for a core of its own
    let wants = active()
        .as_ref()
        .map(|run| (run.plan.placement, run.plan.rate_bytes));
    let Some((TaskPlacement::OwnCore(cpu), rate_bytes)) = wants else {
        return Ok(None);
    };
    // the whole rate, on the one runner
    let Some((plan, entry, index)) = register(format!("core-{cpu}"), rate_bytes) else {
        return Ok(None);
    };
    // pinned as the control thread is; its buffers are global, so its rings need little
    let thread = LocalExecutorBuilder::new(Placement::Fixed(cpu))
        .name("shoal-x9")
        .io_memory(1 << 20)
        .spawn(move || run_runner(plan, entry, index, stop))?;
    Ok(Some(OwnCore { thread }))
}

/// The pool a runner writes to: a ring of slots in a file or on a device
struct Slice {
    /// The file or device
    file: Rc<DmaFile>,
    /// Where the ring begins
    base: u64,
    /// How many unit slots the ring holds
    slots: u64,
    /// The next slot to write
    next: u64,
    /// The unit, in bytes
    unit: u64,
}

impl Slice {
    /// The offset of the next slot, moving past it
    fn advance(&mut self) -> u64 {
        // round the ring
        let offset = self.base + (self.next % self.slots) * self.unit;
        self.next += 1;
        offset
    }
}

/// Open a runner's slice of the pool
///
/// A device is written at the runner's own range of it. A file is created once and written
/// through before it is used, as a slice keeps its file written ahead (X6), so no run's writes
/// convert unwritten extents; a file of the ring's size already there is reused as it is.
///
/// # Arguments
///
/// * `plan` - The run's plan
/// * `label` - The runner's name
/// * `index` - The runner's number in the run
async fn open_slice(
    plan: &ObjectWorkPlan,
    label: &str,
    index: usize,
) -> Result<Slice, ServerError> {
    // the ring's size in slots
    let slots = plan.ring_bytes / plan.unit as u64;
    // a block device: this runner's range of it
    if std::fs::metadata(&plan.dir).is_ok_and(|meta| meta.file_type().is_block_device()) {
        let file = OpenOptions::new().write(true).dma_open(&plan.dir).await?;
        return Ok(Slice {
            file: Rc::new(file),
            base: index as u64 * plan.ring_bytes,
            slots,
            next: 0,
            unit: plan.unit as u64,
        });
    }
    // a file of its own in the pool's directory
    let path = plan.dir.join(format!("x9-{label}.ring"));
    let ready = std::fs::metadata(&path).is_ok_and(|meta| meta.len() >= plan.ring_bytes);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .dma_open(&path)
        .await?;
    // written through once, in whole mebibytes, then synced
    if !ready {
        file.pre_allocate(plan.ring_bytes, false).await?;
        let chunk = (1usize << 20).min(plan.ring_bytes as usize);
        let mut offset = 0;
        while offset < plan.ring_bytes {
            let mut buffer = glommio::allocate_dma_buffer_global(chunk);
            buffer.as_bytes_mut().fill(0);
            file.write_at(buffer, offset).await?;
            offset += chunk as u64;
        }
        file.fdatasync().await?;
    }
    Ok(Slice {
        file: Rc::new(file),
        base: 0,
        slots,
        next: 0,
        unit: plan.unit as u64,
    })
}

/// A stripe's tally, folded into the runner's counters once the stripe is written
#[derive(Default)]
struct StripeTally {
    /// Receive steps
    receive_ns: Vec<u32>,
    /// Checksum steps
    checksum_ns: Vec<u32>,
    /// Encode steps
    encode_ns: Vec<u32>,
    /// Holds
    holds_ns: Vec<u32>,
    /// Yields offered
    yields_offered: u64,
    /// Yields taken
    yields_taken: u64,
}

impl StripeTally {
    /// Keep a step's time under its kind
    ///
    /// # Arguments
    ///
    /// * `kind` - The step's kind
    /// * `took` - How long it took
    fn step(&mut self, kind: StepKind, took: Duration) {
        let nanos = nanos(took);
        match kind {
            StepKind::Receive => self.receive_ns.push(nanos),
            StepKind::Checksum => self.checksum_ns.push(nanos),
            StepKind::Encode => self.encode_ns.push(nanos),
        }
    }
}

/// A duration in nanoseconds, saturating at four seconds
///
/// # Arguments
///
/// * `took` - The duration
fn nanos(took: Duration) -> u32 {
    u32::try_from(took.as_nanos()).unwrap_or(u32::MAX)
}

/// Run one runner until its stop flag is set
///
/// # Arguments
///
/// * `plan` - The run's plan
/// * `entry` - Where this runner's figures go
/// * `index` - Its number in the run
/// * `stop` - Set when the pool is shutting down
async fn run_runner(
    plan: ObjectWorkPlan,
    entry: Arc<RunnerEntry>,
    index: usize,
    stop: Arc<AtomicBool>,
) -> Result<(), ServerError> {
    // where it found itself
    // SAFETY: sched_getcpu takes nothing and returns the calling thread's cpu or -1
    let cpu = unsafe { libc::sched_getcpu() };
    *entry
        .cpu
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = usize::try_from(cpu).ok();
    // its slice of the pool
    let mut slice = open_slice(&plan, &entry.label, index).await?;
    // the bytes its units are copied in from: one unit seeded, and copies of it each marked
    // with their number. What the bytes are moves neither the checksum's cost nor the encode's;
    // that there are enough of them to leave every stripe's input cold in the cache does
    let unit = plan.unit;
    let mut seeded = glommio::allocate_dma_buffer_global(unit);
    fill_seeded(seeded.as_bytes_mut(), index as u64);
    let mut source = Vec::with_capacity((SOURCE_BYTES / unit).max(DATA_UNITS));
    for number in 0..(SOURCE_BYTES / unit).max(DATA_UNITS) {
        let mut buffer = glommio::allocate_dma_buffer_global(unit);
        buffer.as_bytes_mut().copy_from_slice(seeded.as_bytes());
        buffer.as_bytes_mut()[..8].copy_from_slice(&(number as u64).to_le_bytes());
        source.push(buffer);
        glommio::yield_if_needed().await;
    }
    // the stripe's six units, reused once each stripe's writes have released them
    let mut units: Vec<Rc<DmaBuffer>> = (0..DATA_UNITS + PARITY_UNITS)
        .map(|_| Rc::new(glommio::allocate_dma_buffer_global(unit)))
        .collect();
    let mut coder = StripeCoder::new()?;
    let steps = stripe_steps(unit, plan.step_bytes);
    // which source unit each data unit is being copied from
    let mut copying = [0usize; DATA_UNITS];
    // the group sync, beside it in its queue
    let written = Rc::new(Cell::new(0u64));
    let syncer = glommio::spawn_local_into(
        sync_on_interval(
            slice.file.clone(),
            written.clone(),
            entry.clone(),
            stop.clone(),
        ),
        glommio::executor().current_task_queue(),
    )?;
    // the schedule, from now
    let schedule = PacingSchedule::new(plan.stripe_data_bytes(), entry.rate_bytes);
    let mut origin = Instant::now();
    *entry
        .origin
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(origin);
    let mut dropped = Duration::ZERO;
    let mut tally = StripeTally::default();
    let mut next_source = 0usize;
    let mut stripe = 0u64;
    while !stop.load(Ordering::Relaxed) {
        // wait until this stripe is due, a slice at a time so a stop is seen
        let due = origin + schedule.due(stripe);
        let now = Instant::now();
        let lag = if due > now {
            glommio::timer::sleep((due - now).min(SLEEP_SLICE)).await;
            if Instant::now() < due {
                continue;
            }
            0
        } else if now - due > MAX_BACKLOG {
            // starved past the bound: the time beyond it is dropped, not owed
            let past = now - due - MAX_BACKLOG;
            origin += past;
            dropped += past;
            MAX_BACKLOG.as_micros()
        } else {
            (now - due).as_micros()
        };
        // the hold starts here: the executor came back to this task
        let mut resumed = Instant::now();
        // every step, offering the executor back between them
        for step in &steps {
            let began = Instant::now();
            match step {
                // a piece of a data unit copied in from the source, as a socket's receive copies it
                StripeStep::Receive { unit: data, bytes } => {
                    // a unit's first piece takes the next source unit
                    if bytes.start == 0 {
                        copying[*data] = next_source % source.len();
                        next_source += 1;
                    }
                    let target =
                        Rc::get_mut(&mut units[*data]).expect("the last stripe's writes are done");
                    target.as_bytes_mut()[bytes.clone()]
                        .copy_from_slice(&source[copying[*data]].as_bytes()[bytes.clone()]);
                }
                // a checksum or a slice of the encode
                _ => {
                    let (data_units, parity_units) = units.split_at_mut(DATA_UNITS);
                    let data: Vec<&[u8]> = data_units.iter().map(|unit| unit.as_bytes()).collect();
                    let mut parity: Vec<&mut [u8]> = parity_units
                        .iter_mut()
                        .map(|unit| {
                            Rc::get_mut(unit)
                                .expect("the last stripe's writes are done")
                                .as_bytes_mut()
                        })
                        .collect();
                    coder.run(step, &data, &mut parity);
                }
            }
            tally.step(step.kind(), began.elapsed());
            // offer the executor back, and count whether it was taken
            tally.yields_offered += 1;
            if glommio::executor().need_preempt() {
                tally.yields_taken += 1;
                tally.holds_ns.push(nanos(resumed.elapsed()));
                glommio::yield_if_needed().await;
                resumed = Instant::now();
            }
        }
        // the hold ends at the writes, which suspend the task
        tally.holds_ns.push(nanos(resumed.elapsed()));
        let offsets: Vec<u64> = units.iter().map(|_| slice.advance()).collect();
        let file = slice.file.clone();
        let writes = units
            .iter()
            .zip(offsets)
            .map(|(unit, offset)| file.write_rc_at(unit.clone(), offset));
        futures::future::try_join_all(writes).await?;
        written.set(written.get() + (unit * (DATA_UNITS + PARITY_UNITS)) as u64);
        // fold the stripe in, under one lock
        {
            let mut counters = entry
                .counters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            counters.stripes += 1;
            counters.late += u64::from(lag > 0);
            counters.dropped_us = u64::try_from(dropped.as_micros()).unwrap_or(u64::MAX);
            counters.data_bytes += (unit * DATA_UNITS) as u64;
            counters.written_bytes += (unit * (DATA_UNITS + PARITY_UNITS)) as u64;
            counters.yields_offered += tally.yields_offered;
            counters.yields_taken += tally.yields_taken;
            counters.receive_ns.append(&mut tally.receive_ns);
            counters.checksum_ns.append(&mut tally.checksum_ns);
            counters.encode_ns.append(&mut tally.encode_ns);
            counters.holds_ns.append(&mut tally.holds_ns);
            counters.lag_us.push(u32::try_from(lag).unwrap_or(u32::MAX));
        }
        tally.yields_offered = 0;
        tally.yields_taken = 0;
        stripe += 1;
    }
    // the sync stops on the same flag
    syncer.await;
    Ok(())
}

/// Sync what a runner wrote, every `SYNC_INTERVAL` that saw a write
///
/// # Arguments
///
/// * `file` - The runner's file or device
/// * `written` - The bytes it has written, which the runner moves
/// * `entry` - Where the syncs are counted
/// * `stop` - Set when the pool is shutting down
async fn sync_on_interval(
    file: Rc<DmaFile>,
    written: Rc<Cell<u64>>,
    entry: Arc<RunnerEntry>,
    stop: Arc<AtomicBool>,
) {
    // what had been written at the last sync
    let mut synced = 0;
    while !stop.load(Ordering::Relaxed) {
        glommio::timer::sleep(SYNC_INTERVAL).await;
        // nothing new since the last one
        let now = written.get();
        if now == synced {
            continue;
        }
        // one flush covering every write since
        let began = Instant::now();
        if let Err(error) = file.fdatasync().await {
            event!(Level::ERROR, msg = "x9 sync failed", %error);
            return;
        }
        synced = now;
        entry
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .syncs_ns
            .push(nanos(began.elapsed()));
    }
}

/// One runner's part of the report
#[derive(Debug, Serialize)]
pub struct RunnerReport {
    /// Its name
    pub label: String,
    /// The cpu it found itself on
    pub cpu: Option<usize>,
    /// Its rate, in data MiB a second
    pub rate_mib: f64,
    /// When its schedule began, after the run began, in milliseconds
    pub origin_ms: Option<f64>,
    /// Whether its schedule began before the measured phase did
    pub began_before_measured: Option<bool>,
    /// The whole run
    pub whole: WindowFigures,
    /// The measured phase alone
    pub measured: Option<WindowFigures>,
}

/// A run's report
#[derive(Debug, Serialize)]
pub struct ObjectWorkReport {
    /// Where the work ran
    pub placement: TaskPlacement,
    /// The third queue's latency goal, in microseconds, if it had one
    pub queue_latency_us: Option<u64>,
    /// The node's rate, in data MiB a second
    pub rate_mib: u64,
    /// The chunk unit in bytes
    pub unit: usize,
    /// The input bytes a step read
    pub step_bytes: usize,
    /// The layout
    pub data_units: usize,
    /// The layout's parity
    pub parity_units: usize,
    /// The pool
    pub dir: PathBuf,
    /// Each runner's ring in bytes
    pub ring_bytes: u64,
    /// The cpus the shards ran on
    pub shard_cpus: Vec<usize>,
    /// The kernel set the coder dispatched to
    pub kernels: &'static str,
    /// Where crc-fast dispatched
    pub crc_target: String,
    /// When the run began, in seconds since the epoch
    pub started_unix: f64,
    /// The measured phase's length in seconds, when both edges were marked
    pub measured_secs: Option<f64>,
    /// Every runner
    pub runners: Vec<RunnerReport>,
}

/// Write the active run's report, and end the run
///
/// A no-op when no run is active or no report was asked for. Called by `ShoalPool::exit` once
/// every runner has stopped.
///
/// # Errors
///
/// The report cannot be written.
#[instrument(name = "x9::write_report", skip_all, err(Debug))]
pub fn write_report() -> Result<(), ServerError> {
    // the run, ended
    let Some(run) = active().take() else {
        return Ok(());
    };
    let Some(path) = run.plan.report.clone() else {
        return Ok(());
    };
    // the measured phase's edges, if the harness marked them
    let start = run
        .marks
        .iter()
        .find(|(edge, ..)| *edge == Edge::MeasuredStart);
    let end = run
        .marks
        .iter()
        .find(|(edge, ..)| *edge == Edge::MeasuredEnd);
    let measured_secs = match (start, end) {
        (Some((_, from, _)), Some((_, to, _))) => Some((*to - *from).as_secs_f64()),
        _ => None,
    };
    let ended = Instant::now();
    // each runner's windows
    let runners = run
        .runners
        .iter()
        .enumerate()
        .map(|(index, runner)| {
            let counters = runner
                .counters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let origin = *runner
                .origin
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // the whole run, from its origin
            let whole_secs = origin.map_or(0.0, |origin| (ended - origin).as_secs_f64());
            let whole = counters.window(&CounterMark::default(), None, whole_secs);
            // the measured phase, between the marks this runner was in
            let measured = match (start, end, measured_secs) {
                (Some((_, _, from)), Some((_, _, to)), Some(secs)) => {
                    let from = from.get(index).copied().unwrap_or_default();
                    to.get(index)
                        .map(|to| counters.window(&from, Some(to), secs))
                }
                _ => None,
            };
            RunnerReport {
                label: runner.label.clone(),
                cpu: *runner
                    .cpu
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                rate_mib: runner.rate_bytes as f64 / f64::from(1 << 20),
                origin_ms: origin.map(|origin| (origin - run.started).as_secs_f64() * 1000.0),
                began_before_measured: match (origin, start) {
                    (Some(origin), Some((_, from, _))) => Some(origin <= *from),
                    _ => None,
                },
                whole,
                measured,
            }
        })
        .collect();
    let report = ObjectWorkReport {
        placement: run.plan.placement,
        queue_latency_us: run
            .plan
            .queue_latency
            .map(|latency| latency.as_micros() as u64),
        rate_mib: run.plan.rate_bytes >> 20,
        unit: run.plan.unit,
        step_bytes: run.plan.step_bytes,
        data_units: DATA_UNITS,
        parity_units: PARITY_UNITS,
        dir: run.plan.dir.clone(),
        ring_bytes: run.plan.ring_bytes,
        shard_cpus: run.shard_cpus.clone(),
        kernels: run.kernels,
        crc_target: run.crc_target.clone(),
        started_unix: run
            .started_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0.0, |since| since.as_secs_f64()),
        measured_secs,
        runners,
    };
    // written whole
    std::fs::write(&path, serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    /// A plan parsed from these variables
    ///
    /// # Arguments
    ///
    /// * `vars` - The variables, by name
    fn parse(vars: &[(&str, &str)]) -> Result<Option<ObjectWorkPlan>, String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        ObjectWorkPlan::parse(|key| vars.get(key).cloned())
    }

    /// The plan is read from its variables, and refuses what it cannot run by name
    #[test]
    fn the_plan_is_read_from_its_variables_and_refuses_what_it_cannot_run() {
        // nothing asked for, or off, is no plan
        assert_eq!(parse(&[]), Ok(None));
        assert_eq!(parse(&[(PLACE_ENV, "off")]), Ok(None));
        // a whole plan on the shards
        let plan = parse(&[
            (PLACE_ENV, "shards"),
            (RATE_ENV, "500"),
            (UNIT_ENV, "65536"),
            (DIR_ENV, "/pool"),
            (LATENCY_ENV, "250"),
        ])
        .expect("parses")
        .expect("a plan");
        assert_eq!(plan.placement, TaskPlacement::TableShards);
        assert_eq!(plan.rate_bytes, 500 << 20);
        assert_eq!(plan.runner_rate(2), 250 << 20);
        assert_eq!(plan.queue_latency, Some(Duration::from_micros(250)));
        assert_eq!(plan.ring_bytes, DEFAULT_RING_MIB << 20);
        // a step is a whole unit unless one is asked for
        assert_eq!(plan.step_bytes, 65536);
        let plan = parse(&[
            (PLACE_ENV, "shards"),
            (RATE_ENV, "500"),
            (UNIT_ENV, "1048576"),
            (DIR_ENV, "/pool"),
            (STEP_ENV, "64"),
        ])
        .expect("parses")
        .expect("a plan");
        assert_eq!(plan.step_bytes, 64 << 10);
        // a core of its own
        let plan = parse(&[
            (PLACE_ENV, "core:1"),
            (RATE_ENV, "100"),
            (UNIT_ENV, "1048576"),
            (DIR_ENV, "/pool"),
        ])
        .expect("parses")
        .expect("a plan");
        assert_eq!(plan.placement, TaskPlacement::OwnCore(1));
        assert_eq!(plan.queue_latency, None);
        // each refusal names what it refused
        let refused = |vars: &[(&str, &str)], names: &str| {
            let error = parse(vars).expect_err("refused");
            assert!(error.contains(names), "{error} does not name {names}");
        };
        refused(&[(PLACE_ENV, "elsewhere")], PLACE_ENV);
        refused(
            &[
                (PLACE_ENV, "shards"),
                (RATE_ENV, "100"),
                (UNIT_ENV, "65536"),
                (DIR_ENV, "/pool"),
                (STEP_ENV, "128"),
            ],
            STEP_ENV,
        );
        refused(
            &[
                (PLACE_ENV, "shards"),
                (RATE_ENV, "100"),
                (UNIT_ENV, "1048576"),
                (DIR_ENV, "/pool"),
                (STEP_ENV, "48"),
            ],
            STEP_ENV,
        );
        refused(&[(PLACE_ENV, "shards")], RATE_ENV);
        refused(&[(PLACE_ENV, "shards"), (RATE_ENV, "fast")], RATE_ENV);
        refused(
            &[(PLACE_ENV, "shards"), (RATE_ENV, "100"), (DIR_ENV, "/pool")],
            UNIT_ENV,
        );
        refused(
            &[
                (PLACE_ENV, "shards"),
                (RATE_ENV, "100"),
                (UNIT_ENV, "1000"),
                (DIR_ENV, "/pool"),
            ],
            UNIT_ENV,
        );
        refused(
            &[
                (PLACE_ENV, "shards"),
                (RATE_ENV, "100"),
                (UNIT_ENV, "65536"),
            ],
            DIR_ENV,
        );
        refused(
            &[
                (PLACE_ENV, "shards"),
                (RATE_ENV, "0"),
                (UNIT_ENV, "65536"),
                (DIR_ENV, "/pool"),
            ],
            RATE_ENV,
        );
        refused(
            &[
                (PLACE_ENV, "shards"),
                (RATE_ENV, "100"),
                (UNIT_ENV, "1048576"),
                (DIR_ENV, "/pool"),
                (RING_ENV, "1"),
            ],
            RING_ENV,
        );
    }

    /// A stripe is due once its bytes have been earned at the rate
    #[test]
    fn stripes_are_due_at_their_bytes_over_the_rate() {
        // 256 KiB stripes at 100 MiB/s: one every 2.5 ms
        let schedule = PacingSchedule::new(256 << 10, 100 << 20);
        assert_eq!(schedule.due(0), Duration::ZERO);
        assert_eq!(schedule.due(1), Duration::from_micros(2500));
        assert_eq!(schedule.due(400), Duration::from_secs(1));
        // 4 MiB stripes at 250 MiB/s, a shard's half of 500: one every 16 ms
        let schedule = PacingSchedule::new(4 << 20, 250 << 20);
        assert_eq!(schedule.due(1), Duration::from_millis(16));
        assert_eq!(schedule.due(125), Duration::from_secs(2));
    }

    /// A stripe's steps cover every byte once, a step's worth of input each: every unit received
    /// and checksummed, every column encoded, and each parity unit checksummed
    #[test]
    fn a_stripe_cut_into_steps_covers_every_byte_once() {
        for (unit, step) in [
            (64 << 10, 64 << 10),
            (1 << 20, 1 << 20),
            (1 << 20, 64 << 10),
            (64 << 10, 16 << 10),
        ] {
            let steps = stripe_steps(unit, step);
            // four data units received and checksummed, the columns encoded, two parity checksummed
            let pieces = unit / step;
            assert_eq!(
                steps.len(),
                pieces * (2 * DATA_UNITS + PARITY_UNITS) + unit / (step / DATA_UNITS)
            );
            // how often each byte of each unit was touched by each kind of step
            let mut received = vec![vec![0u8; unit]; DATA_UNITS];
            let mut summed = vec![vec![0u8; unit]; DATA_UNITS + PARITY_UNITS];
            let mut columns = vec![0u8; unit];
            for step_taken in &steps {
                let (seen, range) = match step_taken {
                    StripeStep::Receive { unit, bytes } => (&mut received[*unit], bytes.clone()),
                    StripeStep::ChecksumData { unit, bytes } => (&mut summed[*unit], bytes.clone()),
                    StripeStep::ChecksumParity { unit, bytes } => {
                        (&mut summed[DATA_UNITS + *unit], bytes.clone())
                    }
                    StripeStep::EncodeColumns(range) => {
                        // each slice is a step's worth of input across the four units
                        assert_eq!(range.len() * DATA_UNITS, step);
                        (&mut columns, range.clone())
                    }
                };
                // never more than a step's worth of input
                assert!(range.len() <= step);
                seen[range].iter_mut().for_each(|seen| *seen += 1);
            }
            assert!(
                columns.iter().all(|seen| *seen == 1),
                "a column encoded other than once"
            );
            assert!(
                received.iter().flatten().all(|seen| *seen == 1),
                "a byte received other than once"
            );
            assert!(
                summed.iter().flatten().all(|seen| *seen == 1),
                "a byte checksummed other than once"
            );
        }
    }

    /// A stripe encoded and checksummed in steps gives what one encode and one checksum a unit
    /// give, whether a step is a unit or a piece of one
    #[test]
    fn a_stripe_encoded_and_checksummed_in_steps_matches_one_call() {
        // CRC-64/NVME's published check value, so the algorithm is the one X5 chose
        assert_eq!(
            crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, b"123456789"),
            0xae8b_1486_0a79_9888
        );
        let unit = 64 << 10;
        // four seeded data units
        let data: Vec<Vec<u8>> = (0..DATA_UNITS)
            .map(|index| {
                let mut bytes = vec![0; unit];
                fill_seeded(&mut bytes, index as u64);
                bytes
            })
            .collect();
        let data_refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
        // the stripe in one call
        let whole = rusty_erasure::coder(Matrix::cauchy(DATA_UNITS, PARITY_UNITS).expect("matrix"))
            .expect("coder");
        let mut once = vec![vec![0u8; unit]; PARITY_UNITS];
        {
            let mut parity: Vec<&mut [u8]> = once.iter_mut().map(Vec::as_mut_slice).collect();
            whole.encode(&data_refs, &mut parity).expect("encodes");
        }
        // and in steps of a unit and of pieces, twice each so a digest left mid unit would show
        let mut coder = StripeCoder::new().expect("a 4+2 coder");
        for step in [unit, 16 << 10, unit, 16 << 10] {
            let mut stepped = vec![vec![0u8; unit]; PARITY_UNITS];
            for step_taken in stripe_steps(unit, step) {
                let mut parity: Vec<&mut [u8]> =
                    stepped.iter_mut().map(Vec::as_mut_slice).collect();
                coder.run(&step_taken, &data_refs, &mut parity);
            }
            assert_eq!(
                stepped, once,
                "the parity encoded in steps of {step} differs from one encode"
            );
            // every checksum is the unit's own
            let sums = coder.sums();
            for (index, unit_bytes) in data.iter().chain(once.iter()).enumerate() {
                assert_eq!(
                    sums[index],
                    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, unit_bytes),
                    "unit {index}'s checksum in steps of {step}"
                );
            }
        }
    }

    /// A window between two marks holds only what happened between them
    #[test]
    fn a_window_between_marks_holds_only_what_happened_in_it() {
        let mut counters = RunnerCounters::default();
        // before the window: one stripe with a long hold
        counters.stripes = 1;
        counters.data_bytes = 100;
        counters.holds_ns.push(9_000_000);
        counters.encode_ns.push(9_000_000);
        counters.lag_us.push(500);
        let from = counters.mark();
        // inside it: two stripes with short holds
        counters.stripes = 3;
        counters.data_bytes = 300;
        counters.holds_ns.extend([1_000, 2_000]);
        counters.encode_ns.extend([1_000, 2_000]);
        counters.lag_us.extend([0, 7]);
        counters.yields_offered = 10;
        counters.yields_taken = 4;
        let to = counters.mark();
        // after it: a stripe nobody should see
        counters.holds_ns.push(8_000_000);
        let window = counters.window(&from, Some(&to), 2.0);
        assert_eq!(window.stripes, 2);
        assert_eq!(window.data_bytes, 200);
        assert_eq!(window.hold.n, 2);
        assert!((window.hold.max_us - 2.0).abs() < 1e-9);
        assert_eq!(window.encode.n, 2);
        assert_eq!(window.max_lag_us, 7);
        assert_eq!(window.yields_offered, 10);
        assert_eq!(window.yields_taken, 4);
    }

    /// Nearest rank picks real samples
    #[test]
    fn a_spread_is_taken_by_nearest_rank() {
        let samples: Vec<u32> = (1..=1000).map(|n| n * 1000).collect();
        let spread = spread(&samples);
        assert_eq!(spread.n, 1000);
        assert!((spread.p50_us - 500.0).abs() < 1e-9);
        assert!((spread.p99_us - 990.0).abs() < 1e-9);
        assert!((spread.p999_us - 999.0).abs() < 1e-9);
        assert!((spread.max_us - 1000.0).abs() < 1e-9);
        assert_eq!(super::spread(&[]), Spread::default());
    }
}
