//! The device store's own work beside a workload's server, at a set rate
//!
//! What X9 asked with a spike's stand-in - what object work costs the tables beside it - asked
//! again of the store M14 built ([F85](../../../../docs/src/features/devices-and-pools.md#performance)).
//! When `SHOAL_OBJECT_LOAD_DEVICE` names a directory, the server this process starts is given a
//! device there, a pool of 4+2 over 64 KiB units whose chunks are 1 MiB, and an object executor on
//! `SHOAL_OBJECT_LOAD_CPU`; once it answers, a task on the client runtime stages a whole chunk on
//! that executor and applies it, at `SHOAL_OBJECT_LOAD_RATE_MIB` MiB of chunks a second, until the
//! server stops. The chunks are rewritten in a ring of `SHOAL_OBJECT_LOAD_RING_MIB`, so a long run
//! never fills its device, and each one's bytes are seeded so no two stripes are alike.
//!
//! The chunk's bytes are made by the load's own driver, which stands where M15's stream will: the
//! work the object executor does is the store's own - its unit checksums, the write into a file
//! written ahead, the record, the sync and the rename - and the free pool's refill behind it. The
//! driver is a task on the client's runtime, or with `SHOAL_OBJECT_LOAD_DRIVER_CPU` a thread of its
//! own pinned to that cpu, so the bytes it copies are never the measured client's cpu time.
//!
//! The harness marks the measured phase, and the report written when the server stops holds that
//! window apart from the seed and the drain. Configured by environment, as X9's work was, since a
//! configuration field would be a knob only a benchmark build reads. Built by `--features
//! object-load` and nothing else.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Serialize;
use shoal::server::objects::store::{ChunkId, StageWhole, StoreError};
use shoal::server::objects::ObjectStore;
use shoal::shared::devices::{ApplySync, DeviceSpec, SliceId};
use shoal::shared::objects::{stripe_key, Label};
use shoal::shared::pool_placement::pg_of;
use shoal::shared::pools::{ByteSize, PoolSpec, Redundancy};
use shoal::shared::rendezvous::mix64;

/// The directory the load's device is made in; unset, there is no load
pub const DEVICE_ENV: &str = "SHOAL_OBJECT_LOAD_DEVICE";
/// The cpu the object executor runs on, `1` unless set
pub const CPU_ENV: &str = "SHOAL_OBJECT_LOAD_CPU";
/// The load's rate in MiB of chunks a second; `0` starts the executor and drives nothing
pub const RATE_ENV: &str = "SHOAL_OBJECT_LOAD_RATE_MIB";
/// The ring of chunks rewritten, in MiB
pub const RING_ENV: &str = "SHOAL_OBJECT_LOAD_RING_MIB";
/// How an apply is made durable, `shared` or `each`
pub const SYNC_ENV: &str = "SHOAL_OBJECT_LOAD_SYNC";
/// The filesystem to report the device on, for a test whose directory is on one the store refuses
pub const FILESYSTEM_ENV: &str = "SHOAL_OBJECT_LOAD_FILESYSTEM";
/// Where the report is written when the server stops
pub const REPORT_ENV: &str = "SHOAL_OBJECT_LOAD_REPORT";
/// The cpu the load's driver thread is pinned to; unset, it is a task on the client's runtime
pub const DRIVER_CPU_ENV: &str = "SHOAL_OBJECT_LOAD_DRIVER_CPU";

/// The bytes of a chunk unit, X9's
pub const UNIT: u32 = 64 << 10;
/// The bytes of a chunk: a 4 MiB stripe over four data chunks
pub const CHUNK: u64 = 1 << 20;
/// The ring rewritten when none is asked for
pub const DEFAULT_RING_MIB: u64 = 256;
/// The furthest the schedule falls behind before the time past it is dropped, as X9's runner did
pub const MAX_BACKLOG: Duration = Duration::from_millis(50);
/// The consumer the load's chunks are bound as
const CONSUMER: u64 = 0x4f4c;
/// The seeded bytes chunks are cut from, rotated so no two chunks are alike
const SOURCE_BYTES: usize = 32 << 20;
/// The most latencies a report keeps of each kind
const MAX_SAMPLES: usize = 1 << 20;

/// What a run of the load was asked to do
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadPlan {
    /// The directory the device is made in
    pub dir: PathBuf,
    /// The object executor's cpu
    pub cpu: usize,
    /// The rate, in bytes of chunks a second
    pub rate_bytes: u64,
    /// The chunks of the ring
    pub ring: u64,
    /// How an apply is made durable
    pub sync: ApplySync,
    /// The filesystem to stand in, if any
    pub filesystem: Option<String>,
    /// Where the report goes
    pub report: Option<PathBuf>,
    /// The cpu the driver's thread is pinned to, if it has one of its own
    pub driver_cpu: Option<usize>,
}

impl LoadPlan {
    /// Read the plan from this process's environment, `None` when no load was asked for
    ///
    /// # Errors
    ///
    /// A variable that does not read as what it names.
    pub fn from_env() -> Result<Option<Self>> {
        Self::parse(|key| std::env::var(key).ok())
    }

    /// Read a plan from variables looked up by `read`
    ///
    /// # Arguments
    ///
    /// * `read` - Looks a variable up by name
    fn parse(read: impl Fn(&str) -> Option<String>) -> Result<Option<Self>> {
        // no device is no load
        let Some(dir) = read(DEVICE_ENV) else {
            return Ok(None);
        };
        // each number by its name
        let number = |key: &str, default: u64| -> Result<u64> {
            read(key).map_or(Ok(default), |value| {
                value.trim().parse().with_context(|| format!("{key}={value} is not a number"))
            })
        };
        let ring_mib = number(RING_ENV, DEFAULT_RING_MIB)?;
        if ring_mib == 0 {
            bail!("{RING_ENV} is zero: the load needs a chunk to write");
        }
        let sync = match read(SYNC_ENV).as_deref() {
            None | Some("shared") => ApplySync::Shared,
            Some("each") => ApplySync::Each,
            Some(other) => bail!("{SYNC_ENV}={other} is neither shared nor each"),
        };
        Ok(Some(LoadPlan {
            dir: PathBuf::from(dir),
            cpu: usize::try_from(number(CPU_ENV, 1)?)?,
            rate_bytes: number(RATE_ENV, 0)? << 20,
            ring: (ring_mib << 20) / CHUNK,
            sync,
            filesystem: read(FILESYSTEM_ENV),
            report: read(REPORT_ENV).map(PathBuf::from),
            driver_cpu: match read(DRIVER_CPU_ENV) {
                Some(cpu) => Some(cpu.trim().parse().with_context(|| format!("{DRIVER_CPU_ENV}={cpu} is not a cpu"))?),
                None => None,
            },
        }))
    }

    /// The device's own directory, made fresh under the one named
    #[must_use]
    pub fn device(&self) -> PathBuf {
        self.dir.join("object-load")
    }

    /// Give a server's configuration the load's device, pool and object executor
    ///
    /// The device's directory is emptied and made again, so every run claims a new device as a
    /// fresh storage root claims a new node.
    ///
    /// # Arguments
    ///
    /// * `conf` - The server's configuration
    ///
    /// # Errors
    ///
    /// When the directory cannot be made.
    pub fn configure(&self, conf: &mut shoal::Conf) -> Result<()> {
        // the device, new for this run
        let device = self.device();
        if device.exists() {
            std::fs::remove_dir_all(&device)
                .with_context(|| format!("emptying the load's device {}", device.display()))?;
        }
        std::fs::create_dir_all(&device)
            .with_context(|| format!("making the load's device {}", device.display()))?;
        // a test's directory stands on a filesystem the store refuses, which it is told to read as another
        if let Some(filesystem) = &self.filesystem {
            shoal::server::devices::probe::set_override(Some(shoal::server::devices::probe::ProbeOverride {
                filesystem: Some(filesystem.clone()),
                shared_filesystem: true,
                ..Default::default()
            }));
        }
        conf.storage.devices = vec![DeviceSpec::new(&device, "ssd").apply_sync(self.sync)];
        // a pool of its class, so the slice keeps files written ahead at the chunk's length
        let mut pool = PoolSpec::new(
            "ssd",
            Redundancy::Erasure {
                data: 4,
                parity: 2,
            },
        );
        pool.stripe = Some(ByteSize(4 * CHUNK));
        pool.unit = Some(ByteSize(u64::from(UNIT)));
        conf.storage.pools.insert("object-load".to_string(), pool);
        // and its executor
        conf.resources.object_cpus = vec![self.cpu];
        Ok(())
    }
}

/// Where the measured phase starts or ends
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edge {
    /// The workload's clock started
    MeasuredStart,
    /// The workload's clock stopped
    MeasuredEnd,
}

/// What the load did inside one window
#[derive(Debug, Clone, Default, Serialize)]
pub struct Window {
    /// The window's length
    pub secs: f64,
    /// The chunks staged and applied in it
    pub chunks: u64,
    /// Their bytes
    pub bytes: u64,
    /// The rate it reached
    pub mib_per_sec: f64,
    /// Each stage's time, in microseconds
    pub stage_us: Percentiles,
    /// Each apply's time
    pub apply_us: Percentiles,
    /// The furthest the schedule fell behind, in milliseconds
    pub behind_ms_max: f64,
    /// The operations refused, by the store's reason
    pub refused: std::collections::BTreeMap<String, u64>,
}

/// A distribution's points, in microseconds
#[derive(Debug, Clone, Default, Serialize)]
pub struct Percentiles {
    /// How many it holds
    pub n: u64,
    /// The median
    pub p50: u64,
    /// The 99th percentile
    pub p99: u64,
    /// The 99.9th percentile
    pub p999: u64,
    /// The longest
    pub max: u64,
}

impl Percentiles {
    /// The points of a set of samples
    ///
    /// # Arguments
    ///
    /// * `samples` - The samples, in any order
    fn of(mut samples: Vec<u64>) -> Self {
        samples.sort_unstable();
        // the sample at a quantile, the last one at one
        let at = |quantile: f64| -> u64 {
            if samples.is_empty() {
                return 0;
            }
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
            let index = ((samples.len() as f64 * quantile) as usize).min(samples.len() - 1);
            samples[index]
        };
        Percentiles {
            n: samples.len() as u64,
            p50: at(0.5),
            p99: at(0.99),
            p999: at(0.999),
            max: samples.last().copied().unwrap_or(0),
        }
    }
}

/// The report written when the server stops
#[derive(Debug, Clone, Serialize)]
pub struct LoadReport {
    /// The device's directory
    pub device: PathBuf,
    /// The cpu asked for
    pub cpu: usize,
    /// The cpu the driver's thread was pinned to, if it had one of its own
    pub driver_cpu: Option<usize>,
    /// The executors that served the device's slices, by cpu
    pub served_on: Vec<usize>,
    /// The rate asked for, in MiB a second
    pub rate_mib: u64,
    /// The chunk's bytes
    pub chunk_bytes: u64,
    /// The ring's chunks
    pub ring: u64,
    /// How an apply was made durable
    pub apply_sync: String,
    /// The whole run
    pub whole: Window,
    /// The measured phase alone
    pub measured: Window,
}

/// What the running load records, read by the report
#[derive(Debug, Default)]
struct Record {
    /// The chunks done, with the instant each finished, its stage and apply times and how far behind it ran
    done: Vec<(Instant, u64, u64, f64)>,
    /// The refusals, with the instant each came and the reason
    refused: Vec<(Instant, String)>,
    /// When the measured phase started and ended
    measured: (Option<Instant>, Option<Instant>),
}

/// A load running beside a server
pub struct Running {
    /// The plan it runs
    plan: LoadPlan,
    /// The store it drives, read for where its slice was served once it stops
    store: ObjectStore,
    /// The slice it writes
    slice: SliceId,
    /// When it started
    started: Instant,
    /// What it did
    record: Arc<Mutex<Record>>,
    /// Set to stop it
    stop: Arc<AtomicBool>,
    /// The task driving it, on the client's runtime
    task: Option<tokio::task::JoinHandle<()>>,
    /// Or the thread driving it, pinned to a cpu of its own
    thread: Option<std::thread::JoinHandle<()>>,
}

/// The load running in this process, between its start and its report
static RUNNING: Mutex<Option<Running>> = Mutex::new(None);

/// Start the load against a server that answers, on the client runtime
///
/// # Arguments
///
/// * `plan` - What was asked for
/// * `store` - The server's object store
/// * `runtime` - The client runtime the load is driven from
///
/// # Errors
///
/// When the server serves no slice for the load's device.
pub fn begin(plan: LoadPlan, store: &ObjectStore, runtime: &tokio::runtime::Runtime) -> Result<()> {
    // the one slice of the load's device, and the executors serving it
    let device = plan.device();
    let slices = store.slices();
    let (slice, _) = slices
        .iter()
        .find(|(_, path)| path.starts_with(&device))
        .context("the server serves no slice of the load's device")?;
    let slice = *slice;
    let record = Arc::new(Mutex::new(Record::default()));
    let stop = Arc::new(AtomicBool::new(false));
    // nothing driven at a rate of zero: the executor alone. The bytes are made here, before the
    // seed, so the load's first chunk is not late by them
    let mut task = None;
    let mut thread = None;
    if plan.rate_bytes > 0 {
        let driven = drive(
            store.clone(),
            slice,
            source(),
            plan.rate_bytes,
            plan.ring,
            record.clone(),
            stop.clone(),
        );
        match plan.driver_cpu {
            // a thread of its own on the cpu asked for, with a runtime of one thread
            Some(cpu) => {
                thread = Some(
                    std::thread::Builder::new()
                        .name("object-load".to_string())
                        .spawn(move || {
                            pin(cpu);
                            let runtime = tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .expect("the load's runtime starts");
                            runtime.block_on(driven);
                        })
                        .context("starting the load's driver thread")?,
                );
            }
            // or a task beside the client's
            None => task = Some(runtime.spawn(driven)),
        }
    }
    *RUNNING.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Running {
        plan,
        store: store.clone(),
        slice,
        started: Instant::now(),
        record,
        stop,
        task,
        thread,
    });
    Ok(())
}

/// Pin the calling thread to one cpu
///
/// # Arguments
///
/// * `cpu` - The cpu
fn pin(cpu: usize) {
    // SAFETY: a zeroed cpu set is a valid empty one, CPU_SET writes inside it for any cpu below
    // its size, and sched_setaffinity reads it for the calling thread alone
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        // a cpu outside the process's affinity is refused by the kernel, and the thread runs where it was
        let _ = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

/// Mark an edge of the measured phase
///
/// # Arguments
///
/// * `edge` - Which edge
pub fn mark(edge: Edge) {
    let running = RUNNING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(running) = running.as_ref() else {
        return;
    };
    let mut record = running.record.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    match edge {
        Edge::MeasuredStart => record.measured.0 = Some(Instant::now()),
        Edge::MeasuredEnd => record.measured.1 = Some(Instant::now()),
    }
}

/// Stop the load, before its server stops, and write its report
///
/// # Arguments
///
/// * `runtime` - The client runtime the load ran on
///
/// # Errors
///
/// When the report cannot be written.
pub fn finish(runtime: &tokio::runtime::Runtime) -> Result<Option<LoadReport>> {
    let Some(mut running) = RUNNING.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() else {
        return Ok(None);
    };
    // the task stops at its next chunk
    running.stop.store(true, Ordering::Relaxed);
    if let Some(task) = running.task.take() {
        let _ = runtime.block_on(task);
    }
    if let Some(thread) = running.thread.take() {
        let _ = thread.join();
    }
    let ended = Instant::now();
    let record = running.record.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    // the whole run, and the measured phase when both its edges were marked
    let whole = window(&record, running.started, ended);
    let measured = match record.measured {
        (Some(start), Some(end)) => window(&record, start, end),
        _ => Window::default(),
    };
    let report = LoadReport {
        device: running.plan.device(),
        cpu: running.plan.cpu,
        driver_cpu: running.plan.driver_cpu,
        served_on: running.store.served_on().get(&running.slice).into_iter().flatten().copied().collect(),
        rate_mib: running.plan.rate_bytes >> 20,
        chunk_bytes: CHUNK,
        ring: running.plan.ring,
        apply_sync: format!("{:?}", running.plan.sync).to_lowercase(),
        whole,
        measured,
    };
    // written where it was asked for
    if let Some(path) = &running.plan.report {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)
            .with_context(|| format!("writing the load's report {}", path.display()))?;
    }
    Ok(Some(report))
}

/// What the load did between two instants
///
/// # Arguments
///
/// * `record` - What it recorded
/// * `start` - The window's start
/// * `end` - Its end
fn window(record: &Record, start: Instant, end: Instant) -> Window {
    // the chunks that finished inside it
    let inside: Vec<&(Instant, u64, u64, f64)> = record
        .done
        .iter()
        .filter(|(at, ..)| *at >= start && *at <= end)
        .collect();
    let secs = end.saturating_duration_since(start).as_secs_f64();
    let chunks = inside.len() as u64;
    let bytes = chunks * CHUNK;
    let mut refused = std::collections::BTreeMap::new();
    for (_, reason) in record.refused.iter().filter(|(at, _)| *at >= start && *at <= end) {
        *refused.entry(reason.clone()).or_insert(0) += 1;
    }
    #[allow(clippy::cast_precision_loss)]
    Window {
        secs,
        chunks,
        bytes,
        mib_per_sec: if secs > 0.0 { bytes as f64 / f64::from(1 << 20) / secs } else { 0.0 },
        stage_us: Percentiles::of(inside.iter().map(|(_, stage, ..)| *stage).collect()),
        apply_us: Percentiles::of(inside.iter().map(|(_, _, apply, _)| *apply).collect()),
        behind_ms_max: inside.iter().map(|(.., behind)| *behind).fold(0.0, f64::max),
        refused,
    }
}

/// Seeded bytes the chunks are cut from
fn source() -> Vec<u8> {
    // eight bytes a draw, each draw its index mixed
    (0..SOURCE_BYTES / 8)
        .flat_map(|index| mix64(index as u64 ^ 0x6f62_6a65_6374).to_le_bytes())
        .collect()
}

/// Stage and apply whole chunks at a rate until told to stop
///
/// # Arguments
///
/// * `store` - The server's object store
/// * `slice` - The slice written
/// * `source` - The seeded bytes chunks are cut from
/// * `rate_bytes` - The rate, in bytes of chunks a second
/// * `ring` - The chunks rewritten
/// * `record` - Where what it did is recorded
/// * `stop` - Set to stop it
async fn drive(
    store: ObjectStore,
    slice: SliceId,
    source: Vec<u8>,
    rate_bytes: u64,
    ring: u64,
    record: Arc<Mutex<Record>>,
    stop: Arc<AtomicBool>,
) {
    // one chunk every so long at the rate
    #[allow(clippy::cast_precision_loss)]
    let every = Duration::from_secs_f64(CHUNK as f64 / rate_bytes as f64);
    let mut due = Instant::now();
    let mut written: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        // wait for the chunk's turn, and drop the time past the backlog a starved load owes
        let now = Instant::now();
        if due > now {
            tokio::time::sleep(due - now).await;
        } else if now - due > MAX_BACKLOG {
            due = now - MAX_BACKLOG;
        }
        let behind = Instant::now().saturating_duration_since(due).as_secs_f64() * 1000.0;
        // the next chunk of the ring, at a label newer than the one it holds
        let object = u128::from(written % ring);
        let id = ChunkId {
            consumer: CONSUMER,
            pg: pg_of(stripe_key(CONSUMER, object, 0), 1),
            object,
            stripe: 0,
            position: 0,
        };
        let label = Label {
            sequence: written / ring + 1,
            tag: 0,
        };
        // a chunk of the source, at an offset of its own
        #[allow(clippy::cast_possible_truncation)]
        let offset = (mix64(written) as usize % (SOURCE_BYTES - CHUNK as usize)) & !7;
        let bytes = source[offset..offset + CHUNK as usize].to_vec();
        // staged, then applied, each timed
        let began = Instant::now();
        let staged = store
            .stage_whole(
                slice,
                StageWhole {
                    id,
                    label,
                    chunk_bytes: CHUNK,
                    unit_bytes: UNIT,
                    length: CHUNK,
                    bytes,
                },
            )
            .await;
        let stage_us = micros(began.elapsed());
        let applied = match staged {
            Ok(()) => {
                let began = Instant::now();
                store.apply(slice, id, label).await.map(|()| micros(began.elapsed()))
            }
            Err(error) => Err(error),
        };
        // recorded, and the next chunk due one interval on
        let mut record = record.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match applied {
            Ok(apply_us) if record.done.len() < MAX_SAMPLES => {
                record.done.push((Instant::now(), stage_us, apply_us, behind));
            }
            Ok(_) => (),
            Err(error) => record.refused.push((Instant::now(), reason(&error))),
        }
        written += 1;
        due += every;
    }
}

/// A duration in whole microseconds
///
/// # Arguments
///
/// * `elapsed` - The duration
fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

/// A refusal's name, without what varies between two of the same kind
///
/// # Arguments
///
/// * `error` - The store's refusal
fn reason(error: &StoreError) -> String {
    // the variant's name, which is what the report groups by
    format!("{error:?}")
        .split(['(', ' ', '{'])
        .next()
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::{LoadPlan, Percentiles, CHUNK, DEVICE_ENV, RATE_ENV, RING_ENV, SYNC_ENV};
    use shoal::shared::devices::ApplySync;
    use std::collections::HashMap;

    /// The plan reads from its variables, nothing named is no load, and a bad one is refused by name
    #[test]
    fn the_plan_reads_from_its_variables() {
        let read = |vars: &[(&str, &str)]| {
            let vars: HashMap<String, String> =
                vars.iter().map(|(key, value)| ((*key).to_string(), (*value).to_string())).collect();
            LoadPlan::parse(move |key| vars.get(key).cloned())
        };
        assert!(read(&[]).unwrap().is_none());
        let plan = read(&[(DEVICE_ENV, "/xfs"), (RATE_ENV, "100"), (RING_ENV, "64"), (SYNC_ENV, "each")])
            .unwrap()
            .unwrap();
        assert_eq!(plan.cpu, 1);
        assert_eq!(plan.rate_bytes, 100 << 20);
        assert_eq!(plan.ring, (64 << 20) / CHUNK);
        assert_eq!(plan.sync, ApplySync::Each);
        assert_eq!(plan.device(), std::path::Path::new("/xfs/object-load"));
        assert_eq!(plan.driver_cpu, None);
        let pinned = read(&[(DEVICE_ENV, "/xfs"), (super::DRIVER_CPU_ENV, "0")]).unwrap().unwrap();
        assert_eq!(pinned.driver_cpu, Some(0));
        let refused = read(&[(DEVICE_ENV, "/xfs"), (SYNC_ENV, "sometimes")]).unwrap_err().to_string();
        assert!(refused.contains("SHOAL_OBJECT_LOAD_SYNC"), "{refused}");
    }

    /// Percentiles are read from sorted samples, and none is all zeros
    #[test]
    fn percentiles_are_read_from_the_samples() {
        let points = Percentiles::of((1..=1000).rev().collect());
        assert_eq!((points.n, points.p50, points.p99, points.max), (1000, 501, 991, 1000));
        assert_eq!(Percentiles::of(Vec::new()).max, 0);
    }
}
