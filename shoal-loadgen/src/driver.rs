//! The driver: workers that keep bundles of queries in flight against a cluster and record
//! every answer
//!
//! Each worker opens an unordered stream on one member's client, fills a bundle with the
//! queries its picker chooses, sends it once it holds `bundle` queries, and keeps topping up
//! until `in_flight` queries are outstanding. Every answer is recorded in the second it
//! arrived, in the worker's own window for that second, so a worker never contends with
//! another; a reporter adds the workers' windows up once a second and sends the sum on.
//!
//! A stream that fails records everything still outstanding on it as failed, waits, and is
//! opened again, so a fault shows up as the seconds it lasted rather than as the end of the
//! run. The load is closed: a worker sends more only as answers come back, so this measures
//! what the cluster sustains at a depth, not what it does at an offered rate.
//!
//! Everything here is generic over the schema's client type. The bounds every function repeats
//! are the ones the client itself needs to read an answer and to hold a stream across a task.
//!
//! Since [F69](../../docs/src/features/driver-operation-kinds.md) a driver can be handed kinds of
//! operation beside read and insert ([`OperationKind`]), which it weighs, picks, times and
//! reports as it does its own two knowing nothing else about them, and every window counts the
//! bytes its streams sent and received on the wire: each bundle a stream wrote, and each answer
//! it read, its kind's and any it was not owed alike.
//!
//! Since [F72](../../docs/src/features/bench-paced-stream.md) an arm can be paced instead: each
//! worker sends its operations on a schedule at an offered rate rather than as answers return,
//! and an operation's latency counts from when it was due rather than from when it went out, so
//! a stall shows as latency instead of as fewer operations. The bench runs one such stream
//! against one table beside its closed loop.

use rkyv::Archive;
use shoal::client::{SendOptions, ShoalQueryStream};
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::dataset::OperationKind;
use shoal::shared::protocol::read::ReadLevel as WireReadLevel;
use shoal::shared::queries::Queries;
use shoal::shared::traits::QuerySupport;
use shoal::{Errors, QuerySuceededOpts, Shoal};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::feed::{FeedFacts, FeedRange, TableSource, Take};
use crate::pick::{Pick, Picker};
use crate::progress::{BenchEvent, Progress};
use crate::results::{EndedEarly, SecondPhase, SecondSample, VerifyFacts};
use crate::spec::{OnExhaust, ReadLevel};
use crate::window::{OpKind, Outcome, Window};

/// How long a worker waits for its next answer before it says what it is still owed
const HUNG_CHECK: Duration = Duration::from_secs(10);

/// How long a stream may go without an answer, while owed one, before it is given up as hung
const HUNG_AFTER: Duration = Duration::from_secs(60);

/// How long a worker waits before opening a stream again after one failed
const REOPEN_AFTER: Duration = Duration::from_millis(200);

/// How long a worker waits on a feed with nothing parsed before asking again
const FEED_POLL: Duration = Duration::from_millis(1);

/// The longest a paced worker with nothing owed sleeps before looking again, so an arm that
/// ends between two of its slots is noticed
const PACE_CHECK: Duration = Duration::from_millis(100);

/// How many times a preload's or a read back's query is sent again after a retriable failure
///
/// A cluster just bootstrapped admits writes before every group has settled, and the first few
/// seconds of a preload can answer `OutcomeUnknown`; a preload has to load every row, so it
/// retries the way the tmdb loader does.
pub const LOAD_RETRIES: u32 = 8;

/// The first wait before a query is sent again
const RETRY_FIRST: Duration = Duration::from_millis(20);

/// The longest wait before a query is sent again
const RETRY_CAP: Duration = Duration::from_millis(500);

/// Whether a failure says that sending the query again may succeed
///
/// The codes the client's own retry repeats a bundle on: turned away before anything ran, a
/// leader that is not one, a quorum that is not there, a lost connection, a deadline, an
/// outcome that is unknown, and a route a stale map chose, which a client routing by topology
/// meets after every move ([Resolved #220](../../docs/src/appendix/resolved/stale-topology-retried.md)).
/// A retry is a new query, which is safe because a benchmark's every write is an insert of a
/// whole row, and a read changes nothing.
///
/// # Arguments
///
/// * `code` - The code the query failed with
#[must_use]
pub fn retriable(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::OutcomeUnknown
            | ErrorCode::Shedding
            | ErrorCode::NotLeader
            | ErrorCode::Unavailable
            | ErrorCode::QuorumUnavailable
            | ErrorCode::ConnectionLost
            | ErrorCode::Timeout
            | ErrorCode::StaleTopology
    )
}

/// How long to wait before sending a query again, after it has been sent `attempts` times
///
/// # Arguments
///
/// * `attempts` - How many times it has been sent
fn backoff(attempts: u32) -> Duration {
    // doubling from the first wait, never past the cap
    RETRY_FIRST
        .saturating_mul(1 << attempts.saturating_sub(1).min(8))
        .min(RETRY_CAP)
}

/// The send options that ask for a read level
///
/// # Arguments
///
/// * `level` - The level asked for
#[must_use]
pub fn send_options(level: ReadLevel) -> SendOptions {
    // the default leaves it to the table and the cluster
    match level {
        ReadLevel::Default => SendOptions::new(),
        ReadLevel::One => SendOptions::new().read(WireReadLevel::One),
        ReadLevel::Quorum => SendOptions::new().read(WireReadLevel::Quorum),
    }
}

/// When a paced worker's operation is due
///
/// The workers' operations interleave on one schedule at the offered rate: worker `w`'s `k`th
/// operation is the `k·workers + w`th of the arm, so however many workers share the rate it is
/// offered evenly, and every slot is fixed from the start rather than from the last send.
///
/// # Arguments
///
/// * `started` - When the arm started
/// * `worker` - Which worker
/// * `workers` - How many workers share the rate
/// * `per_sec` - The rate offered, operations a second over every worker
/// * `index` - Which of this worker's operations
#[must_use]
pub fn slot(started: Instant, worker: usize, workers: usize, per_sec: f64, index: u64) -> Instant {
    // the operation's place in the whole arm's sequence, over the rate
    let place = index as f64 * workers.max(1) as f64 + worker as f64;
    started + Duration::from_secs_f64(place / per_sec)
}

/// An arm's clock: when it started, when it is due to stop, and whether it was stopped early
#[derive(Debug)]
pub struct ArmClock {
    /// When the arm started; every second and mark is counted from here
    started: Instant,
    /// When workers stop sending, in milliseconds since the start
    deadline_ms: AtomicU64,
    /// Whether the arm was stopped before its deadline
    stopped: AtomicBool,
    /// Why, and when
    ended: Mutex<Option<EndedEarly>>,
}

impl ArmClock {
    /// A clock that starts now and is due to stop after a while
    ///
    /// # Arguments
    ///
    /// * `until` - How long until workers stop sending
    #[must_use]
    pub fn start(until: Duration) -> Arc<Self> {
        Arc::new(ArmClock {
            started: Instant::now(),
            deadline_ms: AtomicU64::new(until.as_millis() as u64),
            stopped: AtomicBool::new(false),
            ended: Mutex::new(None),
        })
    }

    /// When the arm started
    #[must_use]
    pub fn started(&self) -> Instant {
        self.started
    }

    /// Milliseconds since the arm started
    #[must_use]
    pub fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// The second of the arm it is now
    #[must_use]
    pub fn second(&self) -> usize {
        self.started.elapsed().as_secs() as usize
    }

    /// Whether workers should still be sending
    #[must_use]
    pub fn live(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
            && self.elapsed_ms() < self.deadline_ms.load(Ordering::Relaxed)
    }

    /// Move the deadline, so an event still running keeps the arm going
    ///
    /// # Arguments
    ///
    /// * `until` - The new deadline, since the start
    pub fn extend_to(&self, until: Duration) {
        // only ever later
        self.deadline_ms
            .fetch_max(until.as_millis() as u64, Ordering::Relaxed);
    }

    /// Stop the arm now, saying why; the first reason given is the one kept
    ///
    /// # Arguments
    ///
    /// * `reason` - Why the arm stopped early
    pub fn stop(&self, reason: &str) {
        // the first stop wins
        if !self.stopped.swap(true, Ordering::Relaxed) {
            *lock(&self.ended) = Some(EndedEarly {
                reason: reason.to_string(),
                at_secs: self.started.elapsed().as_secs_f64(),
            });
        }
    }

    /// When workers stop sending, since the start: the arm's time, or later if an event kept it
    /// running
    #[must_use]
    pub fn deadline(&self) -> Duration {
        Duration::from_millis(self.deadline_ms.load(Ordering::Relaxed))
    }

    /// Why the arm ended early, if it did
    #[must_use]
    pub fn ended_early(&self) -> Option<EndedEarly> {
        lock(&self.ended).clone()
    }
}

/// Lock a mutex, taking over its value if a thread panicked holding it
///
/// # Arguments
///
/// * `mutex` - The mutex to lock
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// What a worker sends
enum Work<K> {
    /// What a picker chooses, for a measured arm
    Arm(Arc<Picker>),
    /// Every table's open feed until each is spent, for a preload
    Load,
    /// A fixed list of reads, until it is empty, for a read back
    Queries(Arc<Mutex<VecDeque<K>>>),
}

impl<K> Clone for Work<K> {
    /// Clone the handle to the work, not the work
    fn clone(&self) -> Self {
        match self {
            Work::Arm(picker) => Work::Arm(picker.clone()),
            Work::Load => Work::Load,
            Work::Queries(queue) => Work::Queries(queue.clone()),
        }
    }
}

/// A kind of operation the driver was handed, as its workers use it
///
/// The [`OperationKind`] it came from, reduced to what a worker that knows only the query type
/// needs: its name, how to build one operation's query, and what its answer has to show.
struct Supplied<K> {
    /// What the kind is called
    name: Arc<str>,
    /// Build one operation's query from its seed
    build: Arc<dyn Fn(u64) -> K + Send + Sync>,
    /// What an answer has to show to count as done rather than as a miss
    expect: QuerySuceededOpts,
}

impl<K> Clone for Supplied<K> {
    /// Clone the handle to the kind, not the kind
    fn clone(&self) -> Self {
        Supplied {
            name: self.name.clone(),
            build: self.build.clone(),
            expect: self.expect,
        }
    }
}

/// What every worker of one arm shares
struct Shared<K> {
    /// The arm's clock
    clock: Arc<ArmClock>,
    /// Every table, in dataset order
    tables: Vec<Arc<dyn TableSource<K>>>,
    /// Each worker's window a second, by second
    seconds: Vec<Mutex<Vec<Window>>>,
    /// What to do when a feed is spent
    on_exhaust: OnExhaust,
    /// How many times a query that failed in a retriable way is sent again
    retries: u32,
    /// How many queries a bundle holds
    bundle: usize,
    /// How many queries a worker keeps outstanding
    in_flight: usize,
    /// What the workers send
    work: Work<K>,
    /// The kinds the driver was handed, in its order
    kinds: Vec<Supplied<K>>,
    /// The rate a paced arm's operations are offered at, a second over every worker
    pace: Option<f64>,
}

impl<K> Shared<K> {
    /// When a worker's operation is due, if the arm is paced
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `index` - Which of its operations
    fn slot(&self, worker: usize, index: u64) -> Option<Instant> {
        // every worker has its own windows, so their count is the worker count
        self.pace
            .map(|per_sec| slot(self.clock.started(), worker, self.seconds.len(), per_sec, index))
    }

    /// The window for the second it is now, of one worker, made if this is its first record
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `record` - What to do with the window
    fn with_window(&self, worker: usize, record: impl FnOnce(&mut Window)) {
        let second = self.clock.second();
        let mut windows = lock(&self.seconds[worker]);
        if windows.len() <= second {
            windows.resize_with(second + 1, Window::default);
        }
        record(&mut windows[second]);
    }

    /// Record the bytes a bundle took on the wire
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `bytes` - How many, frame header included
    fn record_sent(&self, worker: usize, bytes: u64) {
        self.with_window(worker, |window| window.bytes_sent += bytes);
    }

    /// Record the bytes an answer took on the wire
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `kind` - What it answered, if the stream owed it
    /// * `bytes` - How many, frame header included
    fn record_received(&self, worker: usize, kind: Option<&OpKind>, bytes: u64) {
        self.with_window(worker, |window| window.record_received(kind, bytes));
    }

    /// Record how an operation ended, in this second of this worker's windows
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `kind` - What kind of operation
    /// * `outcome` - How it ended
    fn record(&self, worker: usize, kind: &OpKind, outcome: Outcome) {
        // the window for the second it is now, made if this is its first answer
        self.with_window(worker, |window| window.record(kind, outcome));
    }

    /// Record that a query is being sent again
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `kind` - What kind of operation
    fn record_retry(&self, worker: usize, kind: &OpKind) {
        self.with_window(worker, |window| window.record(kind, Outcome::Retried));
    }

    /// Record a bundle whose answers are all in
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `latency` - From its send to its last answer
    fn record_bundle(&self, worker: usize, latency: Duration) {
        let second = self.clock.second();
        let mut windows = lock(&self.seconds[worker]);
        if windows.len() <= second {
            windows.resize_with(second + 1, Window::default);
        }
        windows[second].record_bundle(latency);
    }

    /// Record time spent waiting on a feed
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `waited` - How long
    fn record_wait(&self, worker: usize, waited: Duration) {
        let second = self.clock.second();
        let mut windows = lock(&self.seconds[worker]);
        if windows.len() <= second {
            windows.resize_with(second + 1, Window::default);
        }
        windows[second].feed_wait_us += waited.as_micros() as u64;
    }

    /// Every worker's windows of one second, added up
    ///
    /// # Arguments
    ///
    /// * `second` - The second
    fn second(&self, second: usize) -> Window {
        // a worker with no answers that second adds nothing
        let mut total = Window::default();
        for windows in &self.seconds {
            if let Some(window) = lock(windows).get(second) {
                total.add(window);
            }
        }
        total
    }

    /// How many seconds any worker recorded in
    fn len(&self) -> usize {
        self.seconds
            .iter()
            .map(|windows| lock(windows).len())
            .max()
            .unwrap_or(0)
    }
}

/// One query that was sent and not yet answered
struct Sent<K> {
    /// What kind of operation it was
    kind: OpKind,
    /// The bundle it went in
    bundle: u64,
    /// The insert to acknowledge if it succeeds: its table and sequence
    ack: Option<(usize, u64)>,
    /// A copy to send again, kept only when retries are allowed
    copy: Option<K>,
    /// How many times it has been sent, this time included
    attempts: u32,
}

/// One query staged in a bundle
struct Staged<K> {
    /// What kind of operation it is
    kind: OpKind,
    /// The insert to acknowledge if it succeeds
    ack: Option<(usize, u64)>,
    /// A copy to send again, kept only when retries are allowed
    copy: Option<K>,
    /// How many times it has been sent before
    attempts: u32,
    /// When a paced arm's operation was due, which its latency counts from
    due: Option<Instant>,
}

/// A query waiting to be sent again
struct Retry<K> {
    /// The query
    query: K,
    /// What kind of operation it is
    kind: OpKind,
    /// The insert to acknowledge if it succeeds
    ack: Option<(usize, u64)>,
    /// How many times it has been sent
    attempts: u32,
    /// When it may be sent again
    at: Instant,
}

/// One bundle that was sent and not all answered
struct Bundle {
    /// When it was sent
    at: Instant,
    /// How many of its queries are still owed an answer
    remaining: usize,
}

/// What a worker's stream is doing, kept across the streams it opens
struct Cursor {
    /// The worker
    worker: usize,
    /// The index of the next operation it chooses
    index: u64,
    /// The bundles it has sent
    bundles: u64,
    /// Whether it has nothing more to send
    spent: bool,
}

/// What one arm did
#[derive(Debug)]
pub struct ArmOutcome {
    /// Every worker's windows, added up, a second
    pub seconds: Vec<Window>,
    /// How busy the driver's process was each second, in percent of one cpu
    pub cpu: Vec<f64>,
    /// Why the arm ended before its time, if it did
    pub ended_early: Option<EndedEarly>,
    /// When its workers were due to stop, since it started: its time, or later if an event kept
    /// it running
    pub deadline: Duration,
    /// What each table's feed did, by table
    pub feeds: Vec<(String, FeedFacts)>,
}

/// How one arm is driven
#[derive(Debug, Clone)]
pub struct ArmSettings {
    /// How many queries a bundle holds
    pub bundle: usize,
    /// How many queries a worker keeps outstanding
    pub in_flight: usize,
    /// How long the arm runs before it is measured
    pub warmup: Duration,
    /// How long it is measured
    pub duration: Duration,
    /// What it does when its inserts run out
    pub on_exhaust: OnExhaust,
    /// How many times a query that failed in a retriable way is sent again
    pub retries: u32,
    /// What its workers choose
    pub picker: Picker,
    /// Whether it inserts at all, so its feeds are opened
    pub inserts: bool,
    /// The rate its operations are offered at, a second over every worker, for a paced arm;
    /// none for a closed loop, which sends as answers come back
    /// ([F72](../../docs/src/features/bench-paced-stream.md))
    pub pace: Option<f64>,
}

/// The process's own cpu time so far, in clock ticks, from `/proc/self/stat`
fn cpu_ticks() -> Option<u64> {
    // utime and stime are fields 14 and 15, counted after the parenthesised command name
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let after = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

/// The kernel's clock ticks a second, which Linux has kept at one hundred for user space
const TICKS_PER_SEC: f64 = 100.0;

/// Drives one schema's cluster through its members' clients
pub struct Driver<S: QuerySupport> {
    /// One client a member, in the order workers are spread over them
    clients: Vec<Arc<Shoal<S>>>,
    /// Every table, in dataset order
    tables: Vec<Arc<dyn TableSource<S::QueryKinds>>>,
    /// How reads are served
    options: SendOptions,
    /// How many streams drive the cluster
    workers: usize,
    /// The kinds of operation it was handed beside read and insert
    kinds: Vec<Supplied<S::QueryKinds>>,
}

impl<S> Driver<S>
where
    S: QuerySupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + Clone + 'static,
    <S::QueryKinds as Archive>::Archived: Send + Sync,
    S::ResponseKinds: Send + Sync,
    <S::ResponseKinds as Archive>::Archived: Send
        + Sync
        + rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    /// A driver over these members' clients and these tables
    ///
    /// # Arguments
    ///
    /// * `clients` - One client a member
    /// * `tables` - Every table, in dataset order
    /// * `options` - How reads are served
    /// * `workers` - How many streams drive the cluster
    ///
    /// # Panics
    ///
    /// When there is no client or no worker.
    #[must_use]
    pub fn new(
        clients: Vec<Arc<Shoal<S>>>,
        tables: Vec<Arc<dyn TableSource<S::QueryKinds>>>,
        options: SendOptions,
        workers: usize,
    ) -> Self {
        // a driver with nothing to drive through is a bug at the call site
        assert!(!clients.is_empty(), "a driver needs a client");
        assert!(workers > 0, "a driver needs a worker");
        Driver {
            clients,
            tables,
            options,
            workers,
            kinds: Vec::new(),
        }
    }

    /// Hand the driver kinds of operation beside read and insert
    ///
    /// A workload names them by their names, and a picker built with [`Driver::kind_names`]
    /// chooses them by their places in this list
    /// ([F69](../../docs/src/features/driver-operation-kinds.md)).
    ///
    /// # Arguments
    ///
    /// * `kinds` - The kinds, in the order their places are counted
    #[must_use]
    pub fn with_kinds(mut self, kinds: Vec<Arc<dyn OperationKind<S>>>) -> Self {
        // each kind as a worker uses it: a name, a builder and what its answer has to show
        self.kinds = kinds
            .into_iter()
            .map(|kind| Supplied {
                name: Arc::from(kind.name()),
                expect: kind.expect(),
                build: Arc::new(move |seed| kind.build(seed)),
            })
            .collect();
        self
    }

    /// The names of the kinds the driver was handed, in its order, for a picker
    #[must_use]
    pub fn kind_names(&self) -> Vec<&str> {
        self.kinds.iter().map(|kind| &*kind.name).collect()
    }

    /// The tables this driver reads and inserts
    #[must_use]
    pub fn tables(&self) -> &[Arc<dyn TableSource<S::QueryKinds>>] {
        &self.tables
    }

    /// A driver through the same members over some of these tables
    ///
    /// The bench's paced stream and its main load are two of these, one table and the rest
    /// ([F72](../../docs/src/features/bench-paced-stream.md)), so that no table's insert feed
    /// is opened by both.
    ///
    /// # Arguments
    ///
    /// * `keep` - Whether a table, by what its scan found, is one of them
    /// * `workers` - How many streams the new driver drives
    ///
    /// # Panics
    ///
    /// When there is no worker.
    #[must_use]
    pub fn narrowed(&self, keep: impl Fn(&crate::feed::TableScan) -> bool, workers: usize) -> Self {
        // the same clients, read options and kinds over the tables kept
        assert!(workers > 0, "a driver needs a worker");
        Driver {
            clients: self.clients.clone(),
            tables: self.tables.iter().filter(|table| keep(table.scan())).cloned().collect(),
            options: self.options.clone(),
            workers,
            kinds: self.kinds.clone(),
        }
    }

    /// Run one arm until its time is up, it is stopped, or its inserts run out
    ///
    /// The clock is the caller's, so whatever else happens during the arm - an event, a mark -
    /// is on the same clock as its answers. A second sample is sent each second.
    ///
    /// # Arguments
    ///
    /// * `settings` - How the arm is driven
    /// * `clock` - The arm's clock, started by the caller
    /// * `progress` - Where each second is sent
    pub async fn run_arm(
        &self,
        settings: &ArmSettings,
        clock: Arc<ArmClock>,
        progress: &Progress,
    ) -> ArmOutcome {
        // an arm that inserts streams each table's insert pool from its start
        if settings.inserts {
            let wrap = settings.on_exhaust == OnExhaust::Wrap;
            for table in &self.tables {
                if table.scan().insert_rows > 0 {
                    table.open_feed(FeedRange::Inserts, wrap);
                }
            }
        }
        let shared = Arc::new(Shared {
            clock: clock.clone(),
            tables: self.tables.clone(),
            seconds: (0..self.workers).map(|_| Mutex::new(Vec::new())).collect(),
            on_exhaust: settings.on_exhaust,
            retries: settings.retries,
            bundle: settings.bundle,
            in_flight: settings.in_flight,
            work: Work::Arm(Arc::new(settings.picker.clone())),
            kinds: self.kinds.clone(),
            pace: settings.pace,
        });
        let warmup_secs = settings.warmup.as_secs() as usize;
        let measured_end = warmup_secs + settings.duration.as_secs() as usize;
        let (seconds, cpu) = self
            .drive(shared, progress, Some((warmup_secs, measured_end)))
            .await;
        // stop every reader and keep what each feed did
        let feeds = self
            .tables
            .iter()
            .map(|table| {
                table.close_feed();
                (table.scan().table.clone(), table.feed_facts())
            })
            .collect();
        ArmOutcome {
            seconds,
            cpu,
            ended_early: clock.ended_early(),
            deadline: clock.deadline(),
            feeds,
        }
    }

    /// Insert every table's preload, at one bundle size, until each is spent
    ///
    /// # Arguments
    ///
    /// * `bundle` - How many inserts a bundle holds
    /// * `in_flight` - How many a worker keeps outstanding
    /// * `progress` - Where each second is sent
    ///
    /// Returns every second's windows and how long the preload took.
    pub async fn preload(
        &self,
        bundle: usize,
        in_flight: usize,
        progress: &Progress,
    ) -> (Vec<Window>, Duration) {
        // every table with a preload streams it, once
        for table in &self.tables {
            if table.scan().preload_rows > 0 {
                table.open_feed(FeedRange::Preload, false);
            }
        }
        // no deadline: the preload ends when every feed is spent
        let clock = ArmClock::start(Duration::from_secs(u64::MAX / 4_000));
        let shared = Arc::new(Shared {
            clock: clock.clone(),
            tables: self.tables.clone(),
            seconds: (0..self.workers).map(|_| Mutex::new(Vec::new())).collect(),
            on_exhaust: OnExhaust::End,
            retries: LOAD_RETRIES,
            bundle,
            in_flight,
            work: Work::Load,
            kinds: self.kinds.clone(),
            pace: None,
        });
        let (seconds, _) = self.drive(shared, progress, None).await;
        for table in &self.tables {
            table.close_feed();
        }
        (seconds, clock.started().elapsed())
    }

    /// Read back every insert the last arm was acknowledged for
    ///
    /// # Arguments
    ///
    /// * `bundle` - How many reads a bundle holds
    /// * `in_flight` - How many a worker keeps outstanding
    /// * `progress` - Where each second is sent
    pub async fn verify(&self, bundle: usize, in_flight: usize, progress: &Progress) -> VerifyFacts {
        // one read a distinct acknowledged row, across every table
        let queries: VecDeque<S::QueryKinds> = self
            .tables
            .iter()
            .flat_map(|table| table.verify_queries())
            .collect();
        let checked = queries.len() as u64;
        let window = self.read_all(queries, bundle, in_flight, progress).await;
        VerifyFacts {
            checked,
            lost: window.read.misses,
            errors: window.read.errors,
        }
    }

    /// Read a fixed list of queries as reads, and add up how they ended
    ///
    /// # Arguments
    ///
    /// * `queries` - The reads
    /// * `bundle` - How many a bundle holds
    /// * `in_flight` - How many a worker keeps outstanding
    /// * `progress` - Where each second is sent
    pub async fn read_all(
        &self,
        queries: VecDeque<S::QueryKinds>,
        bundle: usize,
        in_flight: usize,
        progress: &Progress,
    ) -> Window {
        // nothing to read is nothing found missing
        if queries.is_empty() {
            return Window::default();
        }
        let clock = ArmClock::start(Duration::from_secs(u64::MAX / 4_000));
        let shared = Arc::new(Shared {
            clock,
            tables: self.tables.clone(),
            seconds: (0..self.workers).map(|_| Mutex::new(Vec::new())).collect(),
            on_exhaust: OnExhaust::End,
            retries: LOAD_RETRIES,
            bundle,
            in_flight,
            work: Work::Queries(Arc::new(Mutex::new(queries))),
            kinds: self.kinds.clone(),
            pace: None,
        });
        let (seconds, _) = self.drive(shared, progress, None).await;
        Window::sum(&seconds)
    }

    /// Run every worker and the reporter until the workers are done
    ///
    /// # Arguments
    ///
    /// * `shared` - What the workers share
    /// * `progress` - Where each second is sent
    /// * `phases` - The first measured second and the first after it, for an arm
    ///
    /// Returns every second's windows and the driver's cpu each second.
    async fn drive(
        &self,
        shared: Arc<Shared<S::QueryKinds>>,
        progress: &Progress,
        phases: Option<(usize, usize)>,
    ) -> (Vec<Window>, Vec<f64>) {
        // start every worker against its member
        let mut handles = Vec::with_capacity(self.workers);
        for worker in 0..self.workers {
            let client = self.clients[worker % self.clients.len()].clone();
            let shared = shared.clone();
            let options = self.options.clone();
            handles.push(tokio::spawn(async move {
                worker_loop(client, worker, shared, options).await;
            }));
        }
        // report each second as it closes, until every worker is done
        let done = Arc::new(AtomicBool::new(false));
        let reporter = {
            let shared = shared.clone();
            let progress = progress.clone();
            let done = done.clone();
            tokio::spawn(async move {
                let mut cpu = Vec::new();
                let mut ticks = cpu_ticks();
                let mut reported = 0usize;
                while !done.load(Ordering::Relaxed) {
                    // wake just after the next second boundary of the arm's clock
                    let next = shared.clock.started() + Duration::from_secs(reported as u64 + 1);
                    tokio::time::sleep_until(tokio::time::Instant::from_std(next) + Duration::from_millis(5)).await;
                    let now = cpu_ticks();
                    let busy = match (ticks, now) {
                        (Some(before), Some(after)) => (after.saturating_sub(before)) as f64 / TICKS_PER_SEC * 100.0,
                        _ => 0.0,
                    };
                    ticks = now;
                    cpu.push(busy);
                    // a preload is shown as how far it got
                    if matches!(shared.work, Work::Load) {
                        let done: u64 = shared.tables.iter().map(|table| table.feed_facts().taken).sum();
                        let total: u64 = shared.tables.iter().map(|table| table.scan().preload_rows).sum();
                        progress.send(BenchEvent::Phase(crate::progress::Phase::Preload { done, total }));
                    }
                    // an arm's seconds are shown as they close
                    if let Some((from, to)) = phases {
                        let phase = if reported < from {
                            SecondPhase::Warmup
                        } else if reported < to {
                            SecondPhase::Measure
                        } else {
                            SecondPhase::Drain
                        };
                        let summary = shared.second(reported).summary(Duration::from_secs(1));
                        progress.send(BenchEvent::Second(SecondSample {
                            at: reported as u64,
                            phase,
                            summary,
                            driver_cpu_pct: busy,
                        }));
                    }
                    reported += 1;
                }
                cpu
            })
        };
        // a worker that panicked has recorded what it could; the rest carry on
        for handle in handles {
            let _ = handle.await;
        }
        // the reporter finishes the second it is in and hands back its cpu figures
        done.store(true, Ordering::Relaxed);
        let cpu = reporter.await.unwrap_or_default();
        // every worker's seconds, added up
        let len = shared.len();
        let seconds = (0..len).map(|second| shared.second(second)).collect();
        (seconds, cpu)
    }
}

/// Drive streams for one worker until it has nothing more to send
///
/// # Arguments
///
/// * `client` - The member this worker goes through
/// * `worker` - Which worker
/// * `shared` - What the workers share
/// * `options` - How reads are served
async fn worker_loop<S>(
    client: Arc<Shoal<S>>,
    worker: usize,
    shared: Arc<Shared<S::QueryKinds>>,
    options: SendOptions,
) where
    S: QuerySupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + Clone + 'static,
    <S::QueryKinds as Archive>::Archived: Send + Sync,
    S::ResponseKinds: Send + Sync,
    <S::ResponseKinds as Archive>::Archived: Send
        + Sync
        + rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // the cursor survives a stream that failed, so choices pick up where they left off
    let mut cursor = Cursor {
        worker,
        index: 0,
        bundles: 0,
        spent: false,
    };
    // the queries waiting to be sent again survive a stream that failed, too
    let mut retries: VecDeque<Retry<S::QueryKinds>> = VecDeque::new();
    while (!cursor.spent || !retries.is_empty()) && shared.clock.live() {
        if drive_stream(&client, &shared, &options, &mut cursor, &mut retries).await.is_err() {
            // everything outstanding on it was recorded; pause so a dead member is not hammered
            tokio::time::sleep(REOPEN_AFTER).await;
        }
    }
}

/// Choose the next query for a worker and stage it in the bundle being filled
///
/// # Arguments
///
/// * `shared` - What the workers share
/// * `cursor` - The worker's place
/// * `buffer` - The bundle being filled
///
/// Returns what was staged, or why nothing was: `Err(true)` for a feed with nothing parsed yet,
/// `Err(false)` for a worker with nothing more to send.
fn stage<K: Clone>(
    shared: &Shared<K>,
    cursor: &mut Cursor,
    buffer: &mut Vec<K>,
    retries: &mut VecDeque<Retry<K>>,
) -> Result<Staged<K>, bool> {
    // a query due to be sent again goes first
    if retries.front().is_some_and(|retry| retry.at <= Instant::now()) {
        let retry = retries.pop_front().expect("there is a front");
        let copy = Some(retry.query.clone());
        buffer.push(retry.query);
        return Ok(Staged {
            kind: retry.kind,
            ack: retry.ack,
            copy,
            attempts: retry.attempts,
            due: None,
        });
    }
    // a worker with nothing new to send only waits on its retries
    if cursor.spent {
        return Err(!retries.is_empty());
    }
    // a copy is kept only when it may be sent again
    let keep = |query: &K| (shared.retries > 0).then(|| query.clone());
    // a paced operation is dated from its slot, which its index fixes before it moves
    let due = shared.slot(cursor.worker, cursor.index);
    let staged = |kind, ack, copy| Staged {
        kind,
        ack,
        copy,
        attempts: 0,
        due,
    };
    match &shared.work {
        Work::Arm(picker) => {
            // the choice at this worker's index; the index moves only once something is staged
            match picker.at(cursor.worker, cursor.index) {
                Pick::Read { table, keys } => {
                    let query = shared.tables[table].read_query(&keys);
                    let copy = keep(&query);
                    buffer.push(query);
                    cursor.index += 1;
                    Ok(staged(OpKind::Read, None, copy))
                }
                Pick::Insert { table } => match shared.tables[table].take_insert() {
                    Take::Row { query, seq } => {
                        let copy = keep(&query);
                        buffer.push(query);
                        cursor.index += 1;
                        Ok(staged(OpKind::Insert, Some((table, seq)), copy))
                    }
                    Take::Stalled => Err(true),
                    Take::Exhausted => {
                        // a spent pool ends the arm, unless it was told to start over
                        if shared.on_exhaust == OnExhaust::End {
                            shared.clock.stop("inserts exhausted");
                        }
                        Err(false)
                    }
                },
                // a kind the driver was handed builds its own query from the operation's seed
                Pick::Supplied { kind, seed } => {
                    let supplied = &shared.kinds[kind];
                    let query = (supplied.build)(seed);
                    let copy = keep(&query);
                    buffer.push(query);
                    cursor.index += 1;
                    Ok(staged(OpKind::Supplied(supplied.name.clone()), None, copy))
                }
            }
        }
        Work::Load => {
            // every table's feed in turn, starting at a different one a worker
            let tables = shared.tables.len();
            let mut stalled = false;
            for offset in 0..tables {
                let table = (cursor.worker + cursor.index as usize + offset) % tables;
                match shared.tables[table].take_insert() {
                    Take::Row { query, seq } => {
                        let copy = keep(&query);
                        buffer.push(query);
                        cursor.index += 1;
                        return Ok(staged(OpKind::Insert, Some((table, seq)), copy));
                    }
                    Take::Stalled => stalled = true,
                    Take::Exhausted => (),
                }
            }
            // nothing anywhere: wait if a feed is still reading, otherwise this worker is done
            Err(stalled)
        }
        Work::Queries(queue) => match lock(queue).pop_front() {
            Some(query) => {
                let copy = keep(&query);
                buffer.push(query);
                Ok(staged(OpKind::Read, None, copy))
            }
            None => Err(false),
        },
    }
}

/// Send the staged bundle, remembering what each query in it was
///
/// # Arguments
///
/// * `shared` - What the workers share, where the bundle's bytes are recorded
/// * `queries_tx` - The stream to send on
/// * `buffer` - The staged queries
/// * `staged` - What each one was
/// * `outstanding` - Where to remember them by index
/// * `bundles` - Where to remember the bundle
/// * `cursor` - The worker's place, which numbers its bundles
async fn flush<S: QuerySupport>(
    shared: &Shared<S::QueryKinds>,
    queries_tx: &mut ShoalQueryStream<S>,
    buffer: &mut Vec<S::QueryKinds>,
    staged: &mut Vec<Staged<S::QueryKinds>>,
    outstanding: &mut HashMap<usize, Sent<S::QueryKinds>>,
    bundles: &mut HashMap<u64, Bundle>,
    cursor: &mut Cursor,
) -> Result<(), Errors> {
    // nothing staged is nothing to send
    if buffer.is_empty() {
        return Ok(());
    }
    let mut queries: Queries<S> = queries_tx.query_with_capacity(buffer.len());
    for query in buffer.drain(..) {
        queries.add_mut(query);
    }
    let base = queries_tx.base_index;
    let bundle = cursor.bundles;
    cursor.bundles += 1;
    // latency counts from the send, so the send's own time is in it, or for a paced arm from
    // when its operation was due, so time spent waiting behind a stall is in it too
    let now = Instant::now();
    let at = staged.iter().filter_map(|item| item.due).min().map_or(now, |due| due.min(now));
    let before = queries_tx.bytes_sent;
    queries_tx.send(queries).await?;
    // what the bundle took on the wire, which the stream counted as it wrote it
    shared.record_sent(cursor.worker, queries_tx.bytes_sent - before);
    // every query is now owed an answer at its index in the stream
    bundles.insert(
        bundle,
        Bundle {
            at,
            remaining: staged.len(),
        },
    );
    for (offset, item) in staged.drain(..).enumerate() {
        outstanding.insert(
            base + offset,
            Sent {
                kind: item.kind,
                bundle,
                ack: item.ack,
                copy: item.copy,
                attempts: item.attempts + 1,
            },
        );
    }
    Ok(())
}

/// The code a failed stream is recorded under
///
/// # Arguments
///
/// * `error` - What the stream failed with
fn stream_code(error: &Errors) -> String {
    // a server failure by its code, anything else by what kind of failure it was
    match error {
        Errors::Server { code, .. } => format!("stream:{code:?}"),
        other => {
            let debug = format!("{other:?}");
            let kind = debug.split(['(', ' ', '{']).next().unwrap_or("unknown").to_string();
            format!("stream:{kind}")
        }
    }
}

/// Drive one stream until the worker has nothing more to send, or the stream fails
///
/// # Arguments
///
/// * `client` - The member this worker goes through
/// * `shared` - What the workers share
/// * `options` - How reads are served
/// * `cursor` - The worker's place
async fn drive_stream<S>(
    client: &Arc<Shoal<S>>,
    shared: &Shared<S::QueryKinds>,
    options: &SendOptions,
    cursor: &mut Cursor,
    retries: &mut VecDeque<Retry<S::QueryKinds>>,
) -> Result<(), Errors>
where
    S: QuerySupport + Send + Sync + 'static,
    S::QueryKinds: Clone,
    <S::ResponseKinds as Archive>::Archived:
        rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // an unordered stream, since answers are recorded as they land
    let (mut queries_tx, mut results_rx) = client.stream_unordered_with(options.clone())?;
    let mut buffer: Vec<S::QueryKinds> = Vec::with_capacity(shared.bundle);
    let mut staged = Vec::with_capacity(shared.bundle);
    let mut outstanding: HashMap<usize, Sent<S::QueryKinds>> = HashMap::with_capacity(shared.in_flight);
    let mut bundles: HashMap<u64, Bundle> = HashMap::new();
    // when the stream last heard an answer, or last owed none, which a hung stream is judged from
    let mut heard = Instant::now();
    let outcome: Result<(), Errors> = async {
        loop {
            // top the pipeline up while there is time and work left
            while (!cursor.spent || retries.front().is_some_and(|retry| retry.at <= Instant::now()))
                && shared.clock.live()
                && outstanding.len() + staged.len() < shared.in_flight
            {
                // a paced worker's next operation waits for its slot, though a retry due goes now
                let retry_due = retries.front().is_some_and(|retry| retry.at <= Instant::now());
                let early = shared
                    .slot(cursor.worker, cursor.index)
                    .is_some_and(|due| due > Instant::now());
                if early && !retry_due && !cursor.spent {
                    break;
                }
                match stage(shared, cursor, &mut buffer, retries) {
                    Ok(sent) => staged.push(sent),
                    Err(true) => {
                        // a feed still parsing: send what is staged, then wait for it
                        flush(shared, &mut queries_tx, &mut buffer, &mut staged, &mut outstanding, &mut bundles, cursor).await?;
                        let waited = Instant::now();
                        tokio::time::sleep(FEED_POLL).await;
                        shared.record_wait(cursor.worker, waited.elapsed());
                        // with answers owed, go and collect them rather than spin
                        if !outstanding.is_empty() {
                            break;
                        }
                    }
                    Err(false) => cursor.spent = true,
                }
                // a spent worker with retries still waiting stops topping up until one is due
                if cursor.spent && !retries.front().is_some_and(|retry| retry.at <= Instant::now()) {
                    break;
                }
                if staged.len() >= shared.bundle {
                    flush(shared, &mut queries_tx, &mut buffer, &mut staged, &mut outstanding, &mut bundles, cursor).await?;
                }
            }
            // a partial bundle goes out once nothing more will be added to it for now
            flush(shared, &mut queries_tx, &mut buffer, &mut staged, &mut outstanding, &mut bundles, cursor).await?;
            // the next paced operation's slot, which every wait below ends at, unless nothing
            // more will be sent or the stream is at its cap and has to hear an answer first
            let next_slot = if cursor.spent || !shared.clock.live() || outstanding.len() >= shared.in_flight {
                None
            } else {
                shared.slot(cursor.worker, cursor.index)
            };
            // stop once there is nothing to send and every answer is in
            if outstanding.is_empty() {
                // a stream that owes nothing is not silent
                heard = Instant::now();
                if !shared.clock.live() || cursor.spent && retries.is_empty() {
                    return Ok(());
                }
                // a retry not yet due, or a paced slot, is waited for rather than spun on; a slot
                // far off is looked at again sooner, so an arm that ends meanwhile is noticed
                let retry = retries.front().map(|retry| retry.at);
                let paced = next_slot.map(|due| due.min(Instant::now() + PACE_CHECK));
                if let Some(wake) = [retry, paced].into_iter().flatten().min() {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(wake)).await;
                }
                continue;
            }
            // wait for the next answer, and for a paced worker no longer than its next slot; a
            // stream silent for a minute with answers owed is hung
            let wait = next_slot.map_or(HUNG_CHECK, |due| {
                due.saturating_duration_since(Instant::now()).min(HUNG_CHECK)
            });
            let response = match tokio::time::timeout(wait, results_rx.next()).await {
                Ok(next) => {
                    heard = Instant::now();
                    match next? {
                        Some(response) => response,
                        None => return Ok(()),
                    }
                }
                Err(_) => {
                    if heard.elapsed() >= HUNG_AFTER {
                        return Err(Errors::Server {
                            query_id: None,
                            index: None,
                            code: shoal::shared::protocol::error::ErrorCode::Timeout,
                            msg: format!("{} answers never came", outstanding.len()),
                        });
                    }
                    continue;
                }
            };
            // every answer took bytes on the wire, whatever it answered
            let received = response.wire_bytes();
            // an answer for a query this stream does not owe is counted and otherwise ignored
            let Some(sent) = outstanding.remove(&response.get_index()) else {
                shared.record_received(cursor.worker, None, received);
                continue;
            };
            shared.record_received(cursor.worker, Some(&sent.kind), received);
            // a failure by its code, a read that found nothing as a miss, a success by its time
            let latency = bundles.get(&sent.bundle).map(|bundle| bundle.at.elapsed());
            let outcome = match response.error() {
                // a failure that says to try again is sent again, while attempts remain
                Some(error)
                    if retriable(error.code()) && sent.copy.is_some() && sent.attempts <= shared.retries =>
                {
                    retries.push_back(Retry {
                        query: sent.copy.clone().expect("a copy is kept"),
                        kind: sent.kind.clone(),
                        ack: sent.ack,
                        attempts: sent.attempts,
                        at: Instant::now() + backoff(sent.attempts),
                    });
                    shared.record_retry(cursor.worker, &sent.kind);
                    Outcome::Retried
                }
                Some(error) => Outcome::Failed {
                    code: format!("{:?}", error.code()),
                    message: error.msg().to_string(),
                },
                None => {
                    // a read has to find its row, and a supplied kind says what it has to show
                    let opts = match &sent.kind {
                        OpKind::Supplied(name) => shared
                            .kinds
                            .iter()
                            .find(|kind| kind.name == *name)
                            .map_or_else(QuerySuceededOpts::default, |kind| kind.expect),
                        kind => QuerySuceededOpts {
                            get: *kind == OpKind::Read,
                            ..QuerySuceededOpts::default()
                        },
                    };
                    match response.suceeded(opts) {
                        Ok(()) => Outcome::Ok(latency.unwrap_or_default()),
                        Err(_) => Outcome::Miss,
                    }
                }
            };
            // an acknowledged insert is one the cluster promised to keep
            if let (Outcome::Ok(_), Some((table, seq))) = (&outcome, sent.ack) {
                shared.tables[table].ack(seq);
            }
            if outcome != Outcome::Retried {
                shared.record(cursor.worker, &sent.kind, outcome);
            }
            // the bundle is done when its last answer is in
            if let Some(bundle) = bundles.get_mut(&sent.bundle) {
                bundle.remaining -= 1;
                if bundle.remaining == 0 {
                    let done = bundles.remove(&sent.bundle).expect("the bundle is here");
                    shared.record_bundle(cursor.worker, done.at.elapsed());
                }
            }
        }
    }
    .await;
    // whatever was still owed when the stream failed will never be answered on it
    if let Err(error) = &outcome {
        let code = stream_code(error);
        let message = error.to_string();
        // what the stream owed is sent again on the next one while attempts remain, and is
        // recorded as failed otherwise
        let now = Instant::now();
        for (_, sent) in outstanding.drain() {
            match sent.copy {
                Some(query) if sent.attempts <= shared.retries => {
                    shared.record_retry(cursor.worker, &sent.kind);
                    retries.push_back(Retry {
                        query,
                        kind: sent.kind,
                        ack: sent.ack,
                        attempts: sent.attempts,
                        at: now + backoff(sent.attempts),
                    });
                }
                _ => shared.record(
                    cursor.worker,
                    &sent.kind,
                    Outcome::Failed {
                        code: code.clone(),
                        message: message.clone(),
                    },
                ),
            }
        }
        // what was staged and never sent is sent on the next stream if it can be; a read with
        // no copy is simply not made, and an insert with none is recorded as failed so it is
        // not lost
        for (item, query) in staged.drain(..).zip(buffer.drain(..)) {
            if shared.retries > 0 {
                retries.push_back(Retry {
                    query,
                    kind: item.kind,
                    ack: item.ack,
                    attempts: item.attempts,
                    at: now,
                });
            } else if item.ack.is_some() {
                shared.record(
                    cursor.worker,
                    &item.kind,
                    Outcome::Failed {
                        code: code.clone(),
                        message: "never sent".to_string(),
                    },
                );
            }
        }
        return outcome;
    }
    // close the stream and read it to its end, which releases its slot in the client
    queries_tx.close().await?;
    let _ = tokio::time::timeout(HUNG_AFTER, async {
        // what is read here was still sent, so its bytes count too
        while let Some(response) = results_rx.next().await? {
            shared.record_received(cursor.worker, None, response.wire_bytes());
        }
        Ok::<(), Errors>(())
    })
    .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{backoff, retriable, slot, RETRY_CAP, RETRY_FIRST};
    use shoal::shared::protocol::error::ErrorCode;
    use std::time::{Duration, Instant};

    /// A retry waits longer each time, up to a cap
    #[test]
    fn a_retry_backs_off_to_a_cap() {
        assert_eq!(backoff(1), RETRY_FIRST);
        assert_eq!(backoff(2), RETRY_FIRST * 2);
        assert_eq!(backoff(3), RETRY_FIRST * 4);
        assert_eq!(backoff(40), RETRY_CAP);
        assert!(RETRY_CAP <= Duration::from_secs(1));
    }

    /// Only a failure that says to try again is retried
    #[test]
    fn only_a_failure_that_says_to_try_again_is_retried() {
        assert!(retriable(ErrorCode::OutcomeUnknown));
        assert!(retriable(ErrorCode::NotLeader));
        assert!(retriable(ErrorCode::StaleTopology));
        assert!(!retriable(ErrorCode::CorruptArchive));
        assert!(!retriable(ErrorCode::WrongCluster));
    }

    /// A paced arm's slots are fixed from its start, at the rate, with its workers interleaved
    #[test]
    fn paced_slots_interleave_the_workers_at_the_rate() {
        let started = Instant::now();
        // within a microsecond, since a slot is computed in floating point
        let at = |worker, workers, per_sec, index, millis: u64| {
            let due = slot(started, worker, workers, per_sec, index);
            let expected = started + Duration::from_millis(millis);
            let gap = if due > expected { due - expected } else { expected - due };
            assert!(gap < Duration::from_micros(1), "{worker}/{workers} #{index}: {gap:?} off");
        };
        // one worker at ten a second is due every tenth of a second, from the start
        at(0, 1, 10.0, 0, 0);
        at(0, 1, 10.0, 1, 100);
        at(0, 1, 10.0, 25, 2_500);
        // two workers share the rate: each is due every fifth, the second a tenth behind
        at(0, 2, 10.0, 0, 0);
        at(1, 2, 10.0, 0, 100);
        at(0, 2, 10.0, 1, 200);
        at(1, 2, 10.0, 1, 300);
        // a rate below one a second spaces them by more than a second
        at(0, 1, 0.5, 3, 6_000);
    }
}
