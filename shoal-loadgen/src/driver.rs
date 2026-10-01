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

use rkyv::Archive;
use shoal::client::{SendOptions, ShoalQueryStream};
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
    /// How many queries a bundle holds
    bundle: usize,
    /// How many queries a worker keeps outstanding
    in_flight: usize,
    /// What the workers send
    work: Work<K>,
}

impl<K> Shared<K> {
    /// Record how an operation ended, in this second of this worker's windows
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `kind` - What kind of operation
    /// * `outcome` - How it ended
    fn record(&self, worker: usize, kind: OpKind, outcome: Outcome) {
        // the window for the second it is now, made if this is its first answer
        let second = self.clock.second();
        let mut windows = lock(&self.seconds[worker]);
        if windows.len() <= second {
            windows.resize_with(second + 1, Window::default);
        }
        windows[second].record(kind, outcome);
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
struct Sent {
    /// What kind of operation it was
    kind: OpKind,
    /// The bundle it went in
    bundle: u64,
    /// The insert to acknowledge if it succeeds: its table and sequence
    ack: Option<(usize, u64)>,
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
    /// What its workers choose
    pub picker: Picker,
    /// Whether it inserts at all, so its feeds are opened
    pub inserts: bool,
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
}

impl<S> Driver<S>
where
    S: QuerySupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + 'static,
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
        }
    }

    /// The tables this driver reads and inserts
    #[must_use]
    pub fn tables(&self) -> &[Arc<dyn TableSource<S::QueryKinds>>] {
        &self.tables
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
            bundle: settings.bundle,
            in_flight: settings.in_flight,
            work: Work::Arm(Arc::new(settings.picker.clone())),
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
            bundle,
            in_flight,
            work: Work::Load,
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
            bundle,
            in_flight,
            work: Work::Queries(Arc::new(Mutex::new(queries))),
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
    S::QueryKinds: Send + Sync + 'static,
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
    while !cursor.spent && shared.clock.live() {
        if drive_stream(&client, &shared, &options, &mut cursor).await.is_err() {
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
fn stage<K>(
    shared: &Shared<K>,
    cursor: &mut Cursor,
    buffer: &mut Vec<K>,
) -> Result<(OpKind, Option<(usize, u64)>), bool> {
    match &shared.work {
        Work::Arm(picker) => {
            // the choice at this worker's index; the index moves only once something is staged
            match picker.at(cursor.worker, cursor.index) {
                Pick::Read { table, keys } => {
                    buffer.push(shared.tables[table].read_query(&keys));
                    cursor.index += 1;
                    Ok((OpKind::Read, None))
                }
                Pick::Insert { table } => match shared.tables[table].take_insert() {
                    Take::Row { query, seq } => {
                        buffer.push(query);
                        cursor.index += 1;
                        Ok((OpKind::Insert, Some((table, seq))))
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
                        buffer.push(query);
                        cursor.index += 1;
                        return Ok((OpKind::Insert, Some((table, seq))));
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
                buffer.push(query);
                Ok((OpKind::Read, None))
            }
            None => Err(false),
        },
    }
}

/// Send the staged bundle, remembering what each query in it was
///
/// # Arguments
///
/// * `queries_tx` - The stream to send on
/// * `buffer` - The staged queries
/// * `staged` - What each one was
/// * `outstanding` - Where to remember them by index
/// * `bundles` - Where to remember the bundle
/// * `cursor` - The worker's place, which numbers its bundles
async fn flush<S: QuerySupport>(
    queries_tx: &mut ShoalQueryStream<S>,
    buffer: &mut Vec<S::QueryKinds>,
    staged: &mut Vec<(OpKind, Option<(usize, u64)>)>,
    outstanding: &mut HashMap<usize, Sent>,
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
    // latency counts from the send, so the send's own time is in it
    let at = Instant::now();
    queries_tx.send(queries).await?;
    // every query is now owed an answer at its index in the stream
    bundles.insert(
        bundle,
        Bundle {
            at,
            remaining: staged.len(),
        },
    );
    for (offset, (kind, ack)) in staged.drain(..).enumerate() {
        outstanding.insert(base + offset, Sent { kind, bundle, ack });
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
) -> Result<(), Errors>
where
    S: QuerySupport + Send + Sync + 'static,
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
    let mut outstanding: HashMap<usize, Sent> = HashMap::with_capacity(shared.in_flight);
    let mut bundles: HashMap<u64, Bundle> = HashMap::new();
    // how long the stream has gone without an answer while owed one
    let mut silent = Duration::ZERO;
    let outcome: Result<(), Errors> = async {
        loop {
            // top the pipeline up while there is time and work left
            while !cursor.spent
                && shared.clock.live()
                && outstanding.len() + staged.len() < shared.in_flight
            {
                match stage(shared, cursor, &mut buffer) {
                    Ok(sent) => staged.push(sent),
                    Err(true) => {
                        // a feed still parsing: send what is staged, then wait for it
                        flush(&mut queries_tx, &mut buffer, &mut staged, &mut outstanding, &mut bundles, cursor).await?;
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
                if staged.len() >= shared.bundle {
                    flush(&mut queries_tx, &mut buffer, &mut staged, &mut outstanding, &mut bundles, cursor).await?;
                }
            }
            // a partial bundle goes out once nothing more will be added to it for now
            flush(&mut queries_tx, &mut buffer, &mut staged, &mut outstanding, &mut bundles, cursor).await?;
            // stop once there is nothing to send and every answer is in
            if outstanding.is_empty() {
                if cursor.spent || !shared.clock.live() {
                    return Ok(());
                }
                continue;
            }
            // wait for the next answer; a stream silent for a minute with answers owed is hung
            let response = match tokio::time::timeout(HUNG_CHECK, results_rx.next()).await {
                Ok(next) => {
                    silent = Duration::ZERO;
                    match next? {
                        Some(response) => response,
                        None => return Ok(()),
                    }
                }
                Err(_) => {
                    silent += HUNG_CHECK;
                    if silent >= HUNG_AFTER {
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
            // an answer for a query this stream does not owe is ignored
            let Some(sent) = outstanding.remove(&response.get_index()) else {
                continue;
            };
            // a failure by its code, a read that found nothing as a miss, a success by its time
            let latency = bundles.get(&sent.bundle).map(|bundle| bundle.at.elapsed());
            let outcome = match response.error() {
                Some(error) => Outcome::Failed {
                    code: format!("{:?}", error.code()),
                    message: error.msg().to_string(),
                },
                None => {
                    let opts = QuerySuceededOpts {
                        get: sent.kind == OpKind::Read,
                        ..QuerySuceededOpts::default()
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
            shared.record(cursor.worker, sent.kind, outcome);
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
        for (_, sent) in outstanding.drain() {
            shared.record(
                cursor.worker,
                sent.kind,
                Outcome::Failed {
                    code: code.clone(),
                    message: message.clone(),
                },
            );
        }
        // what was staged and never sent goes back to nobody: a read is simply not made, and
        // an insert taken from a feed and never sent is recorded as failed so it is not lost
        for (kind, ack) in staged.drain(..) {
            if ack.is_some() {
                shared.record(
                    cursor.worker,
                    kind,
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
        while results_rx.next().await?.is_some() {}
        Ok::<(), Errors>(())
    })
    .await;
    Ok(())
}
