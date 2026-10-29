//! A table scanned whole, through the walk every node already answers
//! ([F115](../../docs/src/features/bench-scans.md))
//!
//! Client scans are M24's ([W4](../../docs/src/future-work/scans/cursor-and-pages.md)), so until
//! then a scan is driven through the one remote path a tablet's rows have: the admin walk
//! ([F87](../../docs/src/features/tablet-walk.md)), a page at a time from the copy the member asked
//! holds. A scan is every tablet of a table walked once, each from a holder the route table names,
//! followers first and spread over the members, as a client's scan prefers them
//! ([W6](../../docs/src/future-work/scans/distribution.md)), with as many pages in flight as it has
//! streams. A node alone, whose topology places nothing, holds every tablet.
//!
//! This is not the scan of a dataset's file ([`crate::feed::TableScan`]), which reads a table's
//! rows off the disk the driver runs on; this one reads a table's partitions off the cluster.
//!
//! What it measures is what the walk reads and holds on a server, never a row's bytes on the
//! wire: an admin walk answers each partition's key and live rows, not the rows.
//!
//! # Invariants
//!
//! - **A tablet's walk never asks below its floor**: every page asks `at_least` the highest
//!   boundary any earlier page of its tablet was read at, whichever copy answers it, which is the
//!   walk's own rule and P21's ([W13](../../docs/src/future-work/scans/contract.md#the-contract)).
//! - **A refusal for now is asked again and counted, by what it was**: a shed page soon, a copy
//!   that cannot answer yet after a while and of another holder; any other refusal fails the
//!   scan by its code, never silently.
//! - **Nothing here names a table's rows**: a scan names its table by the name the schema spells.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::shared::identity::{NodeId, TableId};
use shoal::shared::placement::TABLET_COUNT;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest, WalkAnswer, WalkClass};
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::routes::RouteTable;
use shoal::shared::traits::QuerySupport;

use crate::driver::{ArmClock, ArmOutcome};
use crate::progress::{BenchEvent, Progress};
use crate::results::{SecondPhase, SecondSample};
use crate::seed::Seeded;
use crate::window::{OpKind, Outcome, PageFigures, Window};

/// The name a scan's pages are recorded under among a window's kinds
pub const SCAN_KIND: &str = "scan";

/// How many pages a scan keeps in flight when its spec names none and the cluster says nothing
/// of its shards
pub const DEFAULT_STREAMS: usize = 8;

/// How many pages a shard reads at once for a scan, which a scan's default streams keep busy
pub const PAGES_A_SHARD: usize = 2;

/// How long to wait before asking again a shard that shed a page past those it reads at once
///
/// A shard sheds a page past the few it reads at once, and one of those is done in about a
/// millisecond, so a shed page is asked again soon, as the lab's loader asks one.
const SHED_AFTER: Duration = Duration::from_millis(2);

/// The most a shed page's wait is lengthened by, drawn a page, so streams do not ask in step
const SHED_JITTER_US: u64 = 2_000;

/// How long to wait before asking again a copy that is starting, installing, behind or not reached
const RETRY_AFTER: Duration = Duration::from_millis(200);

/// How long one page may go unanswered, asked again and again, before the scan gives it up
///
/// A time rather than a count: a shard sheds a page in microseconds, so a count that a copy's
/// restart outlasts would be spent by sheds in a second.
const GIVE_UP: Duration = Duration::from_secs(60);

/// How long a scan waits for a member's first topology before it takes it for a node alone
const TOPOLOGY_WAIT: Duration = Duration::from_secs(5);

/// A scan arm: one table scanned whole, again and again, with this many pages in flight
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanArm {
    /// The table, by the name the schema spells it
    pub table: String,
    /// How many pages are in flight, or none for two a shard of every member
    pub streams: Option<usize>,
}

/// One member a scan asks its pages of
pub struct ScanMember<S: QuerySupport> {
    /// Its name, as the inventory names it, which its figures are kept under
    pub name: String,
    /// Its node, as the topology names it, or none for a node started alone
    pub node: Option<NodeId>,
    /// A client of it, as the cluster's admin: a walk reads a table whole, so only an admin may ask
    pub client: Arc<Shoal<S>>,
}

impl<S: QuerySupport> Clone for ScanMember<S> {
    /// The same member, its client shared
    fn clone(&self) -> Self {
        ScanMember {
            name: self.name.clone(),
            node: self.node,
            client: self.client.clone(),
        }
    }
}

/// How a whole scan ended
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanEnd {
    /// Whether every tablet was walked to its end
    pub finished: bool,
    /// The pages answered
    pub pages: u64,
    /// The partitions they handed out
    pub partitions: u64,
    /// How long it took
    pub took: Duration,
    /// Why it failed, if a tablet's walk was refused by a code no wait answers
    pub failure: Option<String>,
}

/// Where a scan reads each tablet: the route table, or one member holding every tablet
enum Routes {
    /// A cluster's routes, by the topology a member pushed
    Table(Box<RouteTable>),
    /// A node alone, whose topology places nothing
    Alone,
}

/// Drives whole scans of a table through every member's admin walk
pub struct ScanDriver<S: QuerySupport> {
    /// The members, each with an admin client
    members: Vec<ScanMember<S>>,
}

/// What one tablet's walk came to
enum TabletEnd {
    /// Every page of it was handed out
    Walked,
    /// The scan was stopped before it was
    Stopped,
    /// A page was refused for good, and why
    Failed(String),
}

/// How a page's ask ended
enum Asked {
    /// It was answered
    Page(Box<WalkAnswer>),
    /// A shard shed it, reading as many pages as it reads at once
    Shed,
    /// The copy could not answer yet, or the member could not be reached: the code, and what it said
    Wait(String, String),
    /// The member holds no copy of the tablet any more, as its topology is now, and what it said
    Stale(String),
    /// It was refused by a code no wait answers
    Refused(String),
}

impl<S> ScanDriver<S>
where
    S: QuerySupport + Send + Sync + 'static,
{
    /// A driver over these members
    ///
    /// # Arguments
    ///
    /// * `members` - The members, each with an admin client
    #[must_use]
    pub fn new(members: Vec<ScanMember<S>>) -> Self {
        ScanDriver { members }
    }

    /// The members this driver asks
    #[must_use]
    pub fn members(&self) -> &[ScanMember<S>] {
        &self.members
    }

    /// Why a scan cannot be driven through these members, if it cannot: a cluster member whose build
    /// does not list `walk_class` among its admin reads would take a scan's pages as a driver's, and
    /// could shed a backfill's ([F116](../../docs/src/features/scan-admission.md))
    ///
    /// Only a member known as a cluster's is asked: a node alone answers no `Members` read, and runs
    /// no driver a scan could shed.
    pub async fn refuses(&self) -> Option<String> {
        for member in self.members.iter().filter(|member| member.node.is_some()) {
            let request = AdminRequest {
                op: shoal::uuid::Uuid::new_v4(),
                expected_version: 0,
                kind: AdminKind::Members,
            };
            // what its build answers, as its Members frame lists it
            let reads = match member.client.admin(&request).await {
                Ok(response) => match response.outcome {
                    Ok(AdminOutcome::Read(json)) => json.get("admin_reads").cloned(),
                    Ok(other) => return Some(format!("{} answered Members with {other:?}", member.name)),
                    Err(error) => return Some(format!("{} refused Members: {}", member.name, error.msg)),
                },
                Err(error) => return Some(format!("{} was not reached: {error:?}", member.name)),
            };
            let answers = reads
                .and_then(|reads| reads.as_array().cloned())
                .is_some_and(|reads| reads.iter().any(|read| read.as_str() == Some("walk_class")));
            if !answers {
                return Some(format!(
                    "{} is on a build that answers no walk class, so a scan's pages would be its drivers'",
                    member.name
                ));
            }
        }
        None
    }

    /// How many pages a scan keeps in flight when it is not told: two a shard of every member
    /// the topology lists, or [`DEFAULT_STREAMS`] when it lists none
    #[must_use]
    pub fn default_streams(&self) -> usize {
        // every member's shards, as any member's topology lists them
        let shards: usize = self
            .members
            .iter()
            .find_map(|member| member.client.topology())
            .map(|frame| frame.members.iter().map(|member| usize::from(member.shards)).sum())
            .unwrap_or(0);
        if shards == 0 {
            DEFAULT_STREAMS
        } else {
            shards * PAGES_A_SHARD
        }
    }

    /// The routes a scan reads its tablets by, from the first member that pushed a topology
    async fn routes(&self) -> Routes {
        // a member's first frame may still be on its way just after it connected
        for member in &self.members {
            if member.client.topology().is_none() {
                let _ = tokio::time::timeout(TOPOLOGY_WAIT, member.client.topology_changed(0)).await;
            }
            if let Some(frame) = member.client.topology() {
                return match RouteTable::from_frame(&frame) {
                    Some(table) => Routes::Table(Box::new(table)),
                    None => Routes::Alone,
                };
            }
        }
        Routes::Alone
    }

    /// The members a tablet's page may be asked of, in the order they are tried: its followers
    /// first, the one a tablet starts at spread by the tablet, then its leader
    ///
    /// # Arguments
    ///
    /// * `routes` - The routes
    /// * `table` - The table's identity
    /// * `tablet` - The tablet
    fn candidates(&self, routes: &Routes, table: TableId, tablet: u16) -> Vec<usize> {
        let routes = match routes {
            Routes::Table(routes) => routes,
            // a node alone holds every tablet
            Routes::Alone => return (0..self.members.len()).collect(),
        };
        // the holders this driver has a client of, by their index here
        let leader = routes.leader(table, usize::from(tablet)).map(|addr| addr.node);
        let mine = |member: u16| {
            let node = routes.members.get(usize::from(member))?.node;
            self.members.iter().position(|ours| ours.node == Some(node)).map(|at| (at, node))
        };
        let holders: Vec<(usize, NodeId)> = routes.holders(usize::from(tablet)).filter_map(mine).collect();
        let mut followers: Vec<usize> = holders
            .iter()
            .filter(|(_, node)| Some(*node) != leader)
            .map(|(at, _)| *at)
            .collect();
        let leaders: Vec<usize> = holders
            .iter()
            .filter(|(_, node)| Some(*node) == leader)
            .map(|(at, _)| *at)
            .collect();
        // each tablet starts at another follower, so the tablets spread over them
        if !followers.is_empty() {
            let start = usize::from(tablet) % followers.len();
            followers.rotate_left(start);
        }
        followers.extend(leaders);
        // a tablet no holder of which this driver can reach is asked of every member, so its
        // refusal says why
        if followers.is_empty() {
            return (0..self.members.len()).collect();
        }
        followers
    }

    /// Scan a table whole once: every tablet walked, `streams` pages in flight, every page
    /// recorded in the second it was answered
    ///
    /// Stops early, unfinished, once the clock does.
    ///
    /// # Arguments
    ///
    /// * `table` - The table, by the name the schema spells it
    /// * `streams` - How many pages are in flight
    /// * `seed` - What the tablets' order is drawn from
    /// * `clock` - The arm's clock
    /// * `seconds` - The windows a page is recorded in, one a second of the clock
    pub async fn scan_once(
        &self,
        table: &str,
        streams: usize,
        seed: u64,
        clock: &Arc<ArmClock>,
        seconds: &Arc<Mutex<Vec<Window>>>,
    ) -> ScanEnd {
        let started = Instant::now();
        let routes = Arc::new(Mutex::new(Arc::new(self.routes().await)));
        // every tablet once, in an order drawn from the seed, so every shard starts working
        let mut tablets: Vec<u16> = (0..TABLET_COUNT as u16).collect();
        let mut draw = Seeded::stream(seed, &format!("scan/{table}"));
        for at in (1..tablets.len()).rev() {
            let swap = draw.below(at as u64 + 1) as usize;
            tablets.swap(at, swap);
        }
        let tablets = Arc::new(tablets);
        let next = Arc::new(AtomicUsize::new(0));
        let walked = Arc::new(AtomicUsize::new(0));
        let first = Arc::new(AtomicBool::new(false));
        let pages = Arc::new(AtomicUsize::new(0));
        let partitions = Arc::new(AtomicUsize::new(0));
        let failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let stopped = Arc::new(AtomicBool::new(false));
        let table_id = TableId::of(table);
        let mut handles = Vec::with_capacity(streams.max(1));
        for stream in 0..streams.max(1) {
            // each stream a driver of its own over the same members, their clients shared
            let driver = ScanDriver {
                members: self.members.clone(),
            };
            let table = table.to_string();
            let routes = routes.clone();
            let tablets = tablets.clone();
            let next = next.clone();
            let walked = walked.clone();
            let first = first.clone();
            let pages = pages.clone();
            let partitions = partitions.clone();
            let failure = failure.clone();
            let stopped = stopped.clone();
            let clock = clock.clone();
            let seconds = seconds.clone();
            handles.push(tokio::spawn(async move {
                let mut jitter = Seeded::stream(seed, &format!("scan/{table}/jitter/{stream}"));
                loop {
                    // the next tablet nobody took, until there is none or the scan stops
                    if !clock.live() || stopped.load(Ordering::Acquire) {
                        return;
                    }
                    let at = next.fetch_add(1, Ordering::AcqRel);
                    let Some(tablet) = tablets.get(at).copied() else {
                        return;
                    };
                    let walked_tablet = driver
                        .walk_tablet(
                            &table, table_id, tablet, &routes, &clock, &seconds, &first, started, &pages,
                            &partitions, &mut jitter,
                        )
                        .await;
                    match walked_tablet {
                        TabletEnd::Walked => {
                            walked.fetch_add(1, Ordering::AcqRel);
                        }
                        TabletEnd::Stopped => return,
                        TabletEnd::Failed(why) => {
                            // one tablet refused for good fails the scan, which stops it
                            failure.lock().expect("the scan's failure").get_or_insert(why);
                            stopped.store(true, Ordering::Release);
                            return;
                        }
                    }
                }
            }));
        }
        for handle in handles {
            let _ = handle.await;
        }
        // finished only once every tablet's walk reached its end, never because each was taken
        let failure = failure.lock().expect("the scan's failure").take();
        ScanEnd {
            finished: failure.is_none() && walked.load(Ordering::Acquire) == tablets.len(),
            pages: pages.load(Ordering::Acquire) as u64,
            partitions: partitions.load(Ordering::Acquire) as u64,
            took: started.elapsed(),
            failure,
        }
    }

    /// Walk one tablet to its end, page by page, its floor raised by every page
    ///
    /// # Arguments
    ///
    /// * `table` - The table, by name
    /// * `table_id` - Its identity, for its leader's route
    /// * `tablet` - The tablet
    /// * `routes` - The routes, read again when a member holds no copy any more
    /// * `clock` - The arm's clock
    /// * `seconds` - Where each page is recorded
    /// * `first` - Whether the scan's first page was answered yet
    /// * `started` - When the scan began
    /// * `pages` - The scan's pages so far
    /// * `partitions` - The scan's partitions so far
    /// * `jitter` - What a shed page's wait is drawn from
    #[allow(clippy::too_many_arguments)]
    async fn walk_tablet(
        &self,
        table: &str,
        table_id: TableId,
        tablet: u16,
        routes: &Arc<Mutex<Arc<Routes>>>,
        clock: &Arc<ArmClock>,
        seconds: &Arc<Mutex<Vec<Window>>>,
        first: &AtomicBool,
        started: Instant,
        pages: &AtomicUsize,
        partitions: &AtomicUsize,
        jitter: &mut Seeded,
    ) -> TabletEnd {
        let kind = OpKind::Supplied(Arc::from(SCAN_KIND));
        let current = routes.lock().expect("the routes").clone();
        let mut candidates = self.candidates(&current, table_id, tablet);
        let mut at = 0usize;
        let mut after: Option<u64> = None;
        let mut floor: Option<u64> = None;
        let mut since_answer = Instant::now();
        loop {
            if !clock.live() {
                return TabletEnd::Stopped;
            }
            let member = &self.members[candidates[at % candidates.len()]];
            // asked as a scan's page, so it never takes a driver's admission (F116)
            let ask = AdminKind::Walk {
                table: table.to_string(),
                tablet,
                low: None,
                high: None,
                after,
                limit: None,
                bytes: None,
                at_least: floor,
                class: Some(WalkClass::Scan),
            };
            let sent = Instant::now();
            let asked = ask_page(member, ask).await;
            match asked {
                Asked::Page(answer) => {
                    // the page, in the second it was answered, and the scan's first if it is
                    let latency = sent.elapsed();
                    let figures = figures_of(&answer);
                    record(seconds, clock, |window| {
                        window.record(&kind, Outcome::Ok(latency));
                        window.scan_mut().page(&member.name, &figures);
                        if !first.swap(true, Ordering::AcqRel) {
                            window.scan_mut().first_page_us.push(micros(started.elapsed()));
                        }
                    });
                    pages.fetch_add(1, Ordering::AcqRel);
                    partitions.fetch_add(answer.keys.len(), Ordering::AcqRel);
                    // the walk moves past the page, its floor never lowered
                    floor = floor.max(answer.boundary);
                    after = answer.next;
                    since_answer = Instant::now();
                    if after.is_none() {
                        return TabletEnd::Walked;
                    }
                }
                Asked::Shed => {
                    // asked again of the same copy soon, a little apart from the other streams
                    record(seconds, clock, |window| {
                        window.record(&kind, Outcome::Retried);
                        window.scan_mut().shed += 1;
                    });
                    let wait = SHED_AFTER + Duration::from_micros(jitter.below(SHED_JITTER_US));
                    tokio::time::sleep(wait).await;
                }
                Asked::Wait(code, message) => {
                    // a copy that cannot answer yet, or a member not reached: another holder, after a while
                    record(seconds, clock, |window| {
                        window.record(&kind, Outcome::Retried);
                        window.scan_mut().wait(&code, message);
                    });
                    at += 1;
                    tokio::time::sleep(RETRY_AFTER).await;
                }
                Asked::Stale(message) => {
                    // the topology moved: its holders as they are now, from the first again
                    record(seconds, clock, |window| {
                        window.record(&kind, Outcome::Retried);
                        window.scan_mut().wait("StaleTopology", message);
                    });
                    let fresh = Arc::new(self.routes().await);
                    *routes.lock().expect("the routes") = fresh.clone();
                    candidates = self.candidates(&fresh, table_id, tablet);
                    at = 0;
                    tokio::time::sleep(RETRY_AFTER).await;
                }
                Asked::Refused(why) => {
                    record(seconds, clock, |window| {
                        window.record(
                            &kind,
                            Outcome::Failed {
                                code: "Refused".to_string(),
                                message: why.clone(),
                            },
                        );
                    });
                    return TabletEnd::Failed(format!("tablet {tablet} of {table}: {why}"));
                }
            }
            if since_answer.elapsed() > GIVE_UP {
                let why = format!("tablet {tablet} of {table}: a page went unanswered for {GIVE_UP:?}");
                record(seconds, clock, |window| {
                    window.record(
                        &kind,
                        Outcome::Failed {
                            code: "GaveUp".to_string(),
                            message: why.clone(),
                        },
                    );
                });
                return TabletEnd::Failed(why);
            }
        }
    }

    /// Run one scan arm until its time is up or it is stopped: whole scans back to back
    ///
    /// # Arguments
    ///
    /// * `arm` - The arm
    /// * `warmup` - How long it runs before it is measured
    /// * `duration` - How long it is measured
    /// * `seed` - The run's seed, which each scan's order is drawn from
    /// * `stream_name` - The name the scan's draws are taken under, the arm's id and run
    /// * `clock` - The arm's clock, started by the caller
    /// * `progress` - Where each second is sent
    #[allow(clippy::too_many_arguments)]
    pub async fn run_arm(
        &self,
        arm: &ScanArm,
        warmup: Duration,
        duration: Duration,
        seed: u64,
        stream_name: &str,
        clock: Arc<ArmClock>,
        progress: &Progress,
    ) -> ArmOutcome {
        let seconds: Arc<Mutex<Vec<Window>>> = Arc::new(Mutex::new(Vec::new()));
        let streams = arm.streams.unwrap_or_else(|| self.default_streams()).max(1);
        // each second reported as it closes, with how busy the driver was
        let done = Arc::new(AtomicBool::new(false));
        let reporter = report_seconds(seconds.clone(), clock.clone(), progress.clone(), done.clone(), warmup, duration);
        // whole scans back to back, each drawn its own order, until the clock stops
        let mut round = 0u64;
        while clock.live() {
            let draw = Seeded::stream(seed, &format!("{stream_name}/{round}")).next_u64();
            let end = self.scan_once(&arm.table, streams, draw, &clock, &seconds).await;
            round += 1;
            if end.finished {
                record_whole(&seconds, &clock, end.took);
            }
            if let Some(failure) = end.failure {
                progress.log(format!("a scan of {} failed: {failure}", arm.table));
                // a refusal for good would only be met again, so the arm ends with it
                clock.stop(&format!("a scan failed: {failure}"));
            }
        }
        done.store(true, Ordering::Relaxed);
        let cpu = reporter.await.unwrap_or_default();
        let seconds = std::mem::take(&mut *seconds.lock().expect("the scan's seconds"));
        ArmOutcome {
            seconds,
            cpu,
            ended_early: clock.ended_early(),
            deadline: clock.deadline(),
            feeds: Vec::new(),
        }
    }
}

/// Ask one page of a member, once
///
/// # Arguments
///
/// * `member` - The member
/// * `kind` - The walk's page
async fn ask_page<S>(member: &ScanMember<S>, kind: AdminKind) -> Asked
where
    S: QuerySupport + Send + Sync + 'static,
{
    let request = AdminRequest {
        op: shoal::uuid::Uuid::new_v4(),
        expected_version: 0,
        kind,
    };
    // a member restarting drops its connections, and is asked again once it is back
    let response = match member.client.admin(&request).await {
        Ok(response) => response,
        Err(error) => return Asked::Wait("Unreached".to_string(), format!("{}: {error:?}", member.name)),
    };
    match response.outcome {
        Ok(AdminOutcome::Read(json)) => match serde_json::from_value::<WalkAnswer>(json) {
            Ok(answer) => Asked::Page(Box::new(answer)),
            Err(error) => Asked::Refused(format!("{} answered a page that does not read: {error}", member.name)),
        },
        Ok(other) => Asked::Refused(format!("{} answered a walk with {other:?}", member.name)),
        Err(error) => match error.code() {
            ErrorCode::Shedding => Asked::Shed,
            // a copy starting, installing, behind its floor or set aside is asked of another
            code @ (ErrorCode::Unavailable | ErrorCode::Timeout | ErrorCode::Quarantined) => {
                Asked::Wait(format!("{code:?}"), format!("{}: {}", member.name, error.msg))
            }
            ErrorCode::StaleTopology => Asked::Stale(format!("{}: {}", member.name, error.msg)),
            code => Asked::Refused(format!("{} refused a page: {} ({code:?})", member.name, error.msg)),
        },
    }
}

/// What a page's answer held, as a scan's window counts it
///
/// # Arguments
///
/// * `answer` - The page
fn figures_of(answer: &WalkAnswer) -> PageFigures {
    PageFigures {
        partitions: answer.keys.len() as u64,
        rows: answer.rows.iter().map(|rows| u64::from(*rows)).sum(),
        bytes: answer.bytes,
        resident: u64::from(answer.resident),
        archived: u64::from(answer.archived),
        reprobed: u64::from(answer.reprobed),
        held_us: answer.held_us,
        read_us: answer.read_us,
    }
}

/// A duration in whole microseconds, saturating
///
/// # Arguments
///
/// * `took` - The duration
fn micros(took: Duration) -> u64 {
    u64::try_from(took.as_micros()).unwrap_or(u64::MAX)
}

/// Count a whole scan that ended now, in the window of the clock's current second
///
/// # Arguments
///
/// * `seconds` - The windows, one a second
/// * `clock` - The arm's clock
/// * `took` - How long the scan took
pub fn record_whole(seconds: &Mutex<Vec<Window>>, clock: &ArmClock, took: Duration) {
    record(seconds, clock, |window| {
        let scan = window.scan_mut();
        scan.scans += 1;
        scan.whole_us.push(micros(took));
    });
}

/// Record something in the window of the clock's current second
///
/// # Arguments
///
/// * `seconds` - The windows, one a second
/// * `clock` - The arm's clock
/// * `what` - What to record
fn record(seconds: &Mutex<Vec<Window>>, clock: &ArmClock, what: impl FnOnce(&mut Window)) {
    // the second's window, made if it is the first thing recorded in it
    let second = clock.second();
    let mut windows = seconds.lock().expect("the scan's seconds");
    while windows.len() <= second {
        windows.push(Window::default());
    }
    what(&mut windows[second]);
}

/// Send each second of a scan arm as it closes, and keep how busy the driver was in it
///
/// # Arguments
///
/// * `seconds` - The arm's windows
/// * `clock` - The arm's clock
/// * `progress` - Where each second is sent
/// * `done` - Set once the arm is over
/// * `warmup` - How long the arm warms up
/// * `duration` - How long it is measured
fn report_seconds(
    seconds: Arc<Mutex<Vec<Window>>>,
    clock: Arc<ArmClock>,
    progress: Progress,
    done: Arc<AtomicBool>,
    warmup: Duration,
    duration: Duration,
) -> tokio::task::JoinHandle<Vec<f64>> {
    let warmup_secs = warmup.as_secs() as usize;
    let measured_end = warmup_secs + duration.as_secs() as usize;
    tokio::spawn(async move {
        let mut cpu = Vec::new();
        let mut ticks = crate::driver::cpu_ticks();
        let mut reported = 0usize;
        while !done.load(Ordering::Relaxed) {
            // a little past each second's end, so its last page is in
            let next = clock.started() + Duration::from_secs(reported as u64 + 1);
            tokio::time::sleep_until(tokio::time::Instant::from_std(next) + Duration::from_millis(5)).await;
            let now = crate::driver::cpu_ticks();
            let busy = match (ticks, now) {
                (Some(before), Some(after)) => {
                    after.saturating_sub(before) as f64 / crate::driver::TICKS_PER_SEC * 100.0
                }
                _ => 0.0,
            };
            ticks = now;
            cpu.push(busy);
            let phase = if reported < warmup_secs {
                SecondPhase::Warmup
            } else if reported < measured_end {
                SecondPhase::Measure
            } else {
                SecondPhase::Drain
            };
            let summary = seconds
                .lock()
                .expect("the scan's seconds")
                .get(reported)
                .map(|window| window.summary(Duration::from_secs(1)))
                .unwrap_or_default();
            progress.send(BenchEvent::Second(SecondSample {
                at: reported as u64,
                phase,
                summary,
                driver_cpu_pct: busy,
            }));
            reported += 1;
        }
        cpu
    })
}
