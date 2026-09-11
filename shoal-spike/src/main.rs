//! The Q1/Q13 spike: how many embedded openraft groups a control thread can hold, and at what cost
//!
//! [C13](../../docs/src/distributed/protocol.md) asks two questions before the data plane is
//! built on an embedded consensus library. Q1: which library and runtime, with evidence. Q13:
//! what a group costs, so that the number of tablet groups a node runs is a number rather than
//! a hope. This program answers both for the pair M1 chose - openraft `0.10.0-alpha.34` on
//! the glommio runtime in `shoal-core/src/server/control/runtime` - and answers them the way
//! the decision record wants: a table, printed, labelled by host and governor.
//!
//! One pinned executor holds N groups of three members each. Every member is a real `Raft`
//! with its own log and state machine; the network between them is a loopback that calls the
//! target's RPC handlers in the same thread and counts every call. The members of one group
//! elect a leader the ordinary way. Then:
//!
//! - the executor is left alone for a window, and the resident set, the process CPU time and
//!   the message count over that window are what an idle group costs
//! - with durable stores - the control store itself, under a temp dir - one group's appends
//!   are timed alone, and then every group's at once, which is what says whether fsyncs from
//!   many groups on one thread queue behind each other
//!
//! Nothing here is a capture. It runs once, on the machine named in its output, and its numbers
//! are pasted into `protocol.md` beside the date. `shoal-bench` measures what is compared
//! across commits; this measures what decides a design.
//!
//! raft-rs was not measured. Its `RawNode` is a state machine driven by the caller with no
//! runtime abstraction to adapt, so a comparison on this axis would be measuring the harness
//! written around it rather than the library; the decision record says so.

use std::cell::RefCell;
use std::collections::{BTreeMap, Bound};
use std::fmt::Debug;
use std::future::Future;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt as _};
use glommio::{LocalExecutorBuilder, Placement};
use openraft::entry::RaftEntry as _;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse};
use openraft::storage::{EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf};
use openraft::{Config, EntryPayload, OptionalSend, Raft, RaftNetworkFactory, RaftNetworkV2, Snapshot, SnapshotMeta, StoredMembership};
use shoal::server::control::store::{self as durable, SnapshotData};
use shoal::server::control::types::{ControlCommand, ControlConfig, ControlResponse, ControlState, MemberRecord};
use shoal::shared::identity::NodeId;

/// The group counts the idle cost is measured at
const IDLE_COUNTS: &[usize] = &[1, 64, 1024, 4096];

/// How long each idle window is
const IDLE_WINDOW: Duration = Duration::from_secs(10);

/// The group count the concurrent durable append is measured at
const DURABLE_COUNT: usize = 64;

/// How many appends each durable timing takes
const APPENDS: usize = 200;

/// A raft in the spike
type SpikeRaft<SM> = Raft<ControlConfig, SM>;

/// One entry of a spike log
type Entry = EntryOf<ControlConfig>;

/// One log id
type LogId = LogIdOf<ControlConfig>;

/// One vote
type Vote = VoteOf<ControlConfig>;

/// A member record for a spike node, which advertises nothing
///
/// # Arguments
///
/// * `node` - The node
fn member(node: NodeId) -> MemberRecord {
    MemberRecord {
        node,
        ..MemberRecord::default()
    }
}

/// The counters the loopback keeps
#[derive(Default)]
struct Counters {
    /// Append entries calls, which is heartbeats and replication both
    appends: u64,
    /// Vote calls
    votes: u64,
}

/// Where the loopback finds a raft by node id
///
/// Generic over the state machine because the memory and durable spikes use different ones,
/// and a `Raft` is generic over its state machine.
struct Registry<SM: RaftStateMachine<ControlConfig>> {
    /// Every raft, by node
    rafts: RefCell<BTreeMap<NodeId, SpikeRaft<SM>>>,
    /// What has crossed the loopback
    counters: RefCell<Counters>,
}

/// A network that calls the target's handlers in the same thread
struct Loopback<SM: RaftStateMachine<ControlConfig>> {
    /// The registry
    registry: Rc<Registry<SM>>,
}

impl<SM: RaftStateMachine<ControlConfig>> Clone for Loopback<SM> {
    /// The same registry
    fn clone(&self) -> Self {
        Loopback {
            registry: self.registry.clone(),
        }
    }
}

impl<SM: RaftStateMachine<ControlConfig, SnapshotData = SnapshotData>> RaftNetworkFactory<ControlConfig>
    for Loopback<SM>
{
    type Network = LoopbackPeer<SM>;

    /// A client for a peer, which is a name to look up on each call
    async fn new_client(&mut self, target: NodeId, _node: &MemberRecord) -> Self::Network {
        LoopbackPeer {
            registry: self.registry.clone(),
            target,
        }
    }
}

/// One peer, reached through the registry
struct LoopbackPeer<SM: RaftStateMachine<ControlConfig>> {
    /// The registry
    registry: Rc<Registry<SM>>,
    /// Who
    target: NodeId,
}

/// Why a peer was not found
#[derive(Debug)]
struct NotRegistered(NodeId);

impl std::fmt::Display for NotRegistered {
    /// Say who
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} is not registered", self.0)
    }
}

impl std::error::Error for NotRegistered {}

impl<SM: RaftStateMachine<ControlConfig>> LoopbackPeer<SM> {
    /// The target's raft handle, cloned out of the registry so the borrow ends before the call
    fn target(&self) -> Result<SpikeRaft<SM>, RPCError<ControlConfig>> {
        self.registry
            .rafts
            .borrow()
            .get(&self.target)
            .cloned()
            .ok_or_else(|| RPCError::Unreachable(Unreachable::new(&NotRegistered(self.target))))
    }
}

impl<SM: RaftStateMachine<ControlConfig, SnapshotData = SnapshotData>> RaftNetworkV2<ControlConfig>
    for LoopbackPeer<SM>
{
    type SnapshotData = SnapshotData;

    /// Hand the request to the target's handler
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<ControlConfig>, RPCError<ControlConfig>> {
        self.registry.counters.borrow_mut().appends += 1;
        let target = self.target()?;
        target
            .append_entries(rpc)
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))
    }

    /// Hand the request to the target's handler
    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ControlConfig>, RPCError<ControlConfig>> {
        self.registry.counters.borrow_mut().votes += 1;
        let target = self.target()?;
        target
            .vote(rpc)
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))
    }

    /// Hand the snapshot to the target's handler
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<ControlConfig>,
        snapshot: SnapshotOf<ControlConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<ControlConfig>, StreamingError<ControlConfig>> {
        let target = self
            .target()
            .map_err(|_| StreamingError::Unreachable(Unreachable::new(&NotRegistered(self.target))))?;
        target
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|error| StreamingError::Unreachable(Unreachable::new(&error)))
    }
}

/// An in-memory log: the control store's shape with the files taken out
#[derive(Clone, Default)]
struct MemLog {
    /// The shared state
    inner: Rc<RefCell<MemLogInner>>,
}

/// The memory log's state
#[derive(Default)]
struct MemLogInner {
    /// Every entry, by index
    entries: BTreeMap<u64, Entry>,
    /// The last vote
    vote: Option<Vote>,
    /// The last committed log id
    committed: Option<LogId>,
    /// The last purged log id
    purged: Option<LogId>,
}

impl RaftLogReader<ControlConfig> for MemLog {
    /// The entries in a range
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let inner = self.inner.borrow();
        let start = match range.start_bound() {
            Bound::Included(s) => Bound::Included(*s),
            Bound::Excluded(s) => Bound::Excluded(*s),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(e) => Bound::Included(*e),
            Bound::Excluded(e) => Bound::Excluded(*e),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(inner.entries.range((start, end)).map(|(_, e)| e.clone()).collect())
    }

    /// The last vote
    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.inner.borrow().vote.clone())
    }
}

impl RaftLogStorage<ControlConfig> for MemLog {
    type LogReader = MemLog;

    /// Where the log begins and ends
    async fn get_log_state(&mut self) -> Result<LogState<ControlConfig>, io::Error> {
        let inner = self.inner.borrow();
        let last = inner
            .entries
            .values()
            .next_back()
            .map(|e| e.log_id())
            .or_else(|| inner.purged.clone());
        Ok(LogState {
            last_purged_log_id: inner.purged.clone(),
            last_log_id: last,
        })
    }

    /// A reader
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Record a vote
    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        self.inner.borrow_mut().vote = Some(vote.clone());
        Ok(())
    }

    /// Record the committed id
    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        self.inner.borrow_mut().committed = committed;
        Ok(())
    }

    /// The committed id
    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.inner.borrow().committed.clone())
    }

    /// Append, and complete the callback at once
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<ControlConfig>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.inner.borrow_mut();
            for entry in entries {
                inner.entries.insert(entry.index(), entry);
            }
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    /// Drop everything after
    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let keep = last_log_id.map(|id| id.index());
        self.inner
            .borrow_mut()
            .entries
            .retain(|index, _| keep.is_some_and(|k| *index <= k));
        Ok(())
    }

    /// Drop everything up to
    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let mut inner = self.inner.borrow_mut();
        inner.entries.retain(|index, _| *index > log_id.index());
        inner.purged = Some(log_id);
        Ok(())
    }
}

/// An in-memory state machine over the control state
#[derive(Clone, Default)]
struct MemMachine {
    /// The shared state
    inner: Rc<RefCell<MemMachineInner>>,
}

/// The memory state machine's state
#[derive(Default)]
struct MemMachineInner {
    /// The last applied
    applied: Option<LogId>,
    /// The last membership
    membership: StoredMembershipOf<ControlConfig>,
    /// The application state
    state: ControlState,
    /// The last snapshot
    snapshot: Option<SnapshotOf<ControlConfig, SnapshotData>>,
}

impl RaftSnapshotBuilder<ControlConfig> for MemMachine {
    type SnapshotData = SnapshotData;

    /// A snapshot of the state
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<ControlConfig, SnapshotData>, io::Error> {
        let mut inner = self.inner.borrow_mut();
        let meta = SnapshotMeta {
            last_log_id: inner.applied.clone(),
            last_membership: inner.membership.clone(),
        };
        let data = serde_json_bytes(&inner.state)?;
        let snapshot = Snapshot {
            meta,
            snapshot: Cursor::new(data),
        };
        inner.snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }
}

/// Serialize a state, through the facade's serde_json
///
/// # Arguments
///
/// * `state` - The state
fn serde_json_bytes(state: &ControlState) -> io::Result<Vec<u8>> {
    shoal::serde_json::to_vec(state).map_err(io::Error::other)
}

impl RaftStateMachine<ControlConfig> for MemMachine {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = MemMachine;

    /// What has been applied
    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembershipOf<ControlConfig>), io::Error> {
        let inner = self.inner.borrow();
        Ok((inner.applied.clone(), inner.membership.clone()))
    }

    /// Apply a batch
    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<ControlConfig>, io::Error>> + Unpin + OptionalSend,
    {
        while let Some(next) = entries.next().await {
            let (entry, responder) = next?;
            let log_id = entry.log_id();
            let mut inner = self.inner.borrow_mut();
            let response = match entry.payload {
                EntryPayload::Blank => ControlResponse::Applied {
                    topology_version: inner.state.topology_version,
                },
                EntryPayload::Normal(command) => inner.state.apply(&command),
                EntryPayload::Membership(membership) => {
                    inner.membership = StoredMembership::new(Some(log_id.clone()), membership);
                    ControlResponse::Applied {
                        topology_version: inner.state.topology_version,
                    }
                }
            };
            inner.applied = Some(log_id);
            drop(inner);
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    /// The builder
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// Install a snapshot
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ControlConfig>,
        snapshot: SnapshotData,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let state: ControlState = shoal::serde_json::from_slice(&data).map_err(io::Error::other)?;
        let mut inner = self.inner.borrow_mut();
        inner.state = state;
        inner.applied = meta.last_log_id.clone();
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Cursor::new(data),
        });
        Ok(())
    }

    /// The last snapshot
    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<ControlConfig, SnapshotData>>, io::Error> {
        Ok(self.inner.borrow().snapshot.clone())
    }
}

/// The resident set of this process, in bytes
fn resident_bytes() -> u64 {
    // the second field of statm is resident pages
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(0);
    // SAFETY: `sysconf` reads a constant
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
    pages * page
}

/// The CPU time this process has used, user and system together
fn cpu_time() -> Duration {
    // SAFETY: a zeroed rusage is a valid out parameter, filled by the call
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let user = Duration::new(usage.ru_utime.tv_sec as u64, (usage.ru_utime.tv_usec * 1000) as u32);
    let system = Duration::new(usage.ru_stime.tv_sec as u64, (usage.ru_stime.tv_usec * 1000) as u32);
    user + system
}

/// The CPU governor cpu 0 runs under, or what the file said instead
fn governor() -> String {
    std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
        .map(|g| g.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// This machine's name
fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|h| h.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Which timers a group runs under
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Timers {
    /// openraft's defaults: 50 ms heartbeats, 150-300 ms elections
    OpenraftDefault,
    /// The timers [C1](../../docs/src/distributed/node-identity.md) proposes for the control
    /// plane: 500 ms heartbeats and elections at 1.5-3 s, which is `primary_failover_after`'s
    /// shape applied to a group whose members are on one machine
    C1Proposal,
}

impl Timers {
    /// The openraft config for these timers
    fn config(self) -> Arc<Config> {
        let (heartbeat, min, max) = match self {
            Timers::OpenraftDefault => {
                let d = Config::default();
                (d.heartbeat_interval, d.election_timeout_min, d.election_timeout_max)
            }
            Timers::C1Proposal => (500, 1500, 3000),
        };
        Arc::new(
            Config {
                cluster_name: "spike".to_string(),
                heartbeat_interval: heartbeat,
                election_timeout_min: min,
                election_timeout_max: max,
                ..Config::default()
            }
            .validate()
            .expect("the timers validate"),
        )
    }

    /// One line naming these timers
    fn label(self) -> String {
        let config = self.config();
        format!(
            "heartbeat {} ms, election {}-{} ms",
            config.heartbeat_interval, config.election_timeout_min, config.election_timeout_max
        )
    }
}

/// Start N groups of three, each with stores the builder makes, and wait for every leader
///
/// # Arguments
///
/// * `count` - How many groups
/// * `timers` - The timers every group runs under
/// * `registry` - Where the rafts go
/// * `stores` - Makes a log and a state machine for one member
async fn start_groups<SM, LS, F, Fut>(
    count: usize,
    timers: Timers,
    registry: &Rc<Registry<SM>>,
    mut stores: F,
) -> Vec<SpikeRaft<SM>>
where
    SM: RaftStateMachine<ControlConfig, SnapshotData = SnapshotData>,
    LS: RaftLogStorage<ControlConfig>,
    F: FnMut(NodeId) -> Fut,
    Fut: Future<Output = (LS, SM)>,
{
    let config = timers.config();
    let mut leaders = Vec::with_capacity(count);
    for group in 0..count {
        // three members, ids that never collide across groups
        let ids: Vec<NodeId> = (0..3)
            .map(|i| NodeId::from((group * 3 + i + 1) as u64))
            .collect();
        let mut first = None;
        for id in &ids {
            let (log, machine) = stores(*id).await;
            let raft = Raft::<ControlConfig, SM>::new(
                *id,
                config.clone(),
                Loopback {
                    registry: registry.clone(),
                },
                log,
                machine,
            )
            .await
            .expect("a raft starts");
            registry.rafts.borrow_mut().insert(*id, raft.clone());
            first.get_or_insert(raft);
        }
        let first = first.expect("three members");
        let members: BTreeMap<NodeId, MemberRecord> = ids.iter().map(|id| (*id, member(*id))).collect();
        first.initialize(members).await.expect("a group initializes");
        leaders.push(first);
    }
    // every group has an established leader before anything is measured: one that has seen
    // its own first entry commit, which is what `state == Leader` says
    for raft in &mut leaders {
        let metrics = raft
            .wait(Some(Duration::from_secs(120)))
            .metrics(|m| m.current_leader.is_some(), "a leader")
            .await
            .expect("a group elects a leader");
        let leader = metrics.current_leader.expect("a leader");
        // measure through the leader, whoever it turned out to be
        let leader = registry.rafts.borrow().get(&leader).cloned().expect("the leader is registered");
        leader
            .wait(Some(Duration::from_secs(120)))
            .state(openraft::ServerState::Leader, "the leader is established")
            .await
            .expect("a leader establishes itself");
        *raft = leader;
    }
    leaders
}

/// Shut every raft in a registry down, and empty it
///
/// # Arguments
///
/// * `registry` - The registry
async fn stop_all<SM: RaftStateMachine<ControlConfig>>(registry: &Rc<Registry<SM>>) {
    let rafts: Vec<SpikeRaft<SM>> = std::mem::take(&mut *registry.rafts.borrow_mut())
        .into_values()
        .collect();
    for raft in rafts {
        let _ = raft.shutdown().await;
    }
}

/// Take a percentile of sorted samples
///
/// # Arguments
///
/// * `sorted` - The samples, ascending
/// * `p` - The percentile, 0 to 100
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

/// Render a duration in microseconds
///
/// # Arguments
///
/// * `d` - The duration
fn us(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1e6)
}

/// Time `APPENDS` writes through one leader, one after another
///
/// A write refused with a forward to another leader follows it: a group whose leadership moved
/// mid-measurement is timed through whoever leads it now, and the sample includes the move.
///
/// # Arguments
///
/// * `leader` - The leader
/// * `registry` - Where a new leader is found
async fn time_appends<SM: RaftStateMachine<ControlConfig>>(
    leader: &SpikeRaft<SM>,
    registry: &Rc<Registry<SM>>,
) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(APPENDS);
    let mut leader = leader.clone();
    for _ in 0..APPENDS {
        let started = Instant::now();
        loop {
            match leader
                .client_write(ControlCommand::ObserveMember(member(NodeId::from(1))))
                .await
            {
                Ok(_) => break,
                Err(openraft::error::RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(
                    forward,
                ))) => {
                    // follow the leader, or wait for one to be elected
                    match forward.leader_id.and_then(|id| registry.rafts.borrow().get(&id).cloned()) {
                        Some(next) => leader = next,
                        None => {
                            glommio::timer::Timer::new(Duration::from_millis(50)).await;
                        }
                    }
                }
                Err(error) => panic!("a write failed: {error}"),
            }
        }
        samples.push(started.elapsed());
    }
    samples.sort();
    samples
}

/// The idle cost of N groups on memory stores
///
/// # Arguments
///
/// * `count` - How many groups
/// * `timers` - The timers they run under
async fn idle(count: usize, timers: Timers) -> String {
    let registry = Rc::new(Registry::<MemMachine> {
        rafts: RefCell::new(BTreeMap::new()),
        counters: RefCell::new(Counters::default()),
    });
    let before_rss = resident_bytes();
    let started = Instant::now();
    let leaders =
        start_groups(count, timers, &registry, |_| async { (MemLog::default(), MemMachine::default()) }).await;
    let startup = started.elapsed();
    // settle, then measure a window with nothing but heartbeats in it
    glommio::timer::Timer::new(Duration::from_secs(1)).await;
    let rss = resident_bytes();
    let cpu_before = cpu_time();
    let appends_before = registry.counters.borrow().appends;
    let window_started = Instant::now();
    glommio::timer::Timer::new(IDLE_WINDOW).await;
    let window = window_started.elapsed();
    let cpu = cpu_time() - cpu_before;
    let appends = registry.counters.borrow().appends - appends_before;
    let per_group_rss = (rss.saturating_sub(before_rss)) as f64 / count as f64 / 1024.0;
    let row = format!(
        "| {count} | {} | {:.1} | {:.1} | {:.2} | {:.0} | {:.1} |",
        leaders.len(),
        rss as f64 / 1024.0 / 1024.0,
        per_group_rss,
        cpu.as_secs_f64() / window.as_secs_f64() * 100.0,
        appends as f64 / window.as_secs_f64(),
        startup.as_secs_f64(),
    );
    stop_all(&registry).await;
    row
}

/// Durable append latency: one group alone, then every group at once
async fn durable() -> Vec<String> {
    let dir = tempfile::tempdir().expect("a temp dir");
    let root = dir.path().to_path_buf();
    let registry = Rc::new(Registry::<durable::ControlStateMachine> {
        rafts: RefCell::new(BTreeMap::new()),
        counters: RefCell::new(Counters::default()),
    });
    let make = |root: std::path::PathBuf| {
        move |id: NodeId| {
            let path = root.join(id.to_string());
            async move {
                durable::open(&path).await.expect("a durable store opens")
            }
        }
    };
    // one group, alone
    let leaders = start_groups(1, Timers::C1Proposal, &registry, make(root.clone())).await;
    let alone = time_appends(&leaders[0], &registry).await;
    stop_all(&registry).await;
    let mut rows = vec![format!(
        "| 1 | 1 | {} | {} | {} |",
        us(percentile(&alone, 50.0)),
        us(percentile(&alone, 99.0)),
        us(*alone.last().unwrap_or(&Duration::ZERO)),
    )];
    // many groups, all appending at once: each leader writes `APPENDS` entries in its own task
    let leaders = start_groups(DURABLE_COUNT, Timers::C1Proposal, &registry, make(root.join("many"))).await;
    let mut tasks = Vec::with_capacity(leaders.len());
    for leader in leaders {
        let registry = registry.clone();
        tasks.push(glommio::spawn_local(async move { time_appends(&leader, &registry).await }).detach());
    }
    let mut all = Vec::with_capacity(DURABLE_COUNT * APPENDS);
    for task in tasks {
        all.extend(task.await.expect("a timing task finishes"));
    }
    all.sort();
    stop_all(&registry).await;
    rows.push(format!(
        "| {DURABLE_COUNT} | {DURABLE_COUNT} | {} | {} | {} |",
        us(percentile(&all, 50.0)),
        us(percentile(&all, 99.0)),
        us(*all.last().unwrap_or(&Duration::ZERO)),
    ));
    rows
}

/// Run the spike on a pinned executor and print the tables
fn main() {
    println!("shoal-spike: openraft {} on the glommio runtime", "0.10.0-alpha.34");
    println!("host {} · governor {} · pinned to cpu 1 · three members per group", hostname(), governor());
    println!();
    let handle = LocalExecutorBuilder::new(Placement::Fixed(1))
        .name("shoal-spike")
        .spawn(|| async move {
            // a warm-up, so the first row measures a group and not the executor's first touch
            // of its own memory
            let _ = idle(1, Timers::OpenraftDefault).await;
            for timers in [Timers::OpenraftDefault, Timers::C1Proposal] {
                println!(
                    "## Idle cost, memory stores, {}s window, {}",
                    IDLE_WINDOW.as_secs(),
                    timers.label()
                );
                println!();
                println!("| groups | leaders | RSS MiB | RSS delta/group KiB | idle CPU % of one core | append_entries/s | startup s |");
                println!("| --- | --- | --- | --- | --- | --- | --- |");
                for count in IDLE_COUNTS {
                    let row = idle(*count, timers).await;
                    println!("{row}");
                }
                println!();
            }
            println!(
                "## Durable append, control store under a temp dir, {APPENDS} writes per leader, {}",
                Timers::C1Proposal.label()
            );
            println!();
            println!("| groups | leaders writing at once | p50 µs | p99 µs | max µs |");
            println!("| --- | --- | --- | --- | --- |");
            for row in durable().await {
                println!("{row}");
            }
        })
        .expect("the spike's executor spawns");
    handle.join().expect("the spike finishes");
}
