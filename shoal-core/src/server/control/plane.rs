//! The control thread: one pinned executor, one group, and the pool's handle to both
//!
//! [`ControlPlane::start`] spawns a thread, pins it to the control core, builds a glommio
//! executor on it, opens the store, and runs the group. What it does next depends on what the
//! directory is ([F39](../../../../docs/src/features/membership.md)):
//!
//! - a **fresh bootstrap** initializes the group with itself, waits to lead it, and writes the
//!   [`ControlCommand::Bootstrap`] that creates the cluster;
//! - a **joiner** dials its seeds' control lanes with no cluster, asks the leader to admit it,
//!   adopts the cluster the leader names into its marker, and waits for the leader's
//!   replication to tell it who its members are;
//! - a **member restarting** does neither: its group has a log, its peers are the committed
//!   members, and it comes up whether or not any of them answers.
//!
//! Every one of them then observes itself through the leader - where it is, and which start
//! of it this is - and is `Joined` once that commits. Readiness is reported before that, once
//! the store is open, the group built and the listener bound, so a member whose peers are all
//! gone still comes up, reports its identity and its log, and never re-initializes.
//!
//! The thread is **one loop over one channel of events**: the pool's requests, the shards'
//! admin calls, the membership RPCs the listener hands over, two timers, the group's metrics
//! and the store's applied hook all post to it. Anything that awaits - a proposal, a join, a
//! promotion, a ping - is a task spawned with clones of the handles it needs and posts what it
//! learned back as another event, so no `RefCell` is ever held across an await.
//!
//! The [`Raft`] handle never leaves this thread. The pool talks to it over a channel, and a
//! shard never waits on the control plane for an ordinary read or write
//! ([C1](../../../../docs/src/distributed/node-identity.md)).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use glommio::{LocalExecutorBuilder, Placement};
use openraft::error::{ClientWriteError, RaftError};
use openraft::metrics::RaftMetrics;
use openraft::raft::VoteRequest;
use openraft::{ChangeMembers, Config, Raft, RaftNetworkV2};
use openraft_rt::WatchReceiver as _;
use serde::{Deserialize, Serialize};
use tracing::{event, instrument, Level};
use uuid::Uuid;

use super::cores::ControlPlacement;
use super::detector::Detector;
use super::listener::{control_acceptor, err, ok, Inbound};
use super::network::{PeerNetwork, RpcFailure};
use super::store::{self, ControlStateMachine, CONTROL_DIR};
use super::plan::{PlanOutcome, PlanPhase, PlanRecord, PlanUpdate, StepState};
use super::planner::{self, NodeInput, PlanInput, SetInput};
use super::repair::{QuarantinedCopy, RepairMode};
use super::types::{
    ControlCommand, ControlConfig, ControlResponse, ControlState, MemberHealth, MemberPhase,
    MemberRecord, MemberRole, MemberState, Tombstone,
};
use crate::server::conf::cluster::{BootstrapPolicy, DialOverride, Migration, PeerTls, Rebalance, Transport};
use crate::server::conf::Conf;
use crate::server::errors::ShoalError;
use crate::server::map::{QuorumShortfall, TabletMap};
use crate::server::meta::{Identity, MarkerMode, StorageMeta};
use crate::server::peer::handshake::PeerAddr;
use crate::server::peer::Local;
use crate::server::ServerError;
use crate::shared::identity::{ClusterId, NodeId, TableId};
use crate::shared::protocol::admin::{
    AdminError, AdminKind, AdminOutcome, AdminRequest, AdminResponse,
};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::peer::{ControlKind, StatusReport};

/// How long the control plane waits for a fresh group of one to elect itself
///
/// A group of one elects itself on its first tick, so this is a bound on a broken runtime
/// rather than on an election.
const LEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a proposal may take to commit, leader search included
pub const PROPOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a write asks its own group again while the leader's lease is being established
///
/// A leader answers a write with an empty forward hint until a quorum has acknowledged it, and
/// the metrics name it as the leader all the while. Asking again at once is a busy loop on the
/// control core that starves the links the acknowledgements ride, so a write polls at this pace.
const LEASE_POLL: Duration = Duration::from_millis(50);

/// How many leaders a proposal follows before it gives up
///
/// A hint may name a leader that has just changed, and a leader whose lease is not established
/// yet answers with no hint at all; each is one more hop.
const PROPOSE_HOPS: usize = 4;

/// How long a node waits after a failed observation before it tries again
///
/// The metrics and the applied index both prompt an observation, and either can change many
/// times a second while a group is settling; without this a failure is retried on every change.
const OBSERVE_BACKOFF: Duration = Duration::from_millis(250);

/// How long a joiner keeps dialling its seeds before it gives up
pub const JOIN_TIMEOUT: Duration = Duration::from_secs(60);

/// How long one join RPC waits for the leader's answer
const JOIN_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a promotion waits for a learner to catch up
const CATCHUP_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the pool waits for the thread to answer a request
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// The longest the leader goes between two commits of a grace's elapsed time
///
/// A grace is committed every eighth of itself or this, whichever is shorter, so a leader
/// change loses at most one increment ([F46](../../../../docs/src/features/capacity-rebalancing.md), Q7).
const GRACE_COMMIT_CAP: Duration = Duration::from_secs(60);

/// The shortest the leader goes between two looks at its plans, however often the state moves
const PLAN_MIN_INTERVAL: Duration = Duration::from_millis(250);

/// How many times a plan's move is proposed again after the version moved under it
const PLAN_MOVE_RETRIES: usize = 8;

/// What the control plane tells the pool
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEvent {
    /// The store is open, the group built and the listener bound
    Ready,
    /// The control plane failed, before or after it was ready
    Failed(String),
}

/// What one control node's probe learned about a peer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VoteProbe {
    /// Whether the peer granted the vote
    pub granted: bool,
}

/// Where a node stands with its control group
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JoinStatus {
    /// A joiner that has not been admitted yet
    Joining,
    /// A member with its log, not yet observed at this incarnation through a leader
    Recovering,
    /// Observed at this incarnation; the cluster has this run on record
    Joined,
}

impl JoinStatus {
    /// The name this status is spelled as
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            JoinStatus::Joining => "joining",
            JoinStatus::Recovering => "recovering",
            JoinStatus::Joined => "joined",
        }
    }
}

/// An administrative request from a shard or the pool, with a way to answer it
pub struct AdminCall {
    /// What is asked
    pub request: AdminRequest,
    /// Who asked, if the connection authenticated
    pub principal: Option<String>,
    /// Whether the caller is the process itself, which needs no principal
    ///
    /// The pool's own seam, which the benchmark harness and the fixture use; a request off the
    /// wire never sets it.
    pub trusted: bool,
    /// Where the answer goes
    pub reply: kanal::Sender<AdminResponse>,
}

/// A shard telling the control plane it died
#[derive(Debug, Clone)]
pub struct ShardHealthEvent {
    /// Which shard
    pub shard: usize,
    /// What it said
    pub error: String,
}

/// Where a new map goes: to every shard, as one `Arc` each
pub type MapSink = Box<dyn FnMut(Arc<TabletMap>) + Send>;

/// What the pool asks the control plane
pub enum ControlRequest {
    /// Describe the cluster as this node sees it
    Topology(mpsc::Sender<TopologyView>),
    /// Say where this node stands
    Readiness(mpsc::Sender<ReadinessView>),
    /// The map as it is now
    Map(mpsc::Sender<Arc<TabletMap>>),
    /// Ping a peer over the control lane and report the round trip
    Ping {
        /// The peer
        node: NodeId,
        /// Where to send how long it took
        reply: mpsc::Sender<Result<Duration, String>>,
    },
    /// Send a peer a vote for a low term and report its answer
    VoteProbe {
        /// The peer
        node: NodeId,
        /// Where to send what it answered
        reply: mpsc::Sender<Result<VoteProbe, String>>,
    },
    /// An administrative request
    Admin(AdminCall),
    /// Where new maps go from now on
    AttachSink(MapSink, mpsc::Sender<()>),
    /// A shard died
    ShardHealth(ShardHealthEvent),
    /// A shard's tablet groups, as it last reported them
    Replication(crate::server::replication::ShardReplication),
    /// A node's own proposal - a repair driver's progress - answered with what it came to
    /// ([F44](../../../../docs/src/features/repair.md))
    Propose {
        /// The command
        command: ControlCommand,
        /// Where its outcome goes
        reply: kanal::Sender<Result<ControlResponse, String>>,
    },
    /// Send the leader one report behind the last, as a replay would be, for a test
    StaleReport(mpsc::Sender<Result<(), String>>),
    /// Stop the group and exit the thread
    Shutdown,
}

/// A cloneable way to make administrative requests as the process, from any thread
///
/// The control thread's request channel and nothing else, so a thread that outlives no
/// handle can still ask ([F44](../../../../docs/src/features/repair.md)).
#[derive(Clone)]
pub struct AdminSender {
    /// Where requests go
    requests: kanal::Sender<ControlRequest>,
}

impl AdminSender {
    /// Make an administrative request as the process itself
    ///
    /// # Arguments
    ///
    /// * `request` - What is asked
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone or did not answer.
    pub fn admin(&self, request: AdminRequest) -> Result<AdminResponse, ServerError> {
        let (reply, rx) = kanal::bounded(1);
        self.requests
            .send(ControlRequest::Admin(AdminCall {
                request,
                principal: None,
                trusted: true,
                reply,
            }))
            .map_err(|_| ServerError::ControlFailed {
                error: "the control thread is not answering".to_string(),
            })?;
        rx.recv_timeout(PROPOSE_TIMEOUT + REQUEST_TIMEOUT).map_err(|_| ServerError::ControlFailed {
            error: "the control thread did not answer an admin request".to_string(),
        })
    }
}

/// The cluster as one node sees it
///
/// Built from the applied state on request, so it is what the committed log says and never a
/// cached copy of it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopologyView {
    /// The cluster, or the nil id on a joiner that has adopted none yet
    pub cluster: ClusterId,
    /// The node reporting
    pub node: NodeId,
    /// Which start of it this is
    pub incarnation: u64,
    /// Where it stands with its group
    pub control: JoinStatus,
    /// The control leader it knows, if any
    pub leader: Option<NodeId>,
    /// How many committed changes the topology has seen
    pub version: u64,
    /// Every member the group knows, in node order
    pub members: Vec<MemberView>,
    /// The members that vote, in node order
    pub voters: Vec<NodeId>,
    /// The members that only learn, in node order
    pub learners: Vec<NodeId>,
    /// Whether a membership change is half way through, so the voters above are a union
    pub joint: bool,
    /// The nodes tablets are placed over, once initialized
    pub initialized: Option<Vec<NodeId>>,
    /// The replication factor the policy asks for
    pub desired_rf: u32,
    /// The replication factor the placement gives
    pub active_rf: u32,
    /// How many replicas short of the policy the cluster is
    pub missing_replicas: u32,
    /// How many members are up
    pub up_members: u32,
    /// The cpu this node's control thread runs on
    pub control_core: usize,
    /// Whether that cpu's physical core is shared with a shard
    pub control_shared: bool,
    /// The policy the cluster runs under
    pub policy: Option<BootstrapPolicy>,
    /// The plans not yet done, in request order
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md))
    #[serde(default)]
    pub plans: Vec<PlanRecord>,
    /// The identities removed for good, by node
    #[serde(default)]
    pub tombstones: BTreeMap<NodeId, Tombstone>,
    /// How many replica sets hold a copy on a member the cluster has given up on
    #[serde(default)]
    pub under_replicated_sets: u32,
    /// The grace the policy removes a down member after, in milliseconds, if it removes
    #[serde(default)]
    pub auto_remove_after_ms: Option<u64>,
}

/// One member as the topology view reports it: the committed state, and what this node adds
///
/// The committed state is flattened, so a reader of the `Members` operation from before
/// [F46](../../../../docs/src/features/capacity-rebalancing.md) finds every field where it
/// was; the rest is derived on the node answering - the one name of the six-state machine, the
/// grace remaining, and the capacity the leader last heard, which is nobody's committed fact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberView {
    /// What the group committed about the member
    #[serde(flatten)]
    pub state: MemberState,
    /// The one name of its state: the phase past plain membership, the health otherwise
    #[serde(default)]
    pub state_name: String,
    /// The weight the planner gives it
    #[serde(default)]
    pub weight: u32,
    /// How much of its grace is left, in milliseconds, while it is under one
    #[serde(default)]
    pub grace_remaining_ms: Option<u64>,
    /// The free bytes it last reported, as the leader heard them
    #[serde(default)]
    pub free_bytes: Option<u64>,
    /// The bytes its groups hold, as the leader last heard them
    #[serde(default)]
    pub held_bytes: Option<u64>,
}

impl std::ops::Deref for MemberView {
    type Target = MemberState;

    /// The committed state, which is what most readers of a member want
    fn deref(&self) -> &MemberState {
        &self.state
    }
}

/// What the leader last heard about one member's capacity
///
/// Reported, never committed: the plan the leader derives from it is what goes in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeCapacity {
    /// The free bytes on its storage, as it read them
    pub free_bytes: u64,
    /// The bytes each of its groups holds, by group number
    pub group_bytes: BTreeMap<u64, u64>,
    /// When it was heard
    pub at: Instant,
    /// The incarnation it reported at
    pub incarnation: u64,
}

/// What the leader has counted of one down member's grace since it last committed it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GraceLocal {
    /// The episode being counted
    episode: Uuid,
    /// When the count started: the last commit, or the first sight of the episode
    since: Instant,
    /// The elapsed time the count started from, as committed
    committed_ms: u64,
}

/// Whether the data on this node can be served under the cluster's policy
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataReadiness {
    /// Whether an operator has initialized a placement
    pub initialized: bool,
    /// Whether this node holds tablets under the placement
    pub placed: bool,
    /// How many members are up
    pub members_up: u32,
    /// The replication factor the policy asks for
    pub desired_rf: u32,
    /// The replication factor the placement gives
    pub active_rf: u32,
    /// Whether a default write is admitted, and if not, why
    pub default_writes: Result<(), QuorumShortfall>,
    /// The shards that have failed on this node, by index
    pub shards_failed: Vec<u16>,
    /// What the node's tablet groups look like, folded over every shard
    ///
    /// Empty before any shard reported ([F40](../../../../docs/src/features/replication.md)).
    #[serde(default)]
    pub replication: crate::server::replication::NodeReplication,
}

/// Where this node stands: live, with its group, and with its data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessView {
    /// Whether the process is serving
    pub process: bool,
    /// Where it stands with its group
    pub control: JoinStatus,
    /// The control leader it knows, if any
    pub leader: Option<NodeId>,
    /// Whether it is that leader
    pub is_leader: bool,
    /// How many members vote
    pub voters: usize,
    /// How many members only learn
    pub learners: usize,
    /// Whether its data can be served under the policy
    pub data: DataReadiness,
}

/// What a joiner asks the leader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    /// What the joiner advertises, at its incarnation
    pub member: MemberRecord,
    /// The structural fingerprint of the schema it serves
    pub schema_id: u64,
}

/// What the leader, or a member the joiner reached, answers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinResponse {
    /// Admitted: the cluster, who leads it, and the topology version the admission moved it to
    Admitted {
        /// The cluster the joiner now belongs to
        cluster: ClusterId,
        /// The leader that admitted it
        leader: NodeId,
        /// The topology version after the admission
        topology_version: u64,
    },
    /// Not the leader; ask this one, or nobody if none is known
    Redirect {
        /// The leader's record, if the answering node knows one
        leader: Option<MemberRecord>,
    },
    /// Refused, and why
    Refused {
        /// Why
        reason: String,
        /// Whether asking again later may succeed
        ///
        /// A refusal of the joiner's identity or schema is final; one because the group was
        /// busy with another membership change, or had no quorum for a moment, is not.
        retry: bool,
    },
}

/// What the leader answers a proposal forwarded to it
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProposeResponse {
    /// Applied, with what the state machine produced
    Applied(ControlResponse),
    /// Not the leader; ask this one, or nobody if none is known
    NotLeader {
        /// The leader's record, if known
        leader: Option<MemberRecord>,
    },
}

/// Why a proposal produced no answer
#[derive(Debug, Clone)]
pub enum ProposeError {
    /// No leader could be reached within the deadline
    NoLeader,
    /// The group is stopped or the RPC failed in a way that is not a missing leader
    Failed(String),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::NoLeader => write!(f, "no control leader could be reached"),
            ProposeError::Failed(msg) => write!(f, "{msg}"),
        }
    }
}

/// What the control thread is started with
struct Startup {
    /// Where the store lives
    root: PathBuf,
    /// Who this node is
    identity: Identity,
    /// Where the thread runs
    placement: ControlPlacement,
    /// What this node advertises
    member: MemberRecord,
    /// The policy a bootstrap seeds
    policy: BootstrapPolicy,
    /// Whether the configuration creates the cluster
    bootstrap: bool,
    /// The control addresses a joiner discovers the cluster through
    seeds: Vec<String>,
    /// Where particular members are dialled instead of where they advertise
    dial: BTreeMap<NodeId, DialOverride>,
    /// Where events go
    events: mpsc::Sender<ControlEvent>,
    /// Where requests come from
    requests: kanal::Receiver<ControlRequest>,
    /// The structural fingerprint of the schema this node serves
    schema_id: u64,
    /// The tables it serves, with their stable identities
    tables: Vec<(String, TableId)>,
    /// The largest frame the peer lanes accept
    max_frame_bytes: u32,
    /// The certificate and authority the peer lanes use, if encrypted
    tls: Option<PeerTls>,
    /// Where this node's control listener binds
    bind: SocketAddr,
    /// The bounds and timers the peer lanes use
    transport: Transport,
    /// The migration settings, for the disk reserve a plan respects
    migration: Migration,
    /// The rebalance settings, for the caps and the interval a plan is driven at
    rebalance: Rebalance,
}

/// The control plane, which is only a namespace for `start`
pub struct ControlPlane;

impl ControlPlane {
    /// Start the control thread
    ///
    /// Returns as soon as the thread is spawned; [`ControlHandle::ready`] is what waits for the
    /// group. The member record is built here from the configuration and the identity, so the
    /// thread is handed facts rather than a config to interpret.
    ///
    /// # Arguments
    ///
    /// * `placement` - Where the thread runs
    /// * `identity` - Who this node is
    /// * `conf` - The configuration, which has to carry a `cluster:` block
    /// * `client` - The address clients reach the shards at
    /// * `shards` - How many shards this node runs
    /// * `schema_id` - The structural fingerprint of the schema it serves
    /// * `tables` - The tables it serves, with their stable identities
    ///
    /// # Errors
    ///
    /// Fails if the configuration is standalone or the thread cannot be spawned.
    #[instrument(name = "ControlPlane::start", skip_all, err(Debug))]
    pub fn start(
        placement: ControlPlacement,
        identity: Identity,
        conf: &Conf,
        client: String,
        shards: usize,
        schema_id: u64,
        tables: Vec<(String, TableId)>,
    ) -> Result<ControlHandle, ServerError> {
        let cluster = conf
            .cluster
            .as_ref()
            .ok_or(ServerError::Shoal(ShoalError::NotClustered))?;
        // what this node tells the group about itself
        let advertise = cluster.advertised(&conf.networking.interface)?;
        let member = MemberRecord {
            node: identity.node,
            client: cluster.client_advertise.clone().unwrap_or(client),
            data: format!("{advertise}:{}", cluster.port),
            control: format!("{advertise}:{}", cluster.control_port),
            control_core: placement.cpu,
            control_shared: placement.shared,
            shards,
            incarnation: identity.incarnation,
            weight: cluster.weight.unwrap_or(0),
        };
        // where the control listener binds
        let bind: SocketAddr = format!("{advertise}:{}", cluster.control_port)
            .parse()
            .map_err(|_| {
                ServerError::Shoal(ShoalError::InvalidConfig(format!(
                    "the control listener address {advertise}:{} is not one",
                    cluster.control_port
                )))
            })?;
        let root = conf
            .storage
            .default
            .filesystem
            .latency_sensitive
            .path
            .clone();
        // the two channels: events up to the pool, requests down to the thread
        let (events_tx, events) = mpsc::channel();
        let (requests_tx, requests_rx) = kanal::bounded(64);
        let startup = Startup {
            root,
            identity,
            placement: placement.clone(),
            member,
            policy: cluster.policy(),
            bootstrap: cluster.bootstrap,
            seeds: cluster.seeds.clone(),
            dial: cluster.dial.clone(),
            events: events_tx,
            requests: requests_rx,
            schema_id,
            tables,
            max_frame_bytes: conf.networking.max_frame_bytes,
            tls: cluster.tls.clone(),
            bind,
            transport: cluster.transport.clone(),
            migration: cluster.migration.clone(),
            rebalance: cluster.rebalance.clone(),
        };
        // the thread, pinned to its core, running the group until told to stop
        let thread = LocalExecutorBuilder::new(Placement::Fixed(placement.cpu))
            .name("shoal-control")
            .spawn(move || run(startup))?;
        Ok(ControlHandle {
            requests: requests_tx,
            events,
            thread: Some(thread),
            placement,
            ready: false,
        })
    }
}

/// The pool's handle to the control thread
pub struct ControlHandle {
    /// Where requests go
    requests: kanal::Sender<ControlRequest>,
    /// Where events come from
    events: Receiver<ControlEvent>,
    /// The thread, until it is joined
    thread: Option<glommio::ExecutorJoinHandle<Result<(), ServerError>>>,
    /// Where the thread runs
    placement: ControlPlacement,
    /// Whether the thread has reported ready
    ready: bool,
}

impl ControlHandle {
    /// Where the control thread runs
    pub fn placement(&self) -> &ControlPlacement {
        &self.placement
    }

    /// A sender the shards use to report their health and relay admin calls
    #[must_use]
    pub fn requests(&self) -> kanal::Sender<ControlRequest> {
        self.requests.clone()
    }

    /// Wait until the thread is serving, or report that it failed
    ///
    /// # Arguments
    ///
    /// * `deadline` - When to stop waiting
    ///
    /// # Errors
    ///
    /// Reports a control plane that failed as [`ServerError::ControlFailed`], and one that is
    /// still starting at the deadline the same way.
    pub fn ready(&mut self, deadline: Instant) -> Result<(), ServerError> {
        if self.ready {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        match self.events.recv_timeout(remaining) {
            Ok(ControlEvent::Ready) => {
                self.ready = true;
                Ok(())
            }
            Ok(ControlEvent::Failed(error)) => Err(ServerError::ControlFailed { error }),
            Err(RecvTimeoutError::Timeout) => Err(ServerError::ControlFailed {
                error: format!("not ready after {remaining:?}"),
            }),
            Err(RecvTimeoutError::Disconnected) => Err(ServerError::ControlFailed {
                error: "exited before reporting ready".to_string(),
            }),
        }
    }

    /// Whether the control plane has failed since it was last asked
    pub fn failure(&self) -> Option<String> {
        match self.events.try_recv() {
            Ok(ControlEvent::Failed(error)) => Some(error),
            Ok(ControlEvent::Ready) | Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some("the control thread exited".to_string()),
        }
    }

    /// Ask the thread something and wait for its answer
    ///
    /// # Arguments
    ///
    /// * `build` - Builds the request around the reply channel
    /// * `what` - What was asked, for the error
    fn ask<T>(
        &self,
        build: impl FnOnce(mpsc::Sender<T>) -> ControlRequest,
        what: &str,
    ) -> Result<T, ServerError> {
        let (tx, rx) = mpsc::channel();
        self.requests
            .send(build(tx))
            .map_err(|_| ServerError::ControlFailed {
                error: "the control thread is not answering".to_string(),
            })?;
        rx.recv_timeout(REQUEST_TIMEOUT)
            .map_err(|_| ServerError::ControlFailed {
                error: format!("the control thread did not answer {what}"),
            })
    }

    /// Ping a peer over the control lane and report the round trip
    ///
    /// # Arguments
    ///
    /// * `node` - The peer to ping
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone or the peer did not answer.
    pub fn ping(&self, node: NodeId) -> Result<Duration, ServerError> {
        self.ask(|reply| ControlRequest::Ping { node, reply }, "a ping")?
            .map_err(|error| ServerError::ControlFailed { error })
    }

    /// Send a peer a vote for a low term and report its answer
    ///
    /// # Arguments
    ///
    /// * `node` - The peer to probe
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone or the peer did not answer.
    pub fn vote_probe(&self, node: NodeId) -> Result<VoteProbe, ServerError> {
        self.ask(|reply| ControlRequest::VoteProbe { node, reply }, "a vote probe")?
            .map_err(|error| ServerError::ControlFailed { error })
    }

    /// The cluster as this node sees it
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone.
    pub fn topology(&self) -> Result<TopologyView, ServerError> {
        self.ask(ControlRequest::Topology, "a topology request")
    }

    /// Where this node stands
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone.
    pub fn readiness(&self) -> Result<ReadinessView, ServerError> {
        self.ask(ControlRequest::Readiness, "a readiness request")
    }

    /// The map as it is now
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone.
    pub fn map(&self) -> Result<Arc<TabletMap>, ServerError> {
        self.ask(ControlRequest::Map, "the map")
    }

    /// Hand the thread where new maps go, and wait until the current one has gone there
    ///
    /// # Arguments
    ///
    /// * `sink` - Where every map goes from now on
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone.
    pub fn attach_sink(&self, sink: MapSink) -> Result<(), ServerError> {
        self.ask(|ack| ControlRequest::AttachSink(sink, ack), "a sink attachment")
    }

    /// Make an administrative request as the process itself
    ///
    /// # Arguments
    ///
    /// * `request` - What is asked
    /// * `principal` - Who is asking, if a wire principal is
    /// * `trusted` - Whether the caller is the process itself
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone or did not answer within the proposal deadline.
    pub fn admin(
        &self,
        request: AdminRequest,
        principal: Option<String>,
        trusted: bool,
    ) -> Result<AdminResponse, ServerError> {
        let (reply, rx) = kanal::bounded(1);
        self.requests
            .send(ControlRequest::Admin(AdminCall {
                request,
                principal,
                trusted,
                reply,
            }))
            .map_err(|_| ServerError::ControlFailed {
                error: "the control thread is not answering".to_string(),
            })?;
        rx.recv_timeout(PROPOSE_TIMEOUT + REQUEST_TIMEOUT)
            .map_err(|_| ServerError::ControlFailed {
                error: "the control thread did not answer an admin request".to_string(),
            })
    }

    /// A handle that can make administrative requests as the process, from any thread
    ///
    /// What a benchmark's background thread holds to ask for a repair mid-run
    /// ([F44](../../../../docs/src/features/repair.md)).
    #[must_use]
    pub fn admin_sender(&self) -> AdminSender {
        AdminSender {
            requests: self.requests.clone(),
        }
    }

    /// Send the leader one stale report, for a test
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone or the report could not be sent.
    pub fn stale_report(&self) -> Result<(), ServerError> {
        self.ask(ControlRequest::StaleReport, "a stale report")?
            .map_err(|error| ServerError::ControlFailed { error })
    }

    /// Stop the group and join the thread
    ///
    /// # Errors
    ///
    /// Returns whatever the thread returned.
    #[instrument(name = "ControlHandle::shutdown", skip_all, err(Debug))]
    pub fn shutdown(mut self) -> Result<(), ServerError> {
        // a thread that is already gone cannot be told anything
        let _ = self.requests.send(ControlRequest::Shutdown);
        match self.thread.take() {
            Some(thread) => thread.join()?,
            None => Ok(()),
        }
    }
}

/// Everything that can happen to the control loop
enum Event {
    /// The pool asked something
    Pool(ControlRequest),
    /// A peer sent a membership RPC
    Rpc(Inbound),
    /// The detector's report interval elapsed
    ReportTick,
    /// The ping interval elapsed
    PingTick,
    /// The group's metrics changed
    Metrics(Box<RaftMetrics<ControlConfig>>),
    /// The state machine applied more of the log
    Applied,
    /// A joiner's admission finished
    Joined(Result<(ClusterId, NodeId), String>),
    /// This node's own observation finished
    Observed(Result<ControlResponse, ProposeError>),
    /// A promotion finished
    Promoted(NodeId, Result<(), String>),
    /// An admission finished, and the next queued joiner may be admitted
    Admitted,
    /// A report was answered, or refused
    Reported(Result<Vec<u8>, RpcFailure>),
    /// A ping was answered, or not
    Pinged(NodeId, Result<Duration, ()>),
    /// A health verdict this leader proposed finished
    HealthProposed(NodeId, Result<ControlResponse, ProposeError>),
    /// A grace count this leader proposed finished
    GraceProposed(NodeId, Result<ControlResponse, ProposeError>),
    /// A plan's progress this leader proposed finished
    PlanProposed(Uuid, Result<ControlResponse, ProposeError>),
    /// A drained member's removal from the control group finished, or not
    Finished(Uuid, NodeId, Result<(), String>),
}

/// What one ping learned about a member, this node's local view
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reachability {
    /// The last round trip, in microseconds
    pub rtt_us: u32,
    /// Consecutive pings that got no answer
    pub misses: u32,
}

/// What the loop owns
///
/// Never borrowed across an await: every handler that awaits is a task spawned with clones of
/// the handles here, posting what it learned back as an [`Event`].
struct Core {
    /// This node
    node: NodeId,
    /// Where the store lives
    root: PathBuf,
    /// Where the thread runs
    placement: ControlPlacement,
    /// What this node advertises
    member: MemberRecord,
    /// The policy the configuration would seed
    policy: BootstrapPolicy,
    /// The tables this node serves
    tables: Vec<(String, TableId)>,
    /// The control addresses a joiner discovers the cluster through
    seeds: Vec<String>,
    /// The bounds and timers
    transport: Transport,
    /// The group
    raft: Raft<ControlConfig, ControlStateMachine>,
    /// The state machine
    machine: ControlStateMachine,
    /// The control network
    network: PeerNetwork,
    /// What this node says about itself
    local: Rc<RefCell<Local>>,
    /// Where events to the pool go
    events: mpsc::Sender<ControlEvent>,
    /// Where events to this loop go
    tx: kanal::AsyncSender<Event>,
    /// The map as last built
    map: Arc<TabletMap>,
    /// Where new maps go
    sink: Option<MapSink>,
    /// The leader the metrics last named
    leader: Option<NodeId>,
    /// Whether this node is the leader
    is_leader: bool,
    /// The last metrics seen
    metrics: Option<Box<RaftMetrics<ControlConfig>>>,
    /// Where this node stands with its group
    status: JoinStatus,
    /// Whether this node's own observation is in flight
    observing: bool,
    /// When the next observation may start, after one failed
    observe_after: Option<Instant>,
    /// Whether a promotion is in flight
    promoting: bool,
    /// Whether a join is in flight
    joining: bool,
    /// Whether an admission is in flight, since the group takes one membership change at a time
    admitting: bool,
    /// The joiners waiting to be admitted, in the order they asked
    join_queue: std::collections::VecDeque<(Inbound, JoinRequest)>,
    /// The shards that have failed on this node
    shards_failed: Vec<u16>,
    /// The next report's sequence
    report_seq: u64,
    /// What this node's own pings learned
    reachability: BTreeMap<NodeId, Reachability>,
    /// The shard health last proposed, so a change is proposed once
    reported_shards: Vec<u16>,
    /// The quarantined copies across this node's shards, as they last reported
    quarantined: Vec<QuarantinedCopy>,
    /// The quarantined copies last proposed, so a change is proposed once
    reported_quarantine: Vec<QuarantinedCopy>,
    /// The failure detector, which only a leader feeds
    detector: Detector,
    /// The members whose health this leader is proposing, so one verdict is in flight per member
    health_in_flight: BTreeSet<NodeId>,
    /// What every shard last reported about its tablet groups, by shard
    replication: BTreeMap<usize, crate::server::replication::ShardReplication>,
    /// The migration settings, for the disk reserve a plan respects
    migration: Migration,
    /// The rebalance settings, for the caps and the interval a plan is driven at
    rebalance: Rebalance,
    /// What every member last reported about its capacity, as this leader heard it
    capacity: BTreeMap<NodeId, NodeCapacity>,
    /// What this leader has counted of each down member's grace since it last committed it
    grace_seen: BTreeMap<NodeId, GraceLocal>,
    /// The members whose grace this leader is committing, so one count is in flight per member
    grace_in_flight: BTreeSet<NodeId>,
    /// The plans whose progress this leader is committing, one proposal in flight per plan
    plan_in_flight: BTreeSet<Uuid>,
    /// The plans whose member this leader is taking out of the control group
    finishing: BTreeSet<Uuid>,
    /// When the plans were last looked at
    last_plan: Option<Instant>,
    /// The topology version the plans were last looked at against
    planned_at: u64,
    /// How many reports have changed the capacity table since the plans were looked at
    capacity_moved: bool,
}

impl Core {
    /// The topology as this node sees it
    fn topology(&self) -> TopologyView {
        let state = self.machine.state();
        let desired_rf = state.desired_rf();
        let active_rf = state.active_rf();
        let grace_ms = state
            .policy
            .as_ref()
            .and_then(|policy| policy.auto_remove_after)
            .map(|grace| u64::try_from(grace.duration().as_millis()).unwrap_or(u64::MAX));
        let members = state
            .members
            .values()
            .map(|member| {
                let capacity = self.capacity.get(&member.record.node);
                MemberView {
                    state_name: member.state_name().to_string(),
                    weight: member.record.effective_weight(),
                    grace_remaining_ms: match (&member.grace, grace_ms) {
                        (Some(grace), Some(total)) if !grace.expired => Some(total.saturating_sub(grace.elapsed_ms)),
                        _ => None,
                    },
                    free_bytes: capacity.map(|capacity| capacity.free_bytes),
                    held_bytes: capacity.map(|capacity| capacity.group_bytes.values().sum()),
                    state: member.clone(),
                }
            })
            .collect();
        TopologyView {
            cluster: state
                .cluster
                .or(self.local.borrow().cluster)
                .unwrap_or_default(),
            node: self.node,
            incarnation: self.member.incarnation,
            control: self.status,
            leader: self.leader,
            version: state.topology_version,
            members,
            voters: state.voters(),
            learners: state.learners(),
            joint: state.joint,
            initialized: state.initialized.clone(),
            desired_rf,
            active_rf,
            missing_replicas: desired_rf.saturating_sub(active_rf),
            up_members: state.up_members(),
            control_core: self.placement.cpu,
            control_shared: self.placement.shared,
            plans: state.open_plans().into_iter().cloned().collect(),
            tombstones: state.tombstones.clone(),
            under_replicated_sets: state.under_replicated_sets(),
            auto_remove_after_ms: grace_ms,
            policy: state.policy,
        }
    }

    /// Where this node stands
    fn readiness(&self) -> ReadinessView {
        let map = &self.map;
        ReadinessView {
            process: true,
            control: self.status,
            leader: self.leader,
            is_leader: self.is_leader,
            voters: map
                .members
                .values()
                .filter(|member| member.role == MemberRole::Voter)
                .count(),
            learners: map
                .members
                .values()
                .filter(|member| member.role == MemberRole::Learner)
                .count(),
            data: DataReadiness {
                initialized: self.machine.state().initialized.is_some(),
                placed: map.places(self.node),
                members_up: map.up(),
                desired_rf: map.desired_rf,
                active_rf: map.active_rf(),
                default_writes: if map.places(self.node) {
                    map.write_admission()
                } else {
                    Err(QuorumShortfall {
                        have: map.up(),
                        need: map.quorum_for(map.write_consistency),
                    })
                },
                shards_failed: self.shards_failed.clone(),
                replication: crate::server::replication::NodeReplication::fold(
                    self.replication.values().cloned().collect(),
                ),
            },
        }
    }

    /// Rebuild the map from the applied state and push it if its version moved
    fn publish(&mut self) {
        let state = self.machine.state();
        let map = TabletMap::from_state(&state, self.leader, &self.tables);
        // the leader is not part of the version, so a leader change alone still goes out
        let moved = map.version != self.map.version || map.leader != self.map.leader;
        if !moved {
            return;
        }
        let map = Arc::new(map);
        self.map = map.clone();
        if let Some(sink) = &mut self.sink {
            sink(map);
        }
    }

    /// The committed record of a member, for dialling it
    fn record_of(&self, node: NodeId) -> Option<MemberRecord> {
        self.machine
            .state()
            .members
            .get(&node)
            .map(|member| member.record.clone())
    }

    /// The leader's committed record, if one is known
    fn leader_record(&self) -> Option<MemberRecord> {
        self.leader.and_then(|leader| self.record_of(leader))
    }

    /// Fail the thread: tell the pool, and stop
    fn fail(&self, error: &ServerError) {
        let _ = self.events.send(ControlEvent::Failed(format!("{error}")));
    }
}

/// Everything the thread does, from open to shutdown
///
/// A failure anywhere is reported as [`ControlEvent::Failed`] and returned; the pool sees both,
/// one through `ready` or `failure` and one through `exit`.
///
/// # Arguments
///
/// * `startup` - What the thread was started with
async fn run(startup: Startup) -> Result<(), ServerError> {
    let events = startup.events.clone();
    match serve(startup).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = events.send(ControlEvent::Failed(format!("{error}")));
            Err(error)
        }
    }
}

/// Open the store, run the group, and answer events until shutdown
///
/// # Arguments
///
/// * `startup` - What the thread was started with
#[allow(clippy::too_many_lines)]
async fn serve(startup: Startup) -> Result<(), ServerError> {
    let Startup {
        root,
        identity,
        placement,
        member,
        policy,
        bootstrap,
        seeds,
        dial,
        events,
        requests,
        schema_id,
        tables,
        max_frame_bytes,
        tls,
        bind,
        transport,
        migration,
        rebalance,
    } = startup;
    let node = identity.node;
    // the store, recovered from whatever the directory holds
    let dir = root.join(CONTROL_DIR);
    let (log, machine) = store::open(&dir).await?;
    // the cluster: the marker's, or none for a joiner that has not adopted one
    let recovered = machine.state();
    let cluster_name = identity
        .cluster
        .or(recovered.cluster)
        .map_or_else(|| "joining".to_string(), |cluster| cluster.to_string());
    // a node that was leading when it stopped comes back as a follower and stands for election
    // like anybody else: openraft would otherwise restore it as the leader of its old term, with
    // no lease until a quorum answers, and a directory copied from a leader would come back as a
    // second leader of that term. A member whose peers are all gone then writes nothing to its
    // log until a majority elects somebody, which is what C1's restart rule asks for
    let config = Config {
        cluster_name,
        enable_leader_restore: Some(false),
        ..Config::default()
    }
    .validate()
    .map_err(|error| ServerError::ControlFailed {
        error: format!("openraft config: {error}"),
    })?;
    // what this node says about itself on the control lane, and the rustls configs it uses
    let local = Rc::new(RefCell::new(Local::new(
        &identity,
        member.shards,
        schema_id,
        max_frame_bytes,
    )));
    let (client_tls, server_tls) = match &tls {
        Some(tls) => {
            // a control node that asked for TLS refuses to start if the kernel cannot do kTLS,
            // the same as a shard listener
            if !crate::shared::tls::ktls::is_available() {
                return Err(crate::shared::tls::TlsError::UlpUnavailable(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "the 'tls' kernel module is not loaded",
                ))
                .into());
            }
            (
                Some(crate::shared::tls::peer_client_config(tls)?),
                Some(crate::shared::tls::peer_server_config(tls)?),
            )
        }
        None => (None, None),
    };
    // the network the group drives its peers with, dialling committed records
    let network = PeerNetwork::new(local.clone(), dial, client_tls, transport.clone());
    let raft = Raft::<ControlConfig, ControlStateMachine>::new(
        node,
        Arc::new(config),
        network.clone(),
        log,
        machine.clone(),
    )
    .await
    .map_err(|error| ServerError::ControlFailed {
        error: format!("starting the group: {error}"),
    })?;
    // the one channel every event arrives on
    let (tx, rx) = kanal::unbounded_async::<Event>();
    // a fresh bootstrap creates its group and its cluster before anything else can see it
    let initialized = raft.is_initialized().await.map_err(|error| ServerError::ControlFailed {
        error: format!("{error}"),
    })?;
    let fresh_bootstrap = bootstrap && !initialized && recovered.cluster.is_none();
    if fresh_bootstrap {
        let mut members = BTreeMap::new();
        members.insert(node, member.clone());
        raft.initialize(members)
            .await
            .map_err(|error| ServerError::ControlFailed {
                error: format!("initializing the group: {error}"),
            })?;
        // a group of one elects itself; wait for it
        raft.wait(Some(LEADER_TIMEOUT))
            .current_leader(node, "the control group elects this node")
            .await
            .map_err(|error| ServerError::ControlFailed {
                error: format!("{error}"),
            })?;
        let cluster = identity
            .cluster
            .ok_or(ServerError::Shoal(ShoalError::NotClustered))?;
        let response = raft
            .client_write(ControlCommand::Bootstrap {
                cluster,
                policy: policy.clone(),
                member: member.clone(),
            })
            .await
            .map_err(|error| ServerError::ControlFailed {
                error: format!("writing the bootstrap: {error}"),
            })?;
        match response.data {
            ControlResponse::Applied { topology_version } => observe(&root, topology_version).await?,
            other => {
                return Err(ServerError::ControlFailed {
                    error: format!("the control group refused the bootstrap: {other:?}"),
                })
            }
        }
    } else if let (Some(marker), Some(committed)) = (identity.cluster, recovered.cluster) {
        // a member whose directory and log disagree about its cluster is a directory copied
        // between clusters
        if marker != committed {
            return Err(ServerError::Shoal(ShoalError::WrongCluster {
                found: committed,
                expected: Some(marker),
            }));
        }
    }
    // record what recovery found, before anything new is written
    let recovered = machine.state();
    if recovered.topology_version > 0 {
        observe(&root, recovered.topology_version).await?;
    }
    // the store tells the loop about every apply from here on
    {
        let hook_tx = tx.clone_sync();
        machine.on_applied(Rc::new(move |_index| {
            let _ = hook_tx.try_send(Event::Applied);
        }));
    }
    // bind the control listener and drive inbound RPCs into this node's group, on this executor
    let listener = crate::server::peer::bind_reusable(bind).map_err(|error| ServerError::ControlFailed {
        error: format!("binding the control listener on {bind}: {error}"),
    })?;
    let (inbound_tx, inbound_rx) = kanal::unbounded_async::<Inbound>();
    let acceptor = glommio::spawn_local(control_acceptor(
        listener,
        raft.clone(),
        machine.clone(),
        local.clone(),
        server_tls,
        inbound_tx,
    ));
    // the relays: the pool's requests, the listener's RPCs, the metrics and the two timers
    let pool_relay = {
        let tx = tx.clone();
        let requests = requests.to_async();
        glommio::spawn_local(async move {
            while let Ok(request) = requests.recv().await {
                if tx.send(Event::Pool(request)).await.is_err() {
                    break;
                }
            }
            // the pool dropped its handle, which is a shutdown
            let _ = tx.send(Event::Pool(ControlRequest::Shutdown)).await;
        })
    };
    let rpc_relay = {
        let tx = tx.clone();
        glommio::spawn_local(async move {
            while let Ok(inbound) = inbound_rx.recv().await {
                if tx.send(Event::Rpc(inbound)).await.is_err() {
                    break;
                }
            }
        })
    };
    let metrics_task = {
        let tx = tx.clone();
        let mut metrics = raft.metrics();
        glommio::spawn_local(async move {
            loop {
                let current = metrics.borrow_watched().clone();
                if tx.send(Event::Metrics(Box::new(current))).await.is_err() {
                    break;
                }
                if metrics.changed().await.is_err() {
                    break;
                }
            }
        })
    };
    let report_timer = {
        let tx = tx.clone();
        let interval = Duration::from_millis(policy.failure_detector.interval_ms.max(10));
        glommio::spawn_local(async move {
            loop {
                glommio::timer::sleep(interval).await;
                if tx.send(Event::ReportTick).await.is_err() {
                    break;
                }
            }
        })
    };
    let ping_timer = {
        let tx = tx.clone();
        let interval = transport.ping_interval.duration().max(Duration::from_millis(10));
        glommio::spawn_local(async move {
            loop {
                glommio::timer::sleep(interval).await;
                if tx.send(Event::PingTick).await.is_err() {
                    break;
                }
            }
        })
    };
    // where this node stands to begin with
    let status = if identity.mode == MarkerMode::Joining {
        JoinStatus::Joining
    } else {
        JoinStatus::Recovering
    };
    let mut core = Core {
        node,
        root: root.clone(),
        placement: placement.clone(),
        member: member.clone(),
        policy: policy.clone(),
        tables,
        seeds,
        transport,
        raft: raft.clone(),
        machine: machine.clone(),
        network: network.clone(),
        local: local.clone(),
        events: events.clone(),
        tx: tx.clone(),
        map: Arc::new(TabletMap::default()),
        sink: None,
        leader: None,
        is_leader: false,
        metrics: None,
        status,
        observing: false,
        observe_after: None,
        promoting: false,
        joining: false,
        admitting: false,
        join_queue: std::collections::VecDeque::new(),
        shards_failed: Vec::new(),
        report_seq: 0,
        reachability: BTreeMap::new(),
        reported_shards: Vec::new(),
        quarantined: Vec::new(),
        reported_quarantine: Vec::new(),
        detector: Detector::new(&policy.failure_detector),
        health_in_flight: BTreeSet::new(),
        replication: BTreeMap::new(),
        migration,
        rebalance,
        capacity: BTreeMap::new(),
        grace_seen: BTreeMap::new(),
        grace_in_flight: BTreeSet::new(),
        plan_in_flight: BTreeSet::new(),
        finishing: BTreeSet::new(),
        last_plan: None,
        planned_at: 0,
        capacity_moved: false,
    };
    core.publish();
    event!(
        Level::INFO,
        msg = "Control plane ready",
        node = node.to_string(),
        cluster = identity.cluster.map(|cluster| cluster.to_string()),
        status = status.name(),
        control_core = placement.cpu,
        control_shared = placement.shared,
        topology_version = machine.state().topology_version,
    );
    let _ = events.send(ControlEvent::Ready);
    // a joiner starts dialling its seeds now
    if status == JoinStatus::Joining {
        core.start_join();
    }
    // then answer events until the pool says stop
    let outcome = loop {
        let Ok(event) = rx.recv().await else {
            break Ok(());
        };
        match core.handle(event).await {
            Ok(true) => {}
            Ok(false) => break Ok(()),
            Err(error) => {
                core.fail(&error);
                break Err(error);
            }
        }
    };
    // stop answering peers before the group goes away
    acceptor.cancel().await;
    pool_relay.cancel().await;
    rpc_relay.cancel().await;
    metrics_task.cancel().await;
    report_timer.cancel().await;
    ping_timer.cancel().await;
    raft.shutdown().await.map_err(|error| ServerError::ControlFailed {
        error: format!("stopping the group: {error}"),
    })?;
    outcome
}

impl Core {
    /// Handle one event; `Ok(false)` means shut down
    ///
    /// # Arguments
    ///
    /// * `event` - What happened
    async fn handle(&mut self, event: Event) -> Result<bool, ServerError> {
        match event {
            Event::Pool(request) => return self.handle_request(request),
            Event::Rpc(inbound) => self.handle_rpc(inbound),
            Event::Applied => self.handle_applied()?,
            Event::Metrics(metrics) => self.handle_metrics(metrics),
            Event::Joined(outcome) => self.handle_joined(outcome).await?,
            Event::Observed(outcome) => self.handle_observed(outcome)?,
            Event::Promoted(learner, outcome) => {
                self.promoting = false;
                match outcome {
                    Ok(()) => event!(Level::INFO, msg = "promoted a learner to voter", node = %learner),
                    Err(error) => event!(Level::WARN, msg = "a promotion failed", node = %learner, error),
                }
                self.maybe_promote();
            }
            Event::Admitted => {
                self.admitting = false;
                self.drain_joins();
            }
            Event::ReportTick => {
                // the tick is also when anything that failed for want of a leader is tried
                // again: this node's own observation, a promotion, a queued admission
                self.maybe_observe();
                self.maybe_promote();
                self.drain_joins();
                self.report();
                self.judge_members();
                self.accrue_graces();
                self.drive_plans(false);
            }
            Event::PingTick => self.ping_members(),
            Event::Reported(outcome) => self.handle_reported(outcome)?,
            Event::Pinged(node, answered) => self.handle_pinged(node, answered),
            Event::HealthProposed(node, outcome) => self.handle_health_proposed(node, outcome),
            Event::GraceProposed(node, outcome) => self.handle_grace_proposed(node, outcome),
            Event::PlanProposed(op, outcome) => self.handle_plan_proposed(op, outcome),
            Event::Finished(op, node, outcome) => self.handle_finished(op, node, outcome),
        }
        Ok(true)
    }

    /// Answer the pool
    ///
    /// # Arguments
    ///
    /// * `request` - What it asked
    fn handle_request(&mut self, request: ControlRequest) -> Result<bool, ServerError> {
        match request {
            ControlRequest::Topology(reply) => {
                let _ = reply.send(self.topology());
            }
            ControlRequest::Readiness(reply) => {
                let _ = reply.send(self.readiness());
            }
            ControlRequest::Map(reply) => {
                let _ = reply.send(self.map.clone());
            }
            ControlRequest::AttachSink(mut sink, ack) => {
                sink(self.map.clone());
                self.sink = Some(sink);
                let _ = ack.send(());
            }
            ControlRequest::Ping { node, reply } => {
                let Some(record) = self.record_of(node) else {
                    let _ = reply.send(Err(format!("{node} is not a member this node knows")));
                    return Ok(true);
                };
                let network = self.network.clone();
                glommio::spawn_local(async move {
                    let mut peer = network.peer(&network.addr_of(&record));
                    let started = Instant::now();
                    let outcome = peer.ping().await.map(|_| started.elapsed());
                    let _ = reply.send(outcome);
                })
                .detach();
            }
            ControlRequest::VoteProbe { node, reply } => {
                let Some(record) = self.record_of(node) else {
                    let _ = reply.send(Err(format!("{node} is not a member this node knows")));
                    return Ok(true);
                };
                let network = self.network.clone();
                let me = self.node;
                glommio::spawn_local(async move {
                    let mut peer = network.peer(&network.addr_of(&record));
                    // a vote for term 1 from this node: a peer that holds a term at least this
                    // high and has voted does not grant it, and answering at all proves its
                    // Raft was reached over the control lane
                    let vote = openraft::vote::Vote::new(1, me);
                    let request = VoteRequest::<ControlConfig>::new(vote, None);
                    let option = openraft::network::RPCOption::new(Duration::from_secs(5));
                    let outcome = match RaftNetworkV2::vote(&mut peer, request, option).await {
                        Ok(response) => Ok(VoteProbe {
                            granted: response.vote_granted,
                        }),
                        Err(error) => Err(format!("{error}")),
                    };
                    let _ = reply.send(outcome);
                })
                .detach();
            }
            ControlRequest::Admin(call) => self.handle_admin(call),
            ControlRequest::Replication(report) => {
                // the newest report per shard is what readiness folds
                self.replication.insert(report.shard, report);
                // the quarantined copies across every shard, for the next report
                self.quarantined = self.quarantined_copies();
            }
            ControlRequest::Propose { command, reply } => {
                // a node's own proposal goes through whoever leads
                let raft = self.raft.clone();
                let network = self.network.clone();
                let machine = self.machine.clone();
                glommio::spawn_local(async move {
                    let outcome = propose(&raft, &network, &machine, command).await.map_err(|error| format!("{error:?}"));
                    let _ = reply.send(outcome);
                })
                .detach();
            }
            ControlRequest::ShardHealth(health) => {
                // a shard runs fewer than a u16 holds
                #[allow(clippy::cast_possible_truncation)]
                let shard = health.shard as u16;
                if !self.shards_failed.contains(&shard) {
                    self.shards_failed.push(shard);
                    self.shards_failed.sort_unstable();
                }
                event!(Level::WARN, msg = "a shard died", shard = health.shard, error = health.error);
                // say so at once rather than on the next tick
                self.report();
            }
            ControlRequest::StaleReport(reply) => {
                // a report behind the last one, as a replay or a reordered delivery would be
                let _ = reply.send(self.send_report(self.member.incarnation, Some(self.report_seq.saturating_sub(1))));
            }
            ControlRequest::Shutdown => return Ok(false),
        }
        Ok(true)
    }

    /// Answer a membership RPC a peer sent
    ///
    /// # Arguments
    ///
    /// * `inbound` - The RPC and where its answer goes
    fn handle_rpc(&mut self, inbound: Inbound) {
        match inbound.kind {
            ControlKind::Join => self.handle_join(inbound),
            ControlKind::Propose => self.handle_propose(inbound),
            ControlKind::StatusReport => self.handle_report(inbound),
            _ => {
                let _ = inbound
                    .reply
                    .send(err(format!("{} is not a membership rpc", inbound.kind.name())));
            }
        }
    }

    /// Admit a joiner, or send it to the leader
    ///
    /// # Arguments
    ///
    /// * `inbound` - The join request and where its answer goes
    fn handle_join(&mut self, inbound: Inbound) {
        let request: JoinRequest = match serde_json::from_slice(&inbound.payload) {
            Ok(request) => request,
            Err(error) => {
                let _ = inbound.reply.send(err(format!("decoding a join: {error}")));
                return;
            }
        };
        // only the leader admits; anybody else says who does
        if !self.is_leader {
            let _ = inbound.reply.send(ok(&JoinResponse::Redirect {
                leader: self.leader_record(),
            }));
            return;
        }
        // built from the same schema, or never
        if request.schema_id != self.local.borrow().schema_id {
            let _ = inbound.reply.send(ok(&JoinResponse::Refused {
                reason: format!(
                    "the joiner was built from schema {:#018x} and this cluster serves {:#018x}",
                    request.schema_id,
                    self.local.borrow().schema_id
                ),
                retry: false,
            }));
            return;
        }
        // the group takes one membership change at a time, so admissions queue
        self.join_queue.push_back((inbound, request));
        self.drain_joins();
    }

    /// Admit the next queued joiner, if none is being admitted
    fn drain_joins(&mut self) {
        if self.admitting {
            return;
        }
        let Some((inbound, request)) = self.join_queue.pop_front() else {
            return;
        };
        // the leader may have changed while this joiner waited
        if !self.is_leader {
            let _ = inbound.reply.send(ok(&JoinResponse::Redirect {
                leader: self.leader_record(),
            }));
            self.drain_joins();
            return;
        }
        let state = self.machine.state();
        let Some(cluster) = state.cluster else {
            let _ = inbound.reply.send(ok(&JoinResponse::Refused {
                reason: "this node has no cluster to admit a joiner to".to_string(),
                retry: true,
            }));
            self.drain_joins();
            return;
        };
        // a removed identity never comes back, by this door or any other: a replacement
        // joins as a new identity ([F46](../../../../docs/src/features/capacity-rebalancing.md))
        let removed = state.tombstones.contains_key(&request.member.node)
            || state.members.get(&request.member.node).is_some_and(|existing| existing.phase == MemberPhase::Removed);
        if removed {
            let _ = inbound.reply.send(ok(&JoinResponse::Refused {
                reason: format!(
                    "removed identity: {} was removed from this cluster and cannot rejoin; a replacement joins as a new identity",
                    request.member.node
                ),
                retry: false,
            }));
            self.drain_joins();
            return;
        }
        // the fencing rule, before the group is told anything: a run the cluster has replaced,
        // or a second run of the same copy, is refused as a duplicate identity
        if let Some(existing) = state.members.get(&request.member.node) {
            let committed = existing.record.incarnation;
            let offered = request.member.incarnation;
            let same_run = offered == committed && existing.record.control == request.member.control;
            if offered < committed || (offered == committed && !same_run) {
                let _ = inbound.reply.send(ok(&JoinResponse::Refused {
                    reason: format!(
                        "duplicate identity: {} is already a member at incarnation {committed} \
                         and this joiner offers {offered}",
                        request.member.node
                    ),
                    retry: false,
                }));
                self.drain_joins();
                return;
            }
        }
        // add it as a learner, then commit its admission, then answer; the next joiner waits
        self.admitting = true;
        let raft = self.raft.clone();
        let network = self.network.clone();
        let machine = self.machine.clone();
        let tx = self.tx.clone();
        let me = self.node;
        let record = request.member;
        let reply = inbound.reply;
        glommio::spawn_local(async move {
            // a promotion may hold the group's one membership change; wait it out briefly
            let mut added = Err(String::new());
            for _ in 0..50 {
                match raft.add_learner(record.node, record.clone(), false).await {
                    Ok(_) => {
                        added = Ok(());
                        break;
                    }
                    Err(error) => {
                        added = Err(format!("adding the learner: {error}"));
                        glommio::timer::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            let answer = match added {
                Err(reason) => JoinResponse::Refused { reason, retry: true },
                Ok(()) => match propose(&raft, &network, &machine, ControlCommand::Admit(record)).await {
                    Ok(ControlResponse::Applied { topology_version }) => JoinResponse::Admitted {
                        cluster,
                        leader: me,
                        topology_version,
                    },
                    Ok(ControlResponse::Fenced { committed, offered, .. }) => JoinResponse::Refused {
                        reason: format!(
                            "duplicate identity: the cluster holds incarnation {committed} and this \
                             joiner offers {offered}"
                        ),
                        retry: false,
                    },
                    Ok(ControlResponse::Refused { reason }) => JoinResponse::Refused { reason, retry: false },
                    Ok(ControlResponse::Removed { node }) => JoinResponse::Refused {
                        reason: format!("removed identity: {node} was removed from this cluster and cannot rejoin"),
                        retry: false,
                    },
                    Ok(ControlResponse::Repeated { .. }) => JoinResponse::Refused {
                        reason: "an admission is not an operation".to_string(),
                        retry: false,
                    },
                    Err(error) => JoinResponse::Refused {
                        reason: format!("committing the admission: {error}"),
                        retry: true,
                    },
                },
            };
            let _ = reply.send(ok(&answer));
            let _ = tx.send(Event::Admitted).await;
        })
        .detach();
    }

    /// Commit a command a member proposed through this node, if it leads
    ///
    /// # Arguments
    ///
    /// * `inbound` - The proposal and where its answer goes
    fn handle_propose(&mut self, inbound: Inbound) {
        let command: ControlCommand = match serde_json::from_slice(&inbound.payload) {
            Ok(command) => command,
            Err(error) => {
                let _ = inbound.reply.send(err(format!("decoding a proposal: {error}")));
                return;
            }
        };
        // a bootstrap is never proposed through anybody
        if matches!(command, ControlCommand::Bootstrap { .. }) {
            let _ = inbound.reply.send(err("a bootstrap cannot be proposed".to_string()));
            return;
        }
        if !self.is_leader {
            let _ = inbound.reply.send(ok(&ProposeResponse::NotLeader {
                leader: self.leader_record(),
            }));
            return;
        }
        let raft = self.raft.clone();
        let me = self.node;
        let reply = inbound.reply;
        glommio::spawn_local(async move {
            // through this node's own group, waiting out a lease that is still being established
            let written = glommio::timer::timeout(PROPOSE_TIMEOUT, async {
                Ok(write_here(&raft, me, command).await)
            })
            .await;
            let answer = match written {
                Ok(Ok(response)) => ok(&ProposeResponse::Applied(response)),
                Ok(Err(LocalWrite::Forward(leader))) => ok(&ProposeResponse::NotLeader { leader }),
                Ok(Err(LocalWrite::Failed(msg))) => err(format!("writing the proposal: {msg}")),
                Err(_) => err("the proposal did not commit within the deadline".to_string()),
            };
            let _ = reply.send(answer);
        })
        .detach();
    }

    /// Take a member's status report, if this node leads
    ///
    /// # Arguments
    ///
    /// * `inbound` - The report and where its answer goes
    fn handle_report(&mut self, inbound: Inbound) {
        let report: StatusReport = match serde_json::from_slice(&inbound.payload) {
            Ok(report) => report,
            Err(error) => {
                let _ = inbound.reply.send(err(format!("decoding a report: {error}")));
                return;
            }
        };
        if !self.is_leader {
            let _ = inbound.reply.send(ok(&ProposeResponse::NotLeader {
                leader: self.leader_record(),
            }));
            return;
        }
        // a report about a run the cluster has replaced is answered as fenced, so the run stops
        let state = self.machine.state();
        if let Some(member) = state.members.get(&report.node) {
            // a report from a removed identity is answered as such, so the run stops
            if member.phase == MemberPhase::Removed || state.tombstones.contains_key(&report.node) {
                let _ = inbound.reply.send(err(format!(
                    "removed: {} was removed from this cluster and its reports are not taken",
                    report.node
                )));
                return;
            }
            if report.incarnation < member.record.incarnation {
                let _ = inbound.reply.send(err(format!(
                    "fenced: the cluster holds incarnation {} of {} and this report is from {}",
                    member.record.incarnation, report.node, report.incarnation
                )));
                return;
            }
            // a replayed or reordered report is counted and changes nothing
            if !self
                .detector
                .observe(report.node, report.incarnation, report.seq, Instant::now())
            {
                let _ = inbound.reply.send(ok(&serde_json::json!({ "seq": report.seq, "stale": true })));
                return;
            }
            // a fresh report from a member the cluster holds down is the evidence it is back
            if member.health == MemberHealth::Down {
                self.propose_health(report.node, MemberHealth::Up, member.record.incarnation, None);
            }
            // what it says about its capacity is kept in memory for the planner, never committed
            // ([F46](../../../../docs/src/features/capacity-rebalancing.md))
            self.note_capacity(report.node, report.incarnation, report.free_bytes, &report.group_bytes);
            // a change in the member's quarantined copies is committed, so every node routes
            // around them ([F44](../../../../docs/src/features/repair.md))
            let copies: Vec<QuarantinedCopy> = report.quarantined.iter().map(QuarantinedCopy::from_member).collect();
            if member.quarantined != copies {
                let raft = self.raft.clone();
                let network = self.network.clone();
                let machine = self.machine.clone();
                let command = ControlCommand::ReportQuarantine {
                    node: report.node,
                    incarnation: report.incarnation,
                    copies,
                };
                glommio::spawn_local(async move {
                    let _ = propose(&raft, &network, &machine, command).await;
                })
                .detach();
            }
            // a change in shard health is committed, so every node sees it
            if member.shards_failed != report.shards_failed {
                let raft = self.raft.clone();
                let network = self.network.clone();
                let machine = self.machine.clone();
                let command = ControlCommand::ReportShards {
                    node: report.node,
                    incarnation: report.incarnation,
                    failed: report.shards_failed.clone(),
                };
                glommio::spawn_local(async move {
                    let _ = propose(&raft, &network, &machine, command).await;
                })
                .detach();
            }
        }
        let _ = inbound.reply.send(ok(&serde_json::json!({ "seq": report.seq })));
    }

    /// Answer an administrative request
    ///
    /// # Arguments
    ///
    /// * `call` - The request, who made it, and where the answer goes
    fn handle_admin(&mut self, call: AdminCall) {
        let state = self.machine.state();
        let version = state.topology_version;
        let answer = |outcome: Result<AdminOutcome, AdminError>| AdminResponse {
            node: self.node,
            topology_version: version,
            outcome,
        };
        // the reads answer from the applied state
        let mutation = match &call.request.kind {
            AdminKind::Members => {
                let _ = call.reply.send(answer(Ok(AdminOutcome::Read(
                    serde_json::to_value(self.topology()).unwrap_or_default(),
                ))));
                return;
            }
            AdminKind::Readiness => {
                let _ = call.reply.send(answer(Ok(AdminOutcome::Read(
                    serde_json::to_value(self.readiness()).unwrap_or_default(),
                ))));
                return;
            }
            AdminKind::Replication => {
                let folded = crate::server::replication::NodeReplication::fold(
                    self.replication.values().cloned().collect(),
                );
                let _ = call.reply.send(answer(Ok(AdminOutcome::Read(
                    serde_json::to_value(folded).unwrap_or_default(),
                ))));
                return;
            }
            AdminKind::Detector => {
                let _ = call.reply.send(answer(Ok(AdminOutcome::Read(serde_json::json!({
                    "local": self.reachability,
                    "leader": self.leader,
                    "is_leader": self.is_leader,
                    "stale_ignored": self.detector.stale_ignored,
                    "members": self.detector.view(Instant::now()),
                })))));
                return;
            }
            AdminKind::Initialize { nodes } => ControlCommand::Initialize {
                op: call.request.op,
                principal: call.principal.clone().unwrap_or_else(|| "process".to_string()),
                expected_version: call.request.expected_version,
                nodes: nodes.clone(),
                tables: self.tables.clone(),
            },
            AdminKind::SetControlVoters { count } => ControlCommand::SetControlVoters {
                op: call.request.op,
                principal: call.principal.clone().unwrap_or_else(|| "process".to_string()),
                expected_version: call.request.expected_version,
                count: *count,
            },
            // the table is resolved by name against what this node serves, and the level
            // parsed, before anything is proposed
            AdminKind::SetTableReadPolicy { table, level } => {
                let Some((_, id)) = self.tables.iter().find(|(name, _)| name == table) else {
                    let _ = call.reply.send(answer(Err(AdminError::new(
                        ErrorCode::Internal,
                        format!("no table is named {table}; the schema serves {:?}", self.tables.iter().map(|(name, _)| name).collect::<Vec<_>>()),
                    ))));
                    return;
                };
                let level = match level.as_deref() {
                    None => None,
                    Some("one") => Some(crate::server::conf::cluster::Consistency::One),
                    Some("quorum") => Some(crate::server::conf::cluster::Consistency::Quorum),
                    Some(other) => {
                        let _ = call.reply.send(answer(Err(AdminError::new(
                            ErrorCode::UnsupportedReadLevel,
                            format!("{other} is not a read level; one or quorum, or nothing to clear"),
                        ))));
                        return;
                    }
                };
                ControlCommand::SetTableReadPolicy {
                    op: call.request.op,
                    principal: call.principal.clone().unwrap_or_else(|| "process".to_string()),
                    expected_version: call.request.expected_version,
                    table: *id,
                    level,
                }
            }
            // the record of a repair, as the applied state holds it
            AdminKind::RepairStatus { op } => {
                let outcome = match state.repairs.get(op) {
                    Some(record) => Ok(AdminOutcome::Read(serde_json::to_value(record).unwrap_or_default())),
                    None => Err(AdminError::new(ErrorCode::Internal, format!("no repair operation {op} is recorded"))),
                };
                let _ = call.reply.send(answer(outcome));
                return;
            }
            // the table and the mode are resolved before anything is proposed
            AdminKind::Repair {
                table,
                tablet,
                mode,
                source,
                release,
            } => {
                let Some((_, id)) = self.tables.iter().find(|(name, _)| name == table) else {
                    let _ = call.reply.send(answer(Err(AdminError::new(
                        ErrorCode::Internal,
                        format!("no table is named {table}; the schema serves {:?}", self.tables.iter().map(|(name, _)| name).collect::<Vec<_>>()),
                    ))));
                    return;
                };
                let Some(mode) = RepairMode::parse(mode) else {
                    let _ = call.reply.send(answer(Err(AdminError::new(
                        ErrorCode::Internal,
                        format!("{mode} is not a repair mode; verify or repair"),
                    ))));
                    return;
                };
                ControlCommand::Repair {
                    op: call.request.op,
                    principal: call.principal.clone().unwrap_or_else(|| "process".to_string()),
                    expected_version: call.request.expected_version,
                    table: *id,
                    tablet: *tablet,
                    mode,
                    source: *source,
                    release: *release,
                }
            }
            // the record of a move, as the applied state holds it
            // ([F45](../../../../docs/src/features/replica-migration.md))
            AdminKind::MoveStatus { op } => {
                let outcome = match state.moves.get(op) {
                    Some(record) => Ok(AdminOutcome::Read(serde_json::to_value(record).unwrap_or_default())),
                    None => Err(AdminError::new(ErrorCode::Internal, format!("no move operation {op} is recorded"))),
                };
                let _ = call.reply.send(answer(outcome));
                return;
            }
            // a move is judged whole by the state machine, against the map it derives
            AdminKind::Move { tablet, from, to } => ControlCommand::Move {
                op: call.request.op,
                principal: call.principal.clone().unwrap_or_else(|| "process".to_string()),
                expected_version: call.request.expected_version,
                tablet: *tablet,
                from: *from,
                to: *to,
            },
            // the placement operations, judged whole by the state machine
            // ([F46](../../../../docs/src/features/capacity-rebalancing.md))
            AdminKind::Decommission { node } => ControlCommand::Decommission {
                op: call.request.op,
                principal: call.principal.clone().unwrap_or_else(|| "process".to_string()),
                expected_version: call.request.expected_version,
                node: *node,
            },
            AdminKind::Remove { node, replacement } => ControlCommand::Remove {
                op: call.request.op,
                principal: call.principal.clone().unwrap_or_else(|| "process".to_string()),
                expected_version: call.request.expected_version,
                node: *node,
                replacement: *replacement,
            },
            AdminKind::Maintenance { node, suspend } => ControlCommand::Maintenance {
                op: call.request.op,
                principal: call.principal.clone().unwrap_or_else(|| "process".to_string()),
                expected_version: call.request.expected_version,
                node: *node,
                suspend: *suspend,
            },
            AdminKind::Rebalance => ControlCommand::Rebalance {
                op: call.request.op,
                principal: call.principal.clone().unwrap_or_else(|| "process".to_string()),
                expected_version: call.request.expected_version,
            },
            // the record of a plan, as the applied state holds it
            AdminKind::PlanStatus { op } => {
                let outcome = match state.plans.get(op) {
                    Some(record) => Ok(AdminOutcome::Read(serde_json::to_value(record).unwrap_or_default())),
                    None => Err(AdminError::new(ErrorCode::Internal, format!("no plan {op} is recorded"))),
                };
                let _ = call.reply.send(answer(outcome));
                return;
            }
            AdminKind::Plans => {
                let mut plans: Vec<&PlanRecord> = state.plans.values().collect();
                plans.sort_by_key(|record| record.requested_at);
                let _ = call.reply.send(answer(Ok(AdminOutcome::Read(serde_json::to_value(plans).unwrap_or_default()))));
                return;
            }
        };
        // a mutation needs a principal the committed policy names, unless the process itself asks
        if !call.trusted {
            let admins = state.policy.as_ref().map(|policy| policy.admins.clone()).unwrap_or_default();
            let allowed = call
                .principal
                .as_ref()
                .is_some_and(|principal| admins.contains(principal));
            if !allowed {
                let _ = call.reply.send(answer(Err(AdminError::new(
                    ErrorCode::Unauthorized,
                    format!(
                        "{} may not change the cluster; cluster.admins names {admins:?}",
                        call.principal.as_deref().unwrap_or("an unauthenticated connection")
                    ),
                ))));
                return;
            }
        }
        // an operation seen before is answered as it was the first time, before the version
        // is judged: the version moved when it applied, so an identical retry would otherwise
        // be refused as stale, and proposing it again would cost a log entry to learn what the
        // applied state already knows
        if let Some(seen) = state.operations.get(&call.request.op) {
            let outcome = match &seen.outcome {
                ControlResponse::Applied { topology_version } => Ok(AdminOutcome::Repeated {
                    version: *topology_version,
                }),
                other => Err(AdminError::new(ErrorCode::Internal, format!("repeated: {other:?}"))),
            };
            let _ = call.reply.send(answer(outcome));
            return;
        }
        // a stale version is refused before anything is proposed
        if call.request.expected_version != version {
            let _ = call.reply.send(answer(Err(AdminError::new(
                ErrorCode::StaleVersion,
                format!(
                    "the request was written against topology version {} and the cluster is at {version}",
                    call.request.expected_version
                ),
            ))));
            return;
        }
        event!(
            Level::INFO,
            msg = "admin operation",
            principal = call.principal.as_deref().unwrap_or("process"),
            op = %call.request.op,
            kind = call.request.kind.name(),
            expected_version = call.request.expected_version,
        );
        let raft = self.raft.clone();
        let network = self.network.clone();
        let machine = self.machine.clone();
        let node = self.node;
        glommio::spawn_local(async move {
            let outcome = match propose(&raft, &network, &machine, mutation).await {
                Ok(ControlResponse::Applied { topology_version }) => Ok(AdminOutcome::Applied {
                    version: topology_version,
                }),
                Ok(ControlResponse::Repeated { first }) => match *first {
                    ControlResponse::Applied { topology_version } => Ok(AdminOutcome::Repeated {
                        version: topology_version,
                    }),
                    ControlResponse::Refused { reason } => Err(AdminError::new(
                        if reason.contains("stale version") {
                            ErrorCode::StaleVersion
                        } else {
                            ErrorCode::Internal
                        },
                        format!("repeated: {reason}"),
                    )),
                    other => Err(AdminError::new(ErrorCode::Internal, format!("repeated: {other:?}"))),
                },
                Ok(ControlResponse::Refused { reason }) => Err(AdminError::new(
                    if reason.contains("stale version") {
                        ErrorCode::StaleVersion
                    } else {
                        ErrorCode::Internal
                    },
                    reason,
                )),
                Ok(ControlResponse::Fenced { .. }) => {
                    Err(AdminError::new(ErrorCode::Internal, "fenced".to_string()))
                }
                Ok(ControlResponse::Removed { node }) => {
                    Err(AdminError::new(ErrorCode::Internal, format!("{node} is a removed identity")))
                }
                Err(ProposeError::NoLeader) => Err(AdminError::new(
                    ErrorCode::NotLeader,
                    "no control leader could be reached; the cluster may lack a quorum".to_string(),
                )),
                Err(ProposeError::Failed(msg)) => Err(AdminError::new(ErrorCode::Internal, msg)),
            };
            let version = machine.state().topology_version;
            let _ = call.reply.send(AdminResponse {
                node,
                topology_version: version,
                outcome,
            });
        })
        .detach();
    }

    /// Act on the state machine having applied something
    fn handle_applied(&mut self) -> Result<(), ServerError> {
        let state = self.machine.state();
        // a run of this node the cluster has replaced stops here, and so does a removed one
        if let Some(mine) = state.members.get(&self.node) {
            if mine.phase == MemberPhase::Removed {
                return Err(ServerError::Shoal(ShoalError::Removed { node: self.node }));
            }
            if mine.record.incarnation > self.member.incarnation {
                return Err(ServerError::Shoal(ShoalError::Fenced {
                    node: self.node,
                    committed: mine.record.incarnation,
                    ours: self.member.incarnation,
                }));
            }
        }
        if state.tombstones.contains_key(&self.node) {
            return Err(ServerError::Shoal(ShoalError::Removed { node: self.node }));
        }
        // a joiner that now sees itself in the state has been replicated to; it observes itself
        if self.status == JoinStatus::Joining && state.members.contains_key(&self.node) {
            self.status = JoinStatus::Recovering;
            self.joining = false;
        }
        // the marker's high water mark, and the map
        if state.topology_version > 0 {
            let root = self.root.clone();
            let version = state.topology_version;
            glommio::spawn_local(async move {
                if let Err(error) = observe(&root, version).await {
                    event!(Level::WARN, msg = "could not record the topology version", ?error);
                }
            })
            .detach();
        }
        self.publish();
        self.maybe_observe();
        self.maybe_promote();
        self.drive_plans(true);
        Ok(())
    }

    /// Act on the group's metrics having changed
    ///
    /// # Arguments
    ///
    /// * `metrics` - The metrics
    fn handle_metrics(&mut self, metrics: Box<RaftMetrics<ControlConfig>>) {
        let leader = metrics.current_leader;
        let was_leader = self.is_leader;
        self.is_leader = leader == Some(self.node);
        if leader != self.leader {
            event!(Level::INFO, msg = "control leader", leader = ?leader, me = self.is_leader);
            self.leader = leader;
            self.publish();
        }
        if self.is_leader && !was_leader {
            // a new leader starts with no evidence about anybody: its detector is seeded with
            // every up member and a grace period, so the election itself calls nobody down
            self.reachability.clear();
            self.detector.reset();
            self.health_in_flight.clear();
            // and no count of anybody's grace, no plan in hand: both resume from what is
            // committed ([F46](../../../../docs/src/features/capacity-rebalancing.md))
            self.grace_seen.clear();
            self.grace_in_flight.clear();
            self.plan_in_flight.clear();
            self.finishing.clear();
            self.capacity.clear();
            self.last_plan = None;
            let now = Instant::now();
            for (node, member) in &self.machine.state().members {
                if *node != self.node && member.health == MemberHealth::Up && member.phase != MemberPhase::Removed {
                    self.detector.seed(*node, member.record.incarnation, now);
                }
            }
        }
        self.metrics = Some(metrics);
        self.maybe_observe();
        self.maybe_promote();
    }

    /// Observe this node through the leader, once one is known and it has not been done
    fn maybe_observe(&mut self) {
        if self.status != JoinStatus::Recovering || self.observing || self.leader.is_none() {
            return;
        }
        // a failed observation is not retried on every metrics change, only after the backoff
        if self.observe_after.is_some_and(|at| Instant::now() < at) {
            return;
        }
        self.observing = true;
        let raft = self.raft.clone();
        let network = self.network.clone();
        let machine = self.machine.clone();
        let tx = self.tx.clone();
        let command = ControlCommand::ObserveMember(self.member.clone());
        glommio::spawn_local(async move {
            let outcome = propose(&raft, &network, &machine, command).await;
            let _ = tx.send(Event::Observed(outcome)).await;
        })
        .detach();
    }

    /// Act on this node's own observation having finished
    ///
    /// # Arguments
    ///
    /// * `outcome` - What it produced
    fn handle_observed(&mut self, outcome: Result<ControlResponse, ProposeError>) -> Result<(), ServerError> {
        self.observing = false;
        match outcome {
            Ok(ControlResponse::Applied { .. } | ControlResponse::Repeated { .. }) => {
                self.status = JoinStatus::Joined;
                self.observe_after = None;
                event!(Level::INFO, msg = "joined", node = %self.node, incarnation = self.member.incarnation);
                self.publish();
                Ok(())
            }
            Ok(ControlResponse::Fenced { committed, offered, .. }) => Err(ServerError::Shoal(ShoalError::Fenced {
                node: self.node,
                committed,
                ours: offered,
            })),
            Ok(ControlResponse::Removed { node }) => Err(ServerError::Shoal(ShoalError::Removed { node })),
            Ok(ControlResponse::Refused { reason }) => {
                event!(Level::WARN, msg = "the observation was refused", reason);
                self.observe_after = Some(Instant::now() + OBSERVE_BACKOFF);
                Ok(())
            }
            Err(error) => {
                // no leader yet; the tick, or a metrics change after the backoff, tries again
                event!(Level::DEBUG, msg = "the observation did not commit", %error);
                self.observe_after = Some(Instant::now() + OBSERVE_BACKOFF);
                Ok(())
            }
        }
    }

    /// Start dialling the seeds
    fn start_join(&mut self) {
        if self.joining {
            return;
        }
        self.joining = true;
        let network = self.network.clone();
        let seeds = self.seeds.clone();
        let member = self.member.clone();
        let schema_id = self.local.borrow().schema_id;
        let transport = self.transport.clone();
        let tx = self.tx.clone();
        glommio::spawn_local(async move {
            let outcome = join(&network, &seeds, member, schema_id, &transport).await;
            let _ = tx.send(Event::Joined(outcome)).await;
        })
        .detach();
    }

    /// Act on the join having finished
    ///
    /// # Arguments
    ///
    /// * `outcome` - The cluster and the leader that admitted this node, or why not
    async fn handle_joined(&mut self, outcome: Result<(ClusterId, NodeId), String>) -> Result<(), ServerError> {
        match outcome {
            Ok((cluster, leader)) => {
                // the cluster is known now, before the leader's first append arrives
                self.local.borrow_mut().cluster = Some(cluster);
                self.leader = Some(leader);
                // and durably, so a restart resumes as a member of it
                let root = self.root.clone();
                glommio::executor()
                    .spawn_blocking(move || StorageMeta::adopt_cluster(&root, cluster))
                    .await?;
                event!(Level::INFO, msg = "admitted", node = %self.node, cluster = %cluster, leader = %leader);
                // the state may already hold this node, if replication beat the answer
                self.handle_applied()
            }
            Err(reason) => Err(ServerError::Shoal(ShoalError::JoinRefused { reason })),
        }
    }

    /// Promote a learner to voter while the policy asks for more voters, if this node leads
    fn maybe_promote(&mut self) {
        if !self.is_leader || self.promoting {
            return;
        }
        let Some(metrics) = &self.metrics else {
            return;
        };
        // no promotion while a joint configuration is uncommitted
        if metrics.membership_config.membership() != metrics.committed_membership_config.membership() {
            return;
        }
        let state = self.machine.state();
        let want = state
            .policy
            .as_ref()
            .map_or(self.policy.control_voters, |policy| policy.control_voters);
        let voters: BTreeSet<NodeId> = metrics.membership_config.membership().voter_ids().collect();
        if voters.len() >= want as usize {
            return;
        }
        // the first learner that is up and staying, in node order
        let candidate = state
            .members
            .iter()
            .find(|(node, member)| {
                member.is_placeable()
                    && !voters.contains(node)
                    && metrics.membership_config.membership().nodes().any(|(id, _)| id == *node)
            })
            .map(|(node, member)| (*node, member.record.clone()));
        let Some((learner, record)) = candidate else {
            return;
        };
        self.promoting = true;
        let raft = self.raft.clone();
        let tx = self.tx.clone();
        glommio::spawn_local(async move {
            let outcome = promote(&raft, learner, record).await;
            let _ = tx.send(Event::Promoted(learner, outcome)).await;
        })
        .detach();
    }

    /// Every quarantined copy across this node's shards, as they last reported
    fn quarantined_copies(&self) -> Vec<QuarantinedCopy> {
        let mut copies: Vec<QuarantinedCopy> = self
            .replication
            .values()
            .flat_map(|shard| shard.groups.iter())
            .filter_map(|group| {
                group.quarantined.map(|reason| QuarantinedCopy {
                    table: group.table,
                    group: group.group,
                    tablets: group.tablet_ids.clone(),
                    reason,
                })
            })
            .collect();
        copies.sort_by_key(|copy| copy.group);
        copies
    }

    /// Send the leader this node's status report
    fn report(&mut self) {
        if self.status != JoinStatus::Joined {
            return;
        }
        // the leader keeps its own health; a change in its shards is committed directly, and
        // its capacity is noted where a report would have put it
        if self.is_leader {
            let (free_bytes, group_bytes) = self.own_capacity();
            self.note_capacity(self.node, self.member.incarnation, free_bytes, &group_bytes);
            if self.reported_quarantine != self.quarantined {
                self.reported_quarantine = self.quarantined.clone();
                let raft = self.raft.clone();
                let network = self.network.clone();
                let machine = self.machine.clone();
                let command = ControlCommand::ReportQuarantine {
                    node: self.node,
                    incarnation: self.member.incarnation,
                    copies: self.quarantined.clone(),
                };
                glommio::spawn_local(async move {
                    let _ = propose(&raft, &network, &machine, command).await;
                })
                .detach();
            }
            if self.reported_shards != self.shards_failed {
                self.reported_shards = self.shards_failed.clone();
                let raft = self.raft.clone();
                let network = self.network.clone();
                let machine = self.machine.clone();
                let command = ControlCommand::ReportShards {
                    node: self.node,
                    incarnation: self.member.incarnation,
                    failed: self.shards_failed.clone(),
                };
                glommio::spawn_local(async move {
                    let _ = propose(&raft, &network, &machine, command).await;
                })
                .detach();
            }
            return;
        }
        let _ = self.send_report(self.member.incarnation, None);
    }

    /// Call down every member the detector suspects, if this node leads
    ///
    /// One verdict per member is in flight at a time, the leader never judges itself, and a
    /// member the cluster already holds down is not called down again.
    fn judge_members(&mut self) {
        if !self.is_leader {
            return;
        }
        let now = Instant::now();
        let state = self.machine.state();
        for node in self.detector.suspects(now) {
            if node == self.node {
                continue;
            }
            let Some(member) = state.members.get(&node) else {
                self.detector.forget(node);
                continue;
            };
            if member.health != MemberHealth::Up {
                continue;
            }
            let phi = self.detector.phi(node, now).unwrap_or(0.0);
            event!(Level::WARN, msg = "a member fell silent", %node, phi);
            self.propose_health(node, MemberHealth::Down, member.record.incarnation, Some(Uuid::new_v4()));
        }
    }

    /// Propose a member's health through the group, once at a time per member
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `health` - What to set it to
    /// * `incarnation` - The run the evidence is about
    /// * `episode` - The down episode this opens, if it opens one
    fn propose_health(&mut self, node: NodeId, health: MemberHealth, incarnation: u64, episode: Option<Uuid>) {
        if !self.health_in_flight.insert(node) {
            return;
        }
        let raft = self.raft.clone();
        let network = self.network.clone();
        let machine = self.machine.clone();
        let tx = self.tx.clone();
        let command = ControlCommand::SetHealth {
            node,
            health,
            incarnation,
            episode,
        };
        glommio::spawn_local(async move {
            let outcome = propose(&raft, &network, &machine, command).await;
            let _ = tx.send(Event::HealthProposed(node, outcome)).await;
        })
        .detach();
    }

    /// Act on a health verdict having finished
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `outcome` - What the group answered
    fn handle_health_proposed(&mut self, node: NodeId, outcome: Result<ControlResponse, ProposeError>) {
        self.health_in_flight.remove(&node);
        match outcome {
            Ok(ControlResponse::Applied { topology_version }) => {
                event!(Level::INFO, msg = "a member's health was committed", %node, topology_version);
            }
            Ok(other) => event!(Level::DEBUG, msg = "a health verdict changed nothing", %node, ?other),
            Err(error) => event!(Level::DEBUG, msg = "a health verdict did not commit", %node, %error),
        }
    }

    /// Send one report at an incarnation
    ///
    /// # Arguments
    ///
    /// * `incarnation` - The incarnation to report as
    /// * `seq` - A sequence to send instead of the next one, which a test uses to replay
    fn send_report(&mut self, incarnation: u64, seq: Option<u64>) -> Result<(), String> {
        let Some(leader) = self.leader_record() else {
            return Err("no leader to report to".to_string());
        };
        let seq = match seq {
            Some(seq) => seq,
            None => {
                self.report_seq += 1;
                self.report_seq
            }
        };
        let (free_bytes, group_bytes) = self.own_capacity();
        let report = StatusReport {
            node: self.node,
            incarnation,
            seq,
            topology_version: self.machine.state().topology_version,
            applied_index: self.machine.applied_index(),
            shards_failed: self.shards_failed.clone(),
            reachability: self
                .reachability
                .iter()
                .filter(|(_, reach)| reach.misses == 0)
                .map(|(node, reach)| (*node, reach.rtt_us))
                .collect(),
            quarantined: self.quarantined.iter().map(QuarantinedCopy::to_member).collect(),
            free_bytes,
            group_bytes,
        };
        let payload = serde_json::to_vec(&report).map_err(|error| error.to_string())?;
        let network = self.network.clone();
        let tx = self.tx.clone();
        let deadline = Duration::from_millis(self.policy.failure_detector.interval_ms.max(50));
        glommio::spawn_local(async move {
            let peer = network.peer(&network.addr_of(&leader));
            let outcome = peer.rpc(ControlKind::StatusReport, payload, deadline).await;
            let _ = tx.send(Event::Reported(outcome)).await;
        })
        .detach();
        Ok(())
    }

    /// Act on the leader's answer to a report
    ///
    /// # Arguments
    ///
    /// * `outcome` - What it answered
    fn handle_reported(&mut self, outcome: Result<Vec<u8>, RpcFailure>) -> Result<(), ServerError> {
        match outcome {
            Ok(_) => Ok(()),
            // a removal is the leader telling this identity it is gone for good
            Err(RpcFailure::Remote(msg)) if msg.starts_with("removed") => {
                Err(ServerError::Shoal(ShoalError::Removed { node: self.node }))
            }
            // a fence is the leader telling this run it has been replaced
            Err(RpcFailure::Remote(msg)) if msg.starts_with("fenced") => {
                let committed = self
                    .machine
                    .state()
                    .members
                    .get(&self.node)
                    .map_or(0, |member| member.record.incarnation);
                Err(ServerError::Shoal(ShoalError::Fenced {
                    node: self.node,
                    committed,
                    ours: self.member.incarnation,
                }))
            }
            Err(error) => {
                event!(Level::DEBUG, msg = "a report was not answered", %error);
                Ok(())
            }
        }
    }

    /// Ping every member this node knows, and note who answered
    ///
    /// A local observation and nothing more: it feeds the detector view and the report's
    /// reachability, and never a membership decision.
    fn ping_members(&mut self) {
        let state = self.machine.state();
        for (node, member) in &state.members {
            if *node == self.node || member.phase == MemberPhase::Removed {
                continue;
            }
            let record = member.record.clone();
            let network = self.network.clone();
            let tx = self.tx.clone();
            let node = *node;
            glommio::spawn_local(async move {
                let mut peer = network.peer(&network.addr_of(&record));
                let started = Instant::now();
                let answered = peer.ping().await.map(|_| started.elapsed()).map_err(|_| ());
                let _ = tx.send(Event::Pinged(node, answered)).await;
            })
            .detach();
        }
    }

    /// Note what a ping learned
    ///
    /// # Arguments
    ///
    /// * `node` - The member pinged
    /// * `answered` - The round trip, or that it did not answer
    fn handle_pinged(&mut self, node: NodeId, answered: Result<Duration, ()>) {
        let entry = self.reachability.entry(node).or_insert(Reachability {
            rtt_us: 0,
            misses: 0,
        });
        match answered {
            Ok(rtt) => {
                // truncation cannot happen for a round trip anybody waits for
                #[allow(clippy::cast_possible_truncation)]
                let rtt_us = rtt.as_micros().min(u128::from(u32::MAX)) as u32;
                entry.rtt_us = rtt_us;
                entry.misses = 0;
            }
            Err(()) => entry.misses = entry.misses.saturating_add(1),
        }
    }

    /// This node's own capacity: the free bytes on its storage and the bytes its groups hold
    ///
    /// Folded from what every shard last reported, so a group hosted on two shards is summed
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md)).
    fn own_capacity(&self) -> (u64, Vec<(u64, u64)>) {
        let free_bytes = super::capacity::free_bytes(&self.root).unwrap_or(0);
        let mut groups: BTreeMap<u64, u64> = BTreeMap::new();
        for shard in self.replication.values() {
            for group in &shard.groups {
                *groups.entry(group.group.0).or_default() += group.bytes;
            }
        }
        (free_bytes, groups.into_iter().collect())
    }

    /// Note what a member reported about its capacity, for the planner
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `incarnation` - The incarnation it reported at
    /// * `free_bytes` - The free bytes on its storage
    /// * `group_bytes` - The bytes each of its groups holds
    fn note_capacity(&mut self, node: NodeId, incarnation: u64, free_bytes: u64, group_bytes: &[(u64, u64)]) {
        let capacity = NodeCapacity {
            free_bytes,
            group_bytes: group_bytes.iter().copied().collect(),
            at: Instant::now(),
            incarnation,
        };
        let moved = self
            .capacity
            .get(&node)
            .is_none_or(|known| known.free_bytes != capacity.free_bytes || known.group_bytes != capacity.group_bytes);
        if moved {
            self.capacity_moved = true;
        }
        self.capacity.insert(node, capacity);
    }

    /// Count every down member's grace and commit what has elapsed, if this node leads
    ///
    /// The count starts from the committed value at the last commit, or at the first sight
    /// of the episode after an election, and is committed every eighth of the grace; when it
    /// reaches the grace the member is removing under a plan minted here. A suspended grace
    /// is neither counted nor committed, and starts again from its committed value on
    /// resumption. Nothing is guessed early: a new leader loses at most one increment
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md), Q7).
    fn accrue_graces(&mut self) {
        if !self.is_leader {
            return;
        }
        let state = self.machine.state();
        let Some(grace) = state.policy.as_ref().and_then(|policy| policy.auto_remove_after) else {
            return;
        };
        let grace = grace.duration();
        let grace_ms = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX);
        let every = (grace / 8).min(GRACE_COMMIT_CAP).max(Duration::from_millis(10));
        let now = Instant::now();
        // forget counts of members no longer under a grace this leader should count
        self.grace_seen.retain(|node, local| {
            state.members.get(node).and_then(|member| member.grace.as_ref()).is_some_and(|grace| {
                grace.episode == local.episode && !grace.suspended && !grace.expired
            })
        });
        for (node, member) in &state.members {
            if *node == self.node || member.health != MemberHealth::Down || member.phase == MemberPhase::Removed {
                continue;
            }
            let Some(committed) = member.grace.as_ref() else {
                continue;
            };
            if committed.suspended || committed.expired || self.grace_in_flight.contains(node) {
                continue;
            }
            // the count this leader keeps, started at the committed value when first seen
            let local = self.grace_seen.entry(*node).or_insert(GraceLocal {
                episode: committed.episode,
                since: now,
                committed_ms: committed.elapsed_ms,
            });
            // a commit that moved under us - another leader's - restarts the local count from it
            if local.committed_ms < committed.elapsed_ms {
                local.committed_ms = committed.elapsed_ms;
                local.since = now;
            }
            let elapsed = now.saturating_duration_since(local.since);
            let total_ms = local
                .committed_ms
                .saturating_add(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX));
            let expire = total_ms >= grace_ms;
            if !expire && elapsed < every {
                continue;
            }
            let command = ControlCommand::GraceElapsed {
                node: *node,
                episode: committed.episode,
                elapsed_ms: total_ms.min(grace_ms),
                expire: expire.then(Uuid::new_v4),
            };
            if expire {
                event!(Level::WARN, msg = "a down member's grace has elapsed; removing it", %node, elapsed_ms = total_ms, grace_ms);
            }
            self.grace_in_flight.insert(*node);
            let raft = self.raft.clone();
            let network = self.network.clone();
            let machine = self.machine.clone();
            let tx = self.tx.clone();
            let node = *node;
            glommio::spawn_local(async move {
                let outcome = propose(&raft, &network, &machine, command).await;
                let _ = tx.send(Event::GraceProposed(node, outcome)).await;
            })
            .detach();
        }
    }

    /// Act on a grace count having finished
    ///
    /// # Arguments
    ///
    /// * `node` - The member
    /// * `outcome` - What the group answered
    fn handle_grace_proposed(&mut self, node: NodeId, outcome: Result<ControlResponse, ProposeError>) {
        self.grace_in_flight.remove(&node);
        match outcome {
            Ok(ControlResponse::Applied { .. }) => {
                // the count starts again from what is now committed
                let committed = self
                    .machine
                    .state()
                    .members
                    .get(&node)
                    .and_then(|member| member.grace.as_ref().map(|grace| grace.elapsed_ms));
                if let (Some(local), Some(committed)) = (self.grace_seen.get_mut(&node), committed) {
                    local.committed_ms = committed;
                    local.since = Instant::now();
                }
            }
            Ok(other) => {
                event!(Level::DEBUG, msg = "a grace count changed nothing", %node, ?other);
                self.grace_seen.remove(&node);
            }
            Err(error) => event!(Level::DEBUG, msg = "a grace count did not commit", %node, %error),
        }
    }

    /// Look at every open plan and move it on, if this node leads
    ///
    /// Every `plan_interval`, and sooner when the state or the capacity moved: a planned
    /// plan gets its steps, a running one has its moving steps judged by their moves and its
    /// pending steps issued under the caps, a blocked one is planned again, and one whose
    /// every step moved is finished. One proposal per plan is in flight at a time, and a
    /// leader change resumes from the record ([F46](../../../../docs/src/features/capacity-rebalancing.md)).
    ///
    /// # Arguments
    ///
    /// * `on_change` - Whether this is a state change rather than the interval
    fn drive_plans(&mut self, on_change: bool) {
        if !self.is_leader || self.status != JoinStatus::Joined {
            return;
        }
        let state = self.machine.state();
        if state.open_plans().is_empty() {
            return;
        }
        let now = Instant::now();
        let interval = self.rebalance.plan_interval.duration();
        let since = self.last_plan.map(|at| now.saturating_duration_since(at));
        // the interval, or a change since the last look after the floor between looks
        let due = match since {
            None => true,
            Some(since) if since >= interval => true,
            Some(since) if since >= PLAN_MIN_INTERVAL => {
                on_change && (state.topology_version != self.planned_at || self.capacity_moved)
            }
            Some(_) => false,
        };
        if !due {
            return;
        }
        self.last_plan = Some(now);
        self.planned_at = state.topology_version;
        self.capacity_moved = false;
        let map = TabletMap::from_state(&state, self.leader, &self.tables);
        for record in state.open_plans() {
            if self.plan_in_flight.contains(&record.op) {
                continue;
            }
            if let Some(update) = self.next_plan_update(&state, &map, record) {
                self.propose_plan(record.op, update);
            }
        }
    }

    /// What a plan needs next, or nothing while its moves run
    ///
    /// # Arguments
    ///
    /// * `state` - The applied state
    /// * `map` - The map it derives
    /// * `record` - The plan
    fn next_plan_update(&mut self, state: &ControlState, map: &TabletMap, record: &PlanRecord) -> Option<PlanUpdate> {
        // a step whose move is done is moved or failed, whichever the move says
        for step in record.live_steps() {
            if step.state != StepState::Moving {
                continue;
            }
            let Some(op) = step.op else {
                continue;
            };
            match state.moves.get(&op) {
                Some(moved) if moved.is_done() => {
                    let outcome = match &moved.outcome {
                        Some(super::migrate::MoveOutcome::Failed { reason }) => StepState::Failed { reason: reason.clone() },
                        _ => StepState::Moved,
                    };
                    return Some(PlanUpdate::Step {
                        tablet: step.tablet,
                        op: Some(op),
                        state: outcome,
                    });
                }
                Some(_) => {}
                // a move the state forgot is a move that will never report
                None => {
                    return Some(PlanUpdate::Step {
                        tablet: step.tablet,
                        op: Some(op),
                        state: StepState::Failed {
                            reason: format!("move {op} is no longer recorded"),
                        },
                    });
                }
            }
        }
        // a pending step under the caps becomes a move
        let in_flight = self.moves_in_flight(state);
        let cap = self.rebalance.moves_per_node;
        for step in record.live_steps() {
            if step.state != StepState::Pending {
                continue;
            }
            let as_source = in_flight.iter().filter(|(from, _)| *from == step.from).count();
            let as_destination = in_flight.iter().filter(|(_, to)| *to == step.to).count();
            if as_source >= cap as usize || as_destination >= cap as usize {
                continue;
            }
            return Some(PlanUpdate::Step {
                tablet: step.tablet,
                op: Some(Uuid::new_v4()),
                state: StepState::Moving,
            });
        }
        // a plan finishing: the member is taken out of the control group, then tombstoned
        if record.phase == PlanPhase::Finishing {
            if let Some(node) = record.kind.drains() {
                self.finish_removal(record.op, node);
            }
            return None;
        }
        // nothing is moving: plan what is left, from the sets as they are served now
        if record.live_steps().any(|step| step.state == StepState::Moving) {
            return None;
        }
        let input = self.plan_input(state, map, record, &in_flight);
        let output = planner::plan(&record.kind, &input);
        if !output.steps.is_empty() {
            return Some(PlanUpdate::Steps {
                steps: output.steps,
                blocked: output.blocked,
            });
        }
        if let Some(reason) = output.blocked {
            // blocked, and pending steps that cannot be issued are nothing to wait for
            let same = record.blocked.as_ref().is_some_and(|blocked| blocked.reason == reason);
            return if same { None } else { Some(PlanUpdate::Blocked(Some(reason))) };
        }
        // nothing to plan: a drain that moved everything finishes, a rebalance is done
        match record.kind.drains() {
            Some(_) => Some(PlanUpdate::Finishing),
            None => Some(PlanUpdate::Done(if record.steps.is_empty() {
                PlanOutcome::Nothing {
                    reason: output.nothing.unwrap_or_else(|| "nothing to move".to_string()),
                }
            } else {
                record.completed()
            })),
        }
    }

    /// Every move not done, as source and destination nodes
    ///
    /// # Arguments
    ///
    /// * `state` - The applied state
    fn moves_in_flight(&self, state: &ControlState) -> Vec<(NodeId, NodeId)> {
        state
            .moves
            .values()
            .filter(|record| !record.is_done())
            .map(|record| (record.from.node, record.to.node))
            .collect()
    }

    /// What the planner is given for a plan: the sets as served, the members, the capacity
    ///
    /// # Arguments
    ///
    /// * `state` - The applied state
    /// * `map` - The map it derives
    /// * `record` - The plan
    /// * `in_flight` - The moves not done
    fn plan_input(&self, state: &ControlState, map: &TabletMap, record: &PlanRecord, in_flight: &[(NodeId, NodeId)]) -> PlanInput {
        let draining = record.kind.drains();
        let nodes: BTreeMap<NodeId, NodeInput> = state
            .members
            .iter()
            .map(|(node, member)| {
                (
                    *node,
                    NodeInput {
                        eligible: member.is_placeable() && Some(*node) != draining,
                        weight: member.record.effective_weight(),
                        free_bytes: self.capacity.get(node).map(|capacity| capacity.free_bytes),
                    },
                )
            })
            .collect();
        // the groups of every set, so a member's reported bytes fold onto the set
        let groups_by_set: BTreeMap<u16, Vec<u64>> = state
            .tables
            .iter()
            .flat_map(|(_, table)| map.groups_of(*table))
            .fold(BTreeMap::new(), |mut acc, (group, _, tablets)| {
                acc.entry(tablets[0]).or_default().push(group.0);
                acc
            });
        let busy_tablets: BTreeSet<u16> = record
            .live_steps()
            .map(|step| step.tablet)
            .chain(state.moves.values().filter(|moved| !moved.is_done()).map(|moved| moved.tablets[0]))
            .collect();
        let sets = map
            .rule_sets_served()
            .into_iter()
            .map(|(members, tablets)| {
                let first = tablets[0];
                let groups = groups_by_set.get(&first).cloned().unwrap_or_default();
                let bytes = members
                    .iter()
                    .map(|member| {
                        let held = self.capacity.get(&member.node).map_or(0, |capacity| {
                            groups.iter().map(|group| capacity.group_bytes.get(group).copied().unwrap_or(0)).sum()
                        });
                        (member.node, held)
                    })
                    .collect();
                SetInput {
                    tablet: first,
                    members: members.iter().map(|member| member.node).collect(),
                    bytes,
                    busy: busy_tablets.contains(&first),
                    failures: record.failures_of(first),
                }
            })
            .collect();
        PlanInput {
            nodes,
            sets,
            disk_reserve: self.migration.disk_reserve,
            moves_per_node: self.rebalance.moves_per_node,
            hysteresis: self.rebalance.hysteresis,
            in_flight: in_flight.to_vec(),
        }
    }

    /// Commit a plan's progress, issuing the move a step names first
    ///
    /// # Arguments
    ///
    /// * `op` - The plan
    /// * `update` - What changed
    fn propose_plan(&mut self, op: Uuid, update: PlanUpdate) {
        if !self.plan_in_flight.insert(op) {
            return;
        }
        let state = self.machine.state();
        // the move a step becomes: issued under the plan's name against the current version
        let moving = match &update {
            PlanUpdate::Step {
                tablet,
                op: Some(moved),
                state: StepState::Moving,
            } => state.plans.get(&op).and_then(|record| {
                record
                    .live_steps()
                    .find(|step| step.tablet == *tablet && step.state == StepState::Pending)
                    .map(|step| ControlCommand::Move {
                        op: *moved,
                        principal: format!("plan {op}"),
                        expected_version: state.topology_version,
                        tablet: step.tablet,
                        from: step.from,
                        to: step.to,
                    })
            }),
            _ => None,
        };
        event!(Level::INFO, msg = "plan progress", plan = %op, ?update);
        let raft = self.raft.clone();
        let network = self.network.clone();
        let machine = self.machine.clone();
        let tx = self.tx.clone();
        let me = self.node;
        let incarnation = self.member.incarnation;
        glommio::spawn_local(async move {
            // the move first; a refusal is the step failing, not a step moving. A version that
            // moved under the proposal - another commit landing between the read and the
            // write - is what an operator's tool retries, so this does, against the current one
            let update = match moving {
                Some(mut command) => {
                    let mut answer = None;
                    for _ in 0..PLAN_MOVE_RETRIES {
                        if let ControlCommand::Move { expected_version, .. } = &mut command {
                            *expected_version = machine.state().topology_version;
                        }
                        match propose(&raft, &network, &machine, command.clone()).await {
                            Ok(ControlResponse::Refused { reason }) if reason.contains("stale version") => {
                                glommio::timer::sleep(LEASE_POLL).await;
                                answer = Some(Ok(ControlResponse::Refused { reason }));
                            }
                            other => {
                                answer = Some(other);
                                break;
                            }
                        }
                    }
                    match answer {
                        Some(Ok(ControlResponse::Applied { .. } | ControlResponse::Repeated { .. })) => update,
                        Some(Ok(other)) => match update {
                            PlanUpdate::Step { tablet, op: moved, .. } => PlanUpdate::Step {
                                tablet,
                                op: moved,
                                state: StepState::Failed {
                                    reason: format!("the move was refused: {other:?}"),
                                },
                            },
                            other => other,
                        },
                        Some(Err(error)) => {
                            let _ = tx.send(Event::PlanProposed(op, Err(error))).await;
                            return;
                        }
                        None => {
                            let _ = tx.send(Event::PlanProposed(op, Err(ProposeError::Failed("no move was proposed".to_string())))).await;
                            return;
                        }
                    }
                }
                None => update,
            };
            let command = ControlCommand::PlanProgress {
                op,
                node: me,
                incarnation,
                progress: update,
            };
            let outcome = propose(&raft, &network, &machine, command).await;
            let _ = tx.send(Event::PlanProposed(op, outcome)).await;
        })
        .detach();
    }

    /// Act on a plan's progress having been committed, or not
    ///
    /// # Arguments
    ///
    /// * `op` - The plan
    /// * `outcome` - What the group answered
    fn handle_plan_proposed(&mut self, op: Uuid, outcome: Result<ControlResponse, ProposeError>) {
        self.plan_in_flight.remove(&op);
        match outcome {
            Ok(ControlResponse::Applied { .. }) => {
                // look again at once: the next step may be issuable now
                self.last_plan = None;
                self.drive_plans(true);
            }
            Ok(other) => event!(Level::DEBUG, msg = "a plan's progress changed nothing", plan = %op, ?other),
            Err(error) => event!(Level::DEBUG, msg = "a plan's progress did not commit", plan = %op, %error),
        }
    }

    /// Take a drained member out of the control group and tombstone it, once per plan at a time
    ///
    /// The tombstone is committed first, while the member still receives the log, so a live
    /// member learns it is removed and stops; then the member leaves the group's
    /// configuration - a voter through the joint transition, which openraft refuses while
    /// the old configuration has no quorum, a learner outright - and the plan is done. A
    /// leader that is the member itself hands the lead over rather than removing itself
    /// ([F46](../../../../docs/src/features/capacity-rebalancing.md)).
    ///
    /// # Arguments
    ///
    /// * `op` - The plan
    /// * `node` - The member
    fn finish_removal(&mut self, op: Uuid, node: NodeId) {
        if !self.finishing.insert(op) {
            return;
        }
        let Some(metrics) = self.metrics.as_ref() else {
            self.finishing.remove(&op);
            return;
        };
        // no membership change while one is half way through
        if metrics.membership_config.membership() != metrics.committed_membership_config.membership() {
            self.finishing.remove(&op);
            return;
        }
        let membership = metrics.membership_config.membership().clone();
        let raft = self.raft.clone();
        let network = self.network.clone();
        let machine = self.machine.clone();
        let tx = self.tx.clone();
        let me = self.node;
        glommio::spawn_local(async move {
            let outcome = async {
                // the leader itself hands the lead to another voter and lets it finish
                if node == me {
                    let successor = membership.voter_ids().find(|voter| *voter != me);
                    let Some(successor) = successor else {
                        return Err("this node is the member being removed and the only voter".to_string());
                    };
                    raft.trigger()
                        .transfer_leader(successor)
                        .await
                        .map_err(|error| format!("handing the lead to {successor}: {error}"))?;
                    return Err(format!("this node is the member being removed; the lead was handed to {successor}"));
                }
                // the tombstone first, while the member still hears the log
                match propose(&raft, &network, &machine, ControlCommand::Tombstone { node, op: Some(op) }).await {
                    Ok(ControlResponse::Applied { .. }) => {}
                    Ok(other) => return Err(format!("the tombstone was refused: {other:?}")),
                    Err(error) => return Err(format!("the tombstone did not commit: {error}")),
                }
                // then out of the configuration: a voter through the joint transition, a learner outright
                let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
                let present = membership.nodes().any(|(id, _)| *id == node);
                if voters.contains(&node) {
                    let mut ids = BTreeSet::new();
                    ids.insert(node);
                    raft.change_membership(ChangeMembers::RemoveVoters(ids), false)
                        .await
                        .map_err(|error| format!("removing the voter: {error}"))?;
                } else if present {
                    let mut ids = BTreeSet::new();
                    ids.insert(node);
                    raft.change_membership(ChangeMembers::RemoveNodes(ids), false)
                        .await
                        .map_err(|error| format!("removing the learner: {error}"))?;
                }
                Ok(())
            }
            .await;
            let _ = tx.send(Event::Finished(op, node, outcome)).await;
        })
        .detach();
    }

    /// Act on a member's removal from the control group having finished, or not
    ///
    /// # Arguments
    ///
    /// * `op` - The plan
    /// * `node` - The member
    /// * `outcome` - Whether it is out, or why not
    fn handle_finished(&mut self, op: Uuid, node: NodeId, outcome: Result<(), String>) {
        self.finishing.remove(&op);
        match outcome {
            Ok(()) => {
                event!(Level::INFO, msg = "a member was removed from the cluster", %node, plan = %op);
                let done = self.machine.state().plans.get(&op).map(PlanRecord::completed);
                if let Some(done) = done {
                    self.propose_plan(op, PlanUpdate::Done(done));
                }
            }
            Err(reason) => {
                event!(Level::WARN, msg = "a member could not be removed from the control group yet", %node, plan = %op, reason);
                // the reason is visible on the record until the next look succeeds
                let same = self
                    .machine
                    .state()
                    .plans
                    .get(&op)
                    .and_then(|record| record.blocked.as_ref().map(|blocked| blocked.reason == reason))
                    .unwrap_or(false);
                if !same {
                    self.propose_plan(op, PlanUpdate::Blocked(Some(reason)));
                }
            }
        }
    }
}

/// What a write through this node's own group produced when it did not commit here
enum LocalWrite {
    /// The group said where the leader is, if it knows
    Forward(Option<MemberRecord>),
    /// The write failed for a reason asking again cannot mend
    Failed(String),
}

/// Write a command through this node's own group, waiting out a lease that is not established
///
/// A leader answers a write with an empty forward hint until a quorum has acknowledged it,
/// which is when its lease starts; the metrics name it as the leader all the while, and a node
/// that was leading when it stopped comes back that way too. Taking the empty hint as "no
/// leader" and asking again at once is a busy loop on the control core that starves the links
/// the acknowledgements ride, so this polls at [`LEASE_POLL`] and leaves the deadline to the
/// caller.
///
/// # Arguments
///
/// * `raft` - This node's group
/// * `me` - This node
/// * `command` - The command
///
/// # Errors
///
/// Says where to forward the write, or why it cannot be written at all.
async fn write_here(
    raft: &Raft<ControlConfig, ControlStateMachine>,
    me: NodeId,
    command: ControlCommand,
) -> Result<ControlResponse, LocalWrite> {
    loop {
        match raft.client_write(command.clone()).await {
            Ok(response) => return Ok(response.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                // a hint naming this node, or none while the metrics name it, is a lease that
                // has not started: wait for it rather than spin
                let names_me = forward.leader_id == Some(me)
                    || (forward.leader_id.is_none()
                        && raft.metrics().borrow_watched().current_leader == Some(me));
                if names_me {
                    glommio::timer::sleep(LEASE_POLL).await;
                    continue;
                }
                return Err(LocalWrite::Forward(forward.leader_node));
            }
            Err(error) => {
                return Err(LocalWrite::Failed(format!(
                    "writing to the control log: {error}"
                )))
            }
        }
    }
}

/// Write a command through the leader, wherever it is
///
/// This node's own group takes it if this node leads; otherwise it is forwarded to the leader
/// the write named, or to the one the metrics name after a bounded wait, and the answer comes
/// back as what the state machine produced. Everything is under one deadline.
///
/// # Arguments
///
/// * `raft` - This node's group
/// * `network` - The control network
/// * `machine` - The state machine, for the leader's record
/// * `command` - The command
///
/// # Errors
///
/// Says whether no leader could be reached or the write failed some other way.
pub async fn propose(
    raft: &Raft<ControlConfig, ControlStateMachine>,
    network: &PeerNetwork,
    machine: &ControlStateMachine,
    command: ControlCommand,
) -> Result<ControlResponse, ProposeError> {
    let me = network.local_node();
    let attempt = async {
        // the local group first, which waits out its own lease if this node leads
        let mut hint = match write_here(raft, me, command.clone()).await {
            Ok(response) => return Ok(response),
            Err(LocalWrite::Forward(hint)) => hint,
            Err(LocalWrite::Failed(msg)) => return Err(ProposeError::Failed(msg)),
        };
        let payload = serde_json::to_vec(&command)
            .map_err(|error| ProposeError::Failed(format!("encoding a proposal: {error}")))?;
        // a hint may name a leader that has just changed, so this follows a few of them
        for _ in 0..PROPOSE_HOPS {
            // no hint: the leader the metrics name once the group elects one
            let Some(record) = hint.take().or_else(|| leader_record(raft, machine)) else {
                let elected = raft
                    .wait(Some(Duration::from_secs(3)))
                    .metrics(|metrics| metrics.current_leader.is_some(), "a leader")
                    .await;
                match elected {
                    Ok(_) => {
                        hint = leader_record(raft, machine);
                        if hint.is_none() {
                            return Err(ProposeError::NoLeader);
                        }
                        continue;
                    }
                    Err(_) => return Err(ProposeError::NoLeader),
                }
            };
            // the leader may be this node by now, in which case its own group takes the write
            // or says where to go next
            if record.node == me {
                match write_here(raft, me, command.clone()).await {
                    Ok(response) => return Ok(response),
                    Err(LocalWrite::Forward(next)) => {
                        hint = next.filter(|next| next.node != me);
                        continue;
                    }
                    Err(LocalWrite::Failed(msg)) => return Err(ProposeError::Failed(msg)),
                }
            }
            let peer = network.peer(&network.addr_of(&record));
            match peer.rpc(ControlKind::Propose, payload.clone(), PROPOSE_TIMEOUT).await {
                Ok(answer) => match serde_json::from_slice::<ProposeResponse>(&answer) {
                    Ok(ProposeResponse::Applied(response)) => return Ok(response),
                    Ok(ProposeResponse::NotLeader { leader: next }) => {
                        // an answer naming nobody is a leader in flux: a moment before asking
                        // the metrics again, so a change of leader is not chased at full speed
                        if next.is_none() {
                            glommio::timer::sleep(LEASE_POLL).await;
                        }
                        hint = next.filter(|next| next.node != me).or_else(|| {
                            leader_record(raft, machine).filter(|record| record.node != me)
                        });
                        // a hint naming this node goes through its own group, above
                        if hint.is_none() && raft.metrics().borrow_watched().current_leader == Some(me) {
                            hint = machine.state().members.get(&me).map(|member| member.record.clone());
                        }
                    }
                    Err(error) => {
                        return Err(ProposeError::Failed(format!("decoding a proposal's answer: {error}")))
                    }
                },
                Err(RpcFailure::Remote(msg)) => return Err(ProposeError::Failed(msg)),
                Err(RpcFailure::Unreachable(_)) => return Err(ProposeError::NoLeader),
            }
        }
        Err(ProposeError::NoLeader)
    };
    match glommio::timer::timeout(PROPOSE_TIMEOUT, async { Ok(attempt.await) }).await {
        Ok(outcome) => outcome,
        Err(_) => Err(ProposeError::NoLeader),
    }
}

/// The record of the leader the metrics name, if they name one this node knows
///
/// The membership the group holds is asked first, then the committed state, since a member's
/// record can be in either before it is in both.
///
/// # Arguments
///
/// * `raft` - This node's group
/// * `machine` - The state machine
fn leader_record(
    raft: &Raft<ControlConfig, ControlStateMachine>,
    machine: &ControlStateMachine,
) -> Option<MemberRecord> {
    let metrics = raft.metrics();
    let metrics = metrics.borrow_watched();
    let id = metrics.current_leader?;
    metrics
        .membership_config
        .membership()
        .get_node(&id)
        .cloned()
        .or_else(|| machine.state().members.get(&id).map(|member| member.record.clone()))
}

/// Catch a learner up and make it a voter
///
/// # Arguments
///
/// * `raft` - This node's group, which has to lead
/// * `learner` - The learner
/// * `record` - Its committed record
async fn promote(
    raft: &Raft<ControlConfig, ControlStateMachine>,
    learner: NodeId,
    record: MemberRecord,
) -> Result<(), String> {
    // re-adding a learner with `blocking` waits until it has the leader's log, which is the
    // catch-up before promotion C3 requires
    let caught_up = glommio::timer::timeout(CATCHUP_TIMEOUT, async {
        Ok(raft.add_learner(learner, record, true).await)
    })
    .await;
    match caught_up {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => return Err(format!("catching the learner up: {error}")),
        Err(_) => return Err("the learner did not catch up within the deadline".to_string()),
    }
    // then the configuration change, through joint consensus, keeping the others as they are
    let mut ids = BTreeSet::new();
    ids.insert(learner);
    raft.change_membership(ChangeMembers::AddVoterIds(ids), true)
        .await
        .map(|_| ())
        .map_err(|error| format!("changing the membership: {error}"))
}

/// Dial the seeds until one of them, or the leader it names, admits this node
///
/// # Arguments
///
/// * `network` - The control network
/// * `seeds` - The control addresses to try, in order
/// * `member` - What this node advertises
/// * `schema_id` - The structural fingerprint of the schema it serves
/// * `transport` - The bounds and timers, for the backoff
async fn join(
    network: &PeerNetwork,
    seeds: &[String],
    member: MemberRecord,
    schema_id: u64,
    transport: &Transport,
) -> Result<(ClusterId, NodeId), String> {
    let deadline = Instant::now() + JOIN_TIMEOUT;
    let request = serde_json::to_vec(&JoinRequest {
        member: member.clone(),
        schema_id,
    })
    .map_err(|error| format!("encoding a join: {error}"))?;
    let mut backoff = transport.reconnect_min.duration();
    let mut last = String::from("no seed answered");
    while Instant::now() < deadline {
        // every seed in order, then whoever a seed redirected to
        let mut targets: Vec<PeerAddr> = seeds.iter().map(|seed| PeerAddr::seed(seed)).collect();
        while let Some(target) = targets.first().cloned() {
            targets.remove(0);
            let peer = network.peer(&target);
            match peer.rpc(ControlKind::Join, request.clone(), JOIN_RPC_TIMEOUT).await {
                Ok(answer) => match serde_json::from_slice::<JoinResponse>(&answer) {
                    Ok(JoinResponse::Admitted { cluster, leader, .. }) => return Ok((cluster, leader)),
                    Ok(JoinResponse::Redirect { leader: Some(record) }) => {
                        // a seed that is not the leader names it; dial it next, expecting it
                        let mut addr = network.addr_of(&record);
                        addr.node = None;
                        addr.shards = 0;
                        targets.insert(0, addr);
                    }
                    Ok(JoinResponse::Redirect { leader: None }) => {
                        last = format!("{} knows no leader yet", target.control);
                    }
                    Ok(JoinResponse::Refused { reason, retry: true }) => {
                        last = format!("{} refused for now: {reason}", target.control);
                    }
                    Ok(JoinResponse::Refused { reason, retry: false }) => return Err(reason),
                    Err(error) => last = format!("decoding {}'s answer: {error}", target.control),
                },
                Err(error) => last = format!("{}: {error}", target.control),
            }
        }
        // nobody admitted us this round; back off and try again
        glommio::timer::sleep(backoff).await;
        backoff = (backoff * 2).min(transport.reconnect_max.duration());
    }
    Err(format!("no seed admitted this node within {JOIN_TIMEOUT:?}: {last}"))
}

/// Record a topology version in the marker, off the executor thread
///
/// The marker write is blocking IO with an fsync in it, and the executor's timers are what the
/// group's heartbeats run on, so it goes to a blocking thread.
///
/// # Arguments
///
/// * `root` - The storage root the marker is in
/// * `version` - The version observed
async fn observe(root: &Path, version: u64) -> Result<(), ServerError> {
    let root = root.to_path_buf();
    glommio::executor()
        .spawn_blocking(move || StorageMeta::observe_topology(&root, version))
        .await
}

/// The applied state, for a caller that holds only the machine
///
/// # Arguments
///
/// * `machine` - The state machine
#[must_use]
pub fn state_of(machine: &ControlStateMachine) -> ControlState {
    machine.state()
}
