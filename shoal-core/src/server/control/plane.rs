//! The control thread: one pinned executor, one group, and the pool's handle to both
//!
//! [`ControlPlane::start`] spawns a thread, pins it to the control core, builds a glommio
//! executor on it, opens the store, and runs the group. On a directory that has never been
//! bootstrapped it initializes the group with itself as the only member, writes the
//! [`ControlCommand::Bootstrap`] that creates the cluster, and writes an
//! [`ControlCommand::ObserveMember`] saying where it is; on a restart it recovers the log and
//! the applied state and writes only the observation, since an address may have changed. Either
//! way it reports [`ControlEvent::Ready`] once the group has a leader and the writes are
//! committed, and every applied entry that moved the topology version is recorded in the
//! storage marker through [`StorageMeta::observe_topology`], on a blocking thread so the
//! executor's timers keep running.
//!
//! The [`Raft`] handle never leaves this thread. The pool talks to it over a channel, and the
//! only things it asks are for the topology and for a shutdown. That is the seam
//! [C1](../../../../docs/src/distributed/node-identity.md) asks for: a shard never waits on the
//! control plane, and the control plane never touches a shard.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use glommio::{LocalExecutorBuilder, Placement};
use openraft::{Config, Raft};
use serde::{Deserialize, Serialize};
use tracing::{event, instrument, Level};

use super::cores::ControlPlacement;
use super::network::UnreachableNetwork;
use super::store::{self, ControlStateMachine, CONTROL_DIR};
use super::types::{ControlCommand, ControlConfig, ControlResponse, ControlState, MemberRecord};
use crate::server::conf::cluster::BootstrapPolicy;
use crate::server::conf::Conf;
use crate::server::errors::ShoalError;
use crate::server::meta::{Identity, StorageMeta};
use crate::server::ServerError;
use crate::shared::identity::{ClusterId, NodeId};

/// How long the control plane waits for its group to elect a leader
///
/// A group of one elects itself on its first tick, so this is a bound on a broken runtime
/// rather than on an election.
const LEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// What the control plane tells the pool
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEvent {
    /// The group has a leader and the bootstrap or recovery is committed
    Ready,
    /// The control plane failed, before or after it was ready
    Failed(String),
}

/// What the pool asks the control plane
enum ControlRequest {
    /// Describe the cluster as this node sees it
    Topology(mpsc::Sender<TopologyView>),
    /// Stop the group and exit the thread
    Shutdown,
}

/// The cluster as one node sees it
///
/// Built from the applied state on request, so it is what the committed log says and never a
/// cached copy of it. `active_rf` is the replicas the members could give, not the replicas any
/// tablet has: nothing places tablets yet, and this view is what says so rather than hiding it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopologyView {
    /// The cluster
    pub cluster: ClusterId,
    /// The node reporting
    pub node: NodeId,
    /// How many committed changes the topology has seen
    pub version: u64,
    /// Every member the group knows, in node order
    pub members: Vec<MemberRecord>,
    /// The replication factor the policy asks for
    pub desired_rf: u32,
    /// The replication factor the members can give
    pub active_rf: u32,
    /// How many replicas short of the policy the cluster is
    pub missing_replicas: u32,
    /// The cpu this node's control thread runs on
    pub control_core: usize,
    /// Whether that cpu's physical core is shared with a shard
    pub control_shared: bool,
    /// The policy the cluster was bootstrapped with
    pub policy: Option<BootstrapPolicy>,
}

impl TopologyView {
    /// Build the view from the applied state
    ///
    /// # Arguments
    ///
    /// * `state` - The applied state
    /// * `node` - The node reporting
    /// * `placement` - Where its control thread runs
    fn from_state(state: &ControlState, node: NodeId, placement: &ControlPlacement) -> Self {
        let desired_rf = state.desired_rf();
        let active_rf = state.active_rf();
        TopologyView {
            // a view is only ever built after the bootstrap, so the cluster exists
            cluster: state.cluster.unwrap_or_default(),
            node,
            version: state.topology_version,
            members: state.members.values().cloned().collect(),
            desired_rf,
            active_rf,
            missing_replicas: desired_rf.saturating_sub(active_rf),
            control_core: placement.cpu,
            control_shared: placement.shared,
            policy: state.policy.clone(),
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
    /// Where events go
    events: mpsc::Sender<ControlEvent>,
    /// Where requests come from
    requests: kanal::Receiver<ControlRequest>,
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
        };
        let root = conf
            .storage
            .default
            .filesystem
            .latency_sensitive
            .path
            .clone();
        // the two channels: events up to the pool, requests down to the thread
        let (events_tx, events) = mpsc::channel();
        let (requests_tx, requests_rx) = kanal::bounded(16);
        let startup = Startup {
            root,
            identity,
            placement: placement.clone(),
            member,
            policy: cluster.policy(),
            events: events_tx,
            requests: requests_rx,
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

    /// Wait until the group is ready, or report that it failed
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

    /// The cluster as this node sees it
    ///
    /// # Errors
    ///
    /// Fails if the control thread is gone.
    pub fn topology(&self) -> Result<TopologyView, ServerError> {
        let (tx, rx) = mpsc::channel();
        self.requests
            .send(ControlRequest::Topology(tx))
            .map_err(|_| ServerError::ControlFailed {
                error: "the control thread is not answering".to_string(),
            })?;
        rx.recv_timeout(LEADER_TIMEOUT)
            .map_err(|_| ServerError::ControlFailed {
                error: "the control thread did not answer a topology request".to_string(),
            })
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

/// Everything the thread does, from open to shutdown
///
/// A failure anywhere before ready is reported as [`ControlEvent::Failed`] and returned; the
/// pool sees both, one through `ready` and one through `exit`.
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

/// Open the store, run the group, and answer requests until shutdown
///
/// # Arguments
///
/// * `startup` - What the thread was started with
async fn serve(startup: Startup) -> Result<(), ServerError> {
    let Startup {
        root,
        identity,
        placement,
        member,
        policy,
        events,
        requests,
    } = startup;
    let node = identity.node;
    // the store, recovered from whatever the directory holds
    let dir = root.join(CONTROL_DIR);
    let (log, machine) = store::open(&dir).await?;
    // the group's configuration: openraft's defaults, named after the cluster
    let cluster = identity
        .cluster
        .ok_or(ServerError::Shoal(ShoalError::NotClustered))?;
    let config = Config {
        cluster_name: cluster.to_string(),
        ..Config::default()
    }
    .validate()
    .map_err(|error| ServerError::ControlFailed {
        error: format!("openraft config: {error}"),
    })?;
    let raft = Raft::<ControlConfig, ControlStateMachine>::new(
        node,
        Arc::new(config),
        UnreachableNetwork,
        log,
        machine.clone(),
    )
    .await
    .map_err(|error| ServerError::ControlFailed {
        error: format!("starting the group: {error}"),
    })?;
    // a group that has never been initialized is initialized with this node as its one member
    let initialized = raft.is_initialized().await.map_err(|error| ServerError::ControlFailed {
        error: format!("{error}"),
    })?;
    if !initialized {
        let mut members = BTreeMap::new();
        members.insert(node, member.clone());
        raft.initialize(members)
            .await
            .map_err(|error| ServerError::ControlFailed {
                error: format!("initializing the group: {error}"),
            })?;
    }
    // a group of one elects itself; wait for it
    raft.wait(Some(LEADER_TIMEOUT))
        .current_leader(node, "the control group elects this node")
        .await
        .map_err(|error| ServerError::ControlFailed {
            error: format!("{error}"),
        })?;
    // record what recovery found, before anything new is written
    let recovered = machine.state();
    if recovered.topology_version > 0 {
        observe(&root, recovered.topology_version).await?;
    }
    // create the cluster if the directory was just claimed for one; refuse to run one whose
    // committed identity is not the marker's, since that is a directory copied between clusters
    match recovered.cluster {
        None => {
            let response = write(
                &raft,
                ControlCommand::Bootstrap {
                    cluster,
                    policy,
                    member: member.clone(),
                },
            )
            .await?;
            observe_response(&root, &response).await?;
        }
        Some(committed) if committed != cluster => {
            return Err(ServerError::Shoal(ShoalError::WrongCluster {
                found: committed,
                expected: Some(cluster),
            }));
        }
        Some(_) => {}
    }
    // and say where this node is now, whether or not that changed
    let response = write(&raft, ControlCommand::ObserveMember(member)).await?;
    observe_response(&root, &response).await?;
    event!(
        Level::INFO,
        msg = "Control plane ready",
        node = node.to_string(),
        cluster = cluster.to_string(),
        control_core = placement.cpu,
        control_shared = placement.shared,
        topology_version = machine.state().topology_version,
    );
    let _ = events.send(ControlEvent::Ready);
    // then answer the pool until it says stop
    let requests = requests.to_async();
    loop {
        match requests.recv().await {
            Ok(ControlRequest::Topology(reply)) => {
                let view = TopologyView::from_state(&machine.state(), node, &placement);
                let _ = reply.send(view);
            }
            // a shutdown, or a pool that dropped its handle, which is the same thing
            Ok(ControlRequest::Shutdown) | Err(_) => break,
        }
    }
    raft.shutdown().await.map_err(|error| ServerError::ControlFailed {
        error: format!("stopping the group: {error}"),
    })?;
    Ok(())
}

/// Write a command through the group and hand back what applying it produced
///
/// # Arguments
///
/// * `raft` - The group
/// * `command` - The command
async fn write(
    raft: &Raft<ControlConfig, ControlStateMachine>,
    command: ControlCommand,
) -> Result<ControlResponse, ServerError> {
    let written = raft
        .client_write(command)
        .await
        .map_err(|error| ServerError::ControlFailed {
            error: format!("writing to the control log: {error}"),
        })?;
    Ok(written.data)
}

/// Record the topology version a response reports, if it applied
///
/// # Arguments
///
/// * `root` - The storage root the marker is in
/// * `response` - What applying a command produced
async fn observe_response(root: &Path, response: &ControlResponse) -> Result<(), ServerError> {
    match response {
        ControlResponse::Applied { topology_version } => observe(root, *topology_version).await,
        // a refusal changed nothing, and a bootstrap refused is a directory that already was one
        ControlResponse::Refused { reason } => Err(ServerError::ControlFailed {
            error: format!("the control group refused a command: {reason}"),
        }),
    }
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
