//! The cluster fixture: real Shoal servers as child processes, with the handles a test needs
//!
//! [C11](../../../docs/src/distributed/testing.md) asks for nodes run as child processes with
//! independent directories, explicit core allocation, actual bound endpoints, readiness and
//! failure reported separately, cleanup that runs even when a test fails, and directed fault
//! controls. This is that. Since [F39](../../../docs/src/features/membership.md) a cluster is
//! a **membership** cluster: node zero bootstraps, every other node joins through node zero's
//! control address, and the placement of tablets over them is the explicit `INITIALIZE` the
//! builder sends once every node has joined - exactly what an operator does.
//!
//! Every test binary that uses this declares `mod cluster;` next to `mod utils;`, since the
//! children are re-executions of the test binary itself (the shape `ack_survives_sigkill` uses)
//! and the temp directories come from `utils::test_dir`.
//!
//! ```text
//! let cluster = Cluster::builder()
//!     .cluster(3, CoreClaim::Count(1))
//!     .lane_links(true)
//!     .start()
//!     .await?;
//! let addr = cluster.node(0).endpoints.client;          // what the child actually bound
//! cluster.control_link(1, 0).cut();                      // node 1 can no longer reach node 0's control lane
//! cluster.node(0).pause()?;                              // SIGSTOP, distinct from a cut
//! ```
//!
//! A `kill` is `SIGKILL`, and the fixture claims nothing about durability from one: the
//! process is gone, the page cache and the device are not (M0's exit rule, C11's "SIGKILL does
//! not model loss of OS/device caches").

#![allow(dead_code)]

pub mod cores;
pub mod link;
pub mod node;
pub mod ports;
pub mod schema;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tempfile::TempDir;

pub use cores::{Allocation, ClusterPlan, CoreClaim, Topology};
pub use link::{Link, LinkState};
pub use node::{
    ChildOverrides, ChildRequest, Endpoints, Node, NodeKind, StagedCluster, CHILD_ENV, FAILED_LINE,
    READY_LINE, REPLY_LINE,
};

/// What can go wrong starting or driving a cluster
#[derive(Debug)]
pub enum FixtureError {
    /// A child could not be spawned or signalled
    Io(std::io::Error),
    /// A child did not report ready in time; the string is the evidence gathered
    NotReady(String),
    /// A child reported that it failed
    ChildFailed(String),
    /// The core allocation could not be satisfied as claimed
    Allocation(String),
    /// A client the test built against a node failed
    Client(shoal::client::Errors),
}

impl From<std::io::Error> for FixtureError {
    /// An io error is a spawn or signal that failed
    fn from(error: std::io::Error) -> Self {
        FixtureError::Io(error)
    }
}

impl From<shoal::client::Errors> for FixtureError {
    /// A client error is a query a test sent through the fixture
    fn from(error: shoal::client::Errors) -> Self {
        FixtureError::Client(error)
    }
}

/// One node a test asks for
#[derive(Debug, Clone)]
pub struct NodeSpec {
    /// Whether it is a real server or a mock peer
    pub kind: NodeKind,
    /// The cores it should own
    pub cores: CoreClaim,
    /// The cpus it may run on at all, if the test narrows them below the process's own
    pub affinity: Option<Vec<usize>>,
    /// A storage marker to stage in its directory before it starts
    pub staged_marker: Option<String>,
}

/// How long a cluster waits for every child to report ready
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Describes a cluster before it is started
#[derive(Debug, Clone)]
pub struct ClusterBuilder {
    /// The nodes, in the order they get ids
    nodes: Vec<NodeSpec>,
    /// Whether to put a directed proxy between every ordered pair of nodes' client endpoints
    links: bool,
    /// The cores the test's own driver claims
    driver: CoreClaim,
    /// How long to wait for readiness
    ready_timeout: Duration,
    /// Whether the server nodes form one membership cluster
    ///
    /// Set by [`ClusterBuilder::cluster`]. At `start` the fixture mints the identities, reserves
    /// a peer and a control port for each node, stages node zero's marker with the cluster and
    /// every joiner's in the joining mode, and hands every joiner node zero's control address.
    membership: bool,
    /// Whether to put a directed proxy between every ordered pair of nodes, per lane
    ///
    /// When set with [`ClusterBuilder::cluster`], each node dials every other node's data and
    /// control lanes through a proxy of that direction's own, so a test can delay or cut one
    /// direction of one lane between two nodes while the others keep flowing.
    lane_links: bool,
    /// Whether each node exports its spans to a file, for the cross-node trace test
    trace: bool,
    /// The replication factor node zero seeds; one unless a test asks for the policy
    replication_factor: u32,
    /// The voter policy node zero seeds
    control_voters: u32,
    /// The principals allowed to change the cluster
    admins: Vec<String>,
    /// Users every node requires of a client
    auth: Vec<(String, String)>,
    /// The detector's report interval, if shortened
    detector_interval_ms: Option<u64>,
    /// Whether to place the tablets over every node once all have joined
    initialize: bool,
    /// Nodes at this index and above are not started until asked
    deferred_from: Option<usize>,
    /// The base data election timeout, in milliseconds
    failover_ms: u64,
    /// The durability one node's persistent table is configured with, by node index
    durability: Vec<(usize, String)>,
    /// The proposal deadline, in milliseconds, if shortened
    write_timeout_ms: Option<u64>,
    /// The bound on bytes proposed and unanswered per group, if lowered
    pending_bytes: Option<usize>,
    /// How many entries a group commits between snapshots, if shortened
    checkpoint_entries: Option<u64>,
    /// How many entries a group keeps behind its snapshot, if shortened
    retained_entries: Option<u64>,
    /// How large a WAL segment grows before it rotates, if shrunk
    segment_bytes: Option<u64>,
    /// The bound on sealed WAL bytes a slow member may pin, if lowered
    retained_bytes: Option<u64>,
    /// How often every group a node leads is scrubbed on its own, if set
    scrub_interval_ms: Option<u64>,
    /// How long one scrub may take, if shortened
    repair_timeout_ms: Option<u64>,
    /// How long one snapshot transfer may take, if shortened
    snapshot_timeout_ms: Option<u64>,
    /// How many bytes one snapshot chunk carries, if shrunk
    snapshot_chunk_bytes: Option<usize>,
    /// The bulk lane's queue bound, if shrunk
    bulk_queue_bytes: Option<usize>,
    /// The cluster's default read level, if set
    read_consistency: Option<String>,
    /// The bundle deadline, in milliseconds, if shortened
    query_deadline_ms: Option<u64>,
    /// How long a retired copy's files are kept, in milliseconds, if shortened
    retire_after_ms: Option<u64>,
    /// How far behind a learner may be when it is made a voter, if set
    catchup_lag: Option<u64>,
    /// How long one phase of a move may take, in milliseconds, if shortened
    migration_timeout_ms: Option<u64>,
    /// How long a write's identity may be retried within, in milliseconds, if shortened
    retry_window_ms: Option<u64>,
    /// The grace a down member is removed after, in milliseconds; none for the default,
    /// zero for never ([F46](../../../docs/src/features/capacity-rebalancing.md))
    auto_remove_after_ms: Option<u64>,
    /// Each node's placement weight, by node index, where a test set one
    weights: Vec<(usize, u32)>,
    /// Each node's stream budget, by node index: bytes per second and streams at a time
    stream_budgets: Vec<(usize, usize, u32)>,
    /// The disk reserve every node keeps, in bytes, if set
    disk_reserve: Option<u64>,
    /// How many moves one member is the source and destination of at a time, if set
    moves_per_node: Option<u32>,
    /// How often the control leader looks at its plans, in milliseconds, if shortened
    plan_interval_ms: Option<u64>,
    /// The slots particular nodes claim, apart from their cores
    /// ([F47](../../../docs/src/features/local-rehome.md))
    slots: Vec<(usize, usize)>,
    /// The wire version particular nodes are pinned at, below the build's newest
    /// ([F48](../../../docs/src/features/rolling-compatibility.md))
    wire_versions: Vec<(usize, u8)>,
    /// Whether the peer lanes are mutual TLS under a fixture authority, every leaf naming its
    /// node ([F50](../../../docs/src/features/cluster-operations.md))
    peer_tls: bool,
    /// Another build of the test binary to start every child from, for a real upgrade
    exe: Option<std::path::PathBuf>,
}

impl ClusterBuilder {
    /// Give one node a core claim of its own, after `cluster` gave every node the same
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    /// * `cores` - The cores it should own
    pub fn node_cores(mut self, id: usize, cores: CoreClaim) -> Self {
        if let Some(spec) = self.nodes.get_mut(id) {
            spec.cores = cores;
        }
        self
    }

    /// Have one node claim this many slots, which its cores must not exceed
    /// ([F47](../../../docs/src/features/local-rehome.md))
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    /// * `slots` - The slots
    pub fn slots(mut self, id: usize, slots: usize) -> Self {
        self.slots.retain(|(node, _)| *node != id);
        self.slots.push((id, slots));
        self
    }

    /// Pin one node at a wire version below the build's newest, as an unupgraded member
    /// ([F48](../../../docs/src/features/rolling-compatibility.md))
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    /// * `version` - The newest version it advertises
    pub fn wire_version(mut self, id: usize, version: u8) -> Self {
        self.wire_versions.retain(|(node, _)| *node != id);
        self.wire_versions.push((id, version));
        self
    }

    /// Start every child from another build of the test binary, for a real rolling upgrade
    /// ([F48](../../../docs/src/features/rolling-compatibility.md))
    ///
    /// # Arguments
    ///
    /// * `exe` - The binary
    pub fn exe(mut self, exe: std::path::PathBuf) -> Self {
        self.exe = Some(exe);
        self
    }

    /// Encrypt the peer lanes under a fixture authority, every node's leaf naming its node
    /// ([F50](../../../docs/src/features/cluster-operations.md))
    ///
    /// The authority and every leaf are minted at start and written under each node's
    /// directory; the cluster keeps the authority's key so a test can reissue a leaf or
    /// rotate the authority and ask the node to reload.
    pub fn peer_tls(mut self) -> Self {
        self.peer_tls = true;
        self
    }

    /// Add a real server, bootstrapped as a cluster of one
    ///
    /// # Arguments
    ///
    /// * `cores` - The cores it should own
    pub fn server(mut self, cores: CoreClaim) -> Self {
        self.nodes.push(NodeSpec {
            kind: NodeKind::Server,
            cores,
            affinity: None,
            staged_marker: None,
        });
        self
    }

    /// Have each node export its spans to `<dir>/trace.jsonl`, for the cross-node trace test
    pub fn trace(mut self) -> Self {
        self.trace = true;
        self
    }

    /// Put a directed proxy between every ordered pair of nodes, on the data and control lanes
    ///
    /// # Arguments
    ///
    /// * `lane_links` - Yes or no
    pub fn lane_links(mut self, lane_links: bool) -> Self {
        self.lane_links = lane_links;
        self
    }

    /// Add `n` servers that form one membership cluster of `n` nodes
    ///
    /// Each gets `cores` cores. Node zero bootstraps; the rest join through it. A cluster of
    /// one is the same as a single `server`, with the membership machinery around it.
    ///
    /// # Arguments
    ///
    /// * `n` - How many nodes
    /// * `cores` - The cores each should own
    pub fn cluster(mut self, n: usize, cores: CoreClaim) -> Self {
        for _ in 0..n {
            self.nodes.push(NodeSpec {
                kind: NodeKind::Server,
                cores: cores.clone(),
                affinity: None,
                staged_marker: None,
            });
        }
        self.membership = true;
        self
    }

    /// The replication factor node zero seeds
    ///
    /// # Arguments
    ///
    /// * `replication_factor` - The factor
    pub fn replication_factor(mut self, replication_factor: u32) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    /// The voter policy node zero seeds
    ///
    /// # Arguments
    ///
    /// * `control_voters` - One, three or five
    pub fn control_voters(mut self, control_voters: u32) -> Self {
        self.control_voters = control_voters;
        self
    }

    /// The principals allowed to change the cluster
    ///
    /// # Arguments
    ///
    /// * `admins` - Their names
    pub fn admins(mut self, admins: Vec<String>) -> Self {
        self.admins = admins;
        self
    }

    /// Require every client to authenticate as one of these users
    ///
    /// # Arguments
    ///
    /// * `user` - The name
    /// * `password` - The password
    pub fn auth(mut self, user: &str, password: &str) -> Self {
        self.auth.push((user.to_string(), password.to_string()));
        self
    }

    /// Shorten the detector's report interval
    ///
    /// # Arguments
    ///
    /// * `interval_ms` - The interval
    pub fn detector_interval_ms(mut self, interval_ms: u64) -> Self {
        self.detector_interval_ms = Some(interval_ms);
        self
    }

    /// Whether to place the tablets over every node once all have joined
    ///
    /// # Arguments
    ///
    /// * `initialize` - Yes or no; yes is the default
    pub fn initialize(mut self, initialize: bool) -> Self {
        self.initialize = initialize;
        self
    }

    /// Set the base data election timeout, which the groups' heartbeat is a tenth of
    ///
    /// # Arguments
    ///
    /// * `failover` - The base
    pub fn primary_failover_after(mut self, failover: Duration) -> Self {
        self.failover_ms = u64::try_from(failover.as_millis()).unwrap_or(u64::MAX);
        self
    }

    /// Configure one node's persistent table with a durability, `"fsync"` or `"async"`
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    /// * `durability` - The durability
    pub fn durability(mut self, id: usize, durability: &str) -> Self {
        self.durability.push((id, durability.to_string()));
        self
    }

    /// Shorten the proposal deadline
    ///
    /// # Arguments
    ///
    /// * `timeout` - The deadline
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout_ms = Some(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX));
        self
    }

    /// Seed the cluster's default read level, `one` or `quorum`
    /// ([F41](../../../docs/src/features/read-consistency.md))
    ///
    /// # Arguments
    ///
    /// * `level` - The level's name
    #[must_use]
    pub fn read_consistency(mut self, level: &str) -> Self {
        self.read_consistency = Some(level.to_string());
        self
    }

    /// Shorten every node's bundle deadline
    ///
    /// # Arguments
    ///
    /// * `deadline` - The budget a bundle gets
    #[must_use]
    pub fn query_deadline(mut self, deadline: Duration) -> Self {
        self.query_deadline_ms = Some(u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX));
        self
    }

    /// Lower the bound on bytes proposed and unanswered per group
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bound
    pub fn pending_bytes(mut self, bytes: usize) -> Self {
        self.pending_bytes = Some(bytes);
        self
    }

    /// Shorten how many entries a group commits between snapshots
    /// ([F43](../../../docs/src/features/node-recovery.md))
    ///
    /// # Arguments
    ///
    /// * `entries` - The count
    #[must_use]
    pub fn checkpoint_entries(mut self, entries: u64) -> Self {
        self.checkpoint_entries = Some(entries);
        self
    }

    /// Scrub every group a node leads on an interval, without an operator
    /// ([F44](../../../docs/src/features/repair.md))
    ///
    /// # Arguments
    ///
    /// * `interval` - The interval
    pub fn scrub_interval(mut self, interval: Duration) -> Self {
        self.scrub_interval_ms = Some(interval.as_millis() as u64);
        self
    }

    /// Shorten how long one scrub may take
    ///
    /// # Arguments
    ///
    /// * `timeout` - The deadline
    pub fn repair_timeout(mut self, timeout: Duration) -> Self {
        self.repair_timeout_ms = Some(timeout.as_millis() as u64);
        self
    }

    /// Shorten how long one snapshot transfer may take
    ///
    /// # Arguments
    ///
    /// * `timeout` - The deadline
    pub fn snapshot_timeout(mut self, timeout: Duration) -> Self {
        self.snapshot_timeout_ms = Some(timeout.as_millis() as u64);
        self
    }

    /// Shorten how long a retired copy's files are kept
    /// ([F45](../../../docs/src/features/replica-migration.md))
    ///
    /// # Arguments
    ///
    /// * `grace` - The grace
    pub fn retire_after(mut self, grace: Duration) -> Self {
        self.retire_after_ms = Some(grace.as_millis() as u64);
        self
    }

    /// Set how far behind a learner may be when it is made a voter
    ///
    /// # Arguments
    ///
    /// * `entries` - The lag
    pub fn catchup_lag(mut self, entries: u64) -> Self {
        self.catchup_lag = Some(entries);
        self
    }

    /// Shorten how long one phase of a move may take
    ///
    /// # Arguments
    ///
    /// * `timeout` - The deadline
    pub fn migration_timeout(mut self, timeout: Duration) -> Self {
        self.migration_timeout_ms = Some(timeout.as_millis() as u64);
        self
    }

    /// Shorten how long a write's identity may be retried within
    /// ([F45](../../../docs/src/features/replica-migration.md))
    ///
    /// # Arguments
    ///
    /// * `window` - The window
    pub fn retry_window(mut self, window: Duration) -> Self {
        self.retry_window_ms = Some(window.as_millis() as u64);
        self
    }

    /// Set the grace a down member is removed after, or none to never remove one
    /// ([F46](../../../docs/src/features/capacity-rebalancing.md))
    ///
    /// # Arguments
    ///
    /// * `grace` - The grace, or none for never
    pub fn auto_remove_after(mut self, grace: Option<Duration>) -> Self {
        self.auto_remove_after_ms = Some(grace.map_or(0, |grace| grace.as_millis() as u64));
        self
    }

    /// Set one node's placement weight
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    /// * `weight` - Its weight
    pub fn weight(mut self, id: usize, weight: u32) -> Self {
        self.weights.push((id, weight));
        self
    }

    /// Bound one node's snapshot streams: bytes per second sent, and streams installed at a time
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    /// * `bytes_per_sec` - The byte budget every stream it sends draws on
    /// * `streams` - How many streams one of its shards installs at a time
    pub fn stream_budget(mut self, id: usize, bytes_per_sec: usize, streams: u32) -> Self {
        self.stream_budgets.push((id, bytes_per_sec, streams));
        self
    }

    /// Set the disk reserve every node keeps
    ///
    /// # Arguments
    ///
    /// * `bytes` - The reserve
    pub fn disk_reserve(mut self, bytes: u64) -> Self {
        self.disk_reserve = Some(bytes);
        self
    }

    /// Set how many moves one member is the source and destination of at a time
    ///
    /// # Arguments
    ///
    /// * `moves` - The cap
    pub fn moves_per_node(mut self, moves: u32) -> Self {
        self.moves_per_node = Some(moves);
        self
    }

    /// Shorten how often the control leader looks at its plans
    ///
    /// # Arguments
    ///
    /// * `interval` - The interval
    pub fn plan_interval(mut self, interval: Duration) -> Self {
        self.plan_interval_ms = Some(interval.as_millis() as u64);
        self
    }

    /// Shorten how many entries a group keeps behind its snapshot
    ///
    /// # Arguments
    ///
    /// * `entries` - The count
    #[must_use]
    pub fn retained_entries(mut self, entries: u64) -> Self {
        self.retained_entries = Some(entries);
        self
    }

    /// Shrink how large a WAL segment grows before it rotates
    ///
    /// # Arguments
    ///
    /// * `bytes` - The size
    #[must_use]
    pub fn segment_bytes(mut self, bytes: u64) -> Self {
        self.segment_bytes = Some(bytes);
        self
    }

    /// Lower the bound on sealed WAL bytes a slow member may pin
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bound
    #[must_use]
    pub fn retained_bytes(mut self, bytes: u64) -> Self {
        self.retained_bytes = Some(bytes);
        self
    }

    /// Shrink how many bytes one snapshot chunk carries, so a stream is many chunks
    ///
    /// # Arguments
    ///
    /// * `bytes` - The chunk size
    #[must_use]
    pub fn snapshot_chunk_bytes(mut self, bytes: usize) -> Self {
        self.snapshot_chunk_bytes = Some(bytes);
        self
    }

    /// Shrink the bulk lane's queue bound, so a sender waits for room chunk by chunk
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bound
    #[must_use]
    pub fn bulk_queue_bytes(mut self, bytes: usize) -> Self {
        self.bulk_queue_bytes = Some(bytes);
        self
    }

    /// Leave the nodes at this index and above unstarted, for [`Cluster::start_deferred`]
    ///
    /// # Arguments
    ///
    /// * `from` - The first index not started
    pub fn deferred_from(mut self, from: usize) -> Self {
        self.deferred_from = Some(from);
        self
    }

    /// Add a real server with no `cluster:` block
    ///
    /// # Arguments
    ///
    /// * `cores` - The cores it should own
    pub fn standalone(mut self, cores: CoreClaim) -> Self {
        self.nodes.push(NodeSpec {
            kind: NodeKind::Standalone,
            cores,
            affinity: None,
            staged_marker: None,
        });
        self
    }

    /// Narrow the cpus the last node added may run on
    ///
    /// # Arguments
    ///
    /// * `cpus` - The cpus
    pub fn affinity(mut self, cpus: Vec<usize>) -> Self {
        let node = self.nodes.last_mut().expect("a node to narrow");
        node.affinity = Some(cpus);
        self
    }

    /// Stage a storage marker for the last node added to find when it starts
    ///
    /// # Arguments
    ///
    /// * `marker` - The marker's bytes, verbatim
    pub fn staged_marker(mut self, marker: impl Into<String>) -> Self {
        let node = self.nodes.last_mut().expect("a node to stage for");
        node.staged_marker = Some(marker.into());
        self
    }

    /// Add a mock peer: a process that listens and echoes, and nothing else
    ///
    /// # Arguments
    ///
    /// * `cores` - The cores it should own
    pub fn mock_peer(mut self, cores: CoreClaim) -> Self {
        self.nodes.push(NodeSpec {
            kind: NodeKind::MockPeer,
            cores,
            affinity: None,
            staged_marker: None,
        });
        self
    }

    /// Whether to build directed links between every ordered pair of nodes' client endpoints
    ///
    /// # Arguments
    ///
    /// * `links` - Yes or no
    pub fn links(mut self, links: bool) -> Self {
        self.links = links;
        self
    }

    /// What the driver claims for itself
    ///
    /// # Arguments
    ///
    /// * `cores` - The claim
    pub fn driver(mut self, cores: CoreClaim) -> Self {
        self.driver = cores;
        self
    }

    /// How long to wait for every child to report ready
    ///
    /// # Arguments
    ///
    /// * `timeout` - The wait
    pub fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// The core allocation this cluster would get, without starting anything
    ///
    /// # Arguments
    ///
    /// * `topology` - The machine to allocate on
    pub fn plan(&self, topology: &Topology) -> Result<ClusterPlan, FixtureError> {
        cores::allocate(&self.nodes, &self.driver, topology)
    }

    /// Start every child, wait for all of them, join them up, and place the tablets
    pub async fn start(self) -> Result<Cluster, FixtureError> {
        // allocate cores first, so a claim that cannot be met fails before a process exists
        let topology = Topology::detect();
        let mut plan = self.plan(&topology)?;
        // one directory per node, alive as long as the cluster
        let dirs: Vec<TempDir> = self
            .nodes
            .iter()
            .map(|_| crate::utils::test_dir())
            .collect();
        // a membership cluster needs its identities and ports decided before any child starts,
        // so node zero's marker names the cluster, every joiner's names its node, and every
        // joiner knows node zero's control address
        let mut staged = if self.membership {
            Some(build_membership_cluster(&self, &dirs, &plan)?)
        } else {
            None
        };
        // when lane links are asked for, put a proxy of its own between every ordered pair of
        // nodes on each lane, and tell each node to dial its peers through them
        let mut data_links: BTreeMap<(usize, usize), Link> = BTreeMap::new();
        let mut control_links: BTreeMap<(usize, usize), Link> = BTreeMap::new();
        if self.lane_links {
            if let Some(staged) = staged.as_mut() {
                let n = staged.per_node.len();
                for from in 0..n {
                    for to in 0..n {
                        if from == to {
                            continue;
                        }
                        let real_data: SocketAddr =
                            format!("127.0.0.1:{}", staged.per_node[to].data_port)
                                .parse()
                                .unwrap();
                        let real_control: SocketAddr =
                            format!("127.0.0.1:{}", staged.per_node[to].control_port)
                                .parse()
                                .unwrap();
                        let dlink = Link::start(real_data).await?;
                        let clink = Link::start(real_control).await?;
                        let peer = staged.per_node[to].node.clone();
                        staged.per_node[from].dial.push((
                            peer,
                            clink.addr().to_string(),
                            dlink.addr().to_string(),
                        ));
                        // a joiner's seed is node zero, reached through this direction's proxy
                        if to == 0 {
                            staged.per_node[from].seeds = vec![clink.addr().to_string()];
                        }
                        data_links.insert((from, to), dlink);
                        control_links.insert((from, to), clink);
                    }
                }
            }
        }
        // spawn everything that is not deferred, then wait for everything, so the children
        // start in parallel
        let deferred_from = self.deferred_from.unwrap_or(self.nodes.len());
        let mut nodes: Vec<Option<Node>> = Vec::with_capacity(self.nodes.len());
        for (id, (spec, dir)) in self.nodes.iter().zip(&dirs).enumerate() {
            if id >= deferred_from {
                nodes.push(None);
                continue;
            }
            let allocation = plan.nodes[id].1.clone();
            let cluster = staged
                .as_ref()
                .and_then(|staged| staged.per_node.get(id).cloned());
            let durability = self
                .durability
                .iter()
                .find(|(node, _)| *node == id)
                .map(|(_, durability)| durability.clone());
            nodes.push(Some(Node::spawn_with(
                id,
                spec.kind,
                allocation,
                dir.path(),
                spec.affinity.clone(),
                spec.staged_marker.clone(),
                cluster,
                durability,
                ChildOverrides {
                    exe: self.exe.clone(),
                    ..ChildOverrides::default()
                },
            )?));
        }
        plan.endpoints = vec![Endpoints::unbound(); self.nodes.len()];
        for (id, node) in nodes.iter_mut().enumerate() {
            if let Some(node) = node {
                // a node that never comes up takes the whole cluster down with it, and the
                // evidence: `Node`'s drop kills the rest
                node.wait_ready(self.ready_timeout)?;
                plan.endpoints[id] = node.endpoints.clone();
            }
        }
        // a directed link per ordered pair, each pointing at the target's client endpoint
        let mut links = BTreeMap::new();
        if self.links {
            for from in 0..nodes.len() {
                for to in 0..nodes.len() {
                    if from != to {
                        if let Some(target) = &nodes[to] {
                            let link = Link::start(target.endpoints.client).await?;
                            plan.proxies.push(((from, to), link.addr()));
                            links.insert((from, to), link);
                        }
                    }
                }
            }
        }
        let mut cluster = Cluster {
            nodes,
            links,
            data_links,
            control_links,
            plan,
            pki: staged.as_mut().and_then(|staged| staged.pki.take()),
            staged: staged.map(|staged| staged.per_node).unwrap_or_default(),
            ready_timeout: self.ready_timeout,
            _dirs: dirs,
        };
        // a membership cluster is not up until every started node has joined, and its tablets
        // are placed over every started node once they have
        if self.membership {
            let started: Vec<usize> = (0..deferred_from.min(cluster.nodes.len())).collect();
            cluster.wait_joined(&started)?;
            if self.initialize {
                cluster.initialize(&started)?;
            }
        }
        Ok(cluster)
    }
}

/// A running cluster
///
/// Dropping it kills and reaps every child and stops every proxy, whether the test passed or
/// panicked: cleanup is what a `Drop` is for.
pub struct Cluster {
    /// The nodes, by id; none for one not started yet
    nodes: Vec<Option<Node>>,
    /// The directed client links, by (from, to)
    links: BTreeMap<(usize, usize), Link>,
    /// The proxy each node dials another's data lane through, by (from, to)
    data_links: BTreeMap<(usize, usize), Link>,
    /// The proxy each node dials another's control lane through, by (from, to)
    control_links: BTreeMap<(usize, usize), Link>,
    /// What was allocated and bound
    plan: ClusterPlan,
    /// What each node was staged with, kept so a restart is the same node
    staged: Vec<StagedCluster>,
    /// How long to wait for a node
    ready_timeout: Duration,
    /// The fixture authority the peer lanes trust, if they are encrypted
    pki: Option<Pki>,
    /// The storage directories, dropped last
    _dirs: Vec<TempDir>,
}

/// The fixture's certificate authority: the one every node trusts, and the one before it
///
/// Kept with its key so a test can reissue a node's leaf, mint a second authority and write
/// a bundle of both, the way an operator rotates one
/// ([F50](../../../docs/src/features/cluster-operations.md)).
pub struct Pki {
    /// The current authority
    ca: rcgen::Certificate,
    /// Its key
    ca_key: rcgen::KeyPair,
    /// The authority before a rotation, kept while a bundle names both
    previous: Option<rcgen::Certificate>,
}

impl Pki {
    /// Mint an authority
    fn mint() -> Self {
        let ca_key = rcgen::KeyPair::generate().expect("a ca key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "shoal fixture authority");
        let ca = params.self_signed(&ca_key).expect("a ca certificate");
        Pki {
            ca,
            ca_key,
            previous: None,
        }
    }

    /// Issue a leaf for the loopback address, naming a node if one is given
    ///
    /// # Arguments
    ///
    /// * `node` - The node the leaf names through its `shoal-node://` URI, or none
    fn issue(&self, node: Option<&str>) -> (String, String) {
        let key = rcgen::KeyPair::generate().expect("a node key");
        let mut params =
            rcgen::CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
                .expect("leaf params");
        if let Some(node) = node {
            params.subject_alt_names.push(rcgen::SanType::URI(
                format!("shoal-node://{node}")
                    .try_into()
                    .expect("a uri san"),
            ));
        }
        let cert = params
            .signed_by(&key, &self.ca, &self.ca_key)
            .expect("a node certificate");
        (cert.pem(), key.serialize_pem())
    }

    /// The authority bundle: the current one, and the previous one while it is kept
    fn bundle(&self) -> String {
        let mut pem = self.ca.pem();
        if let Some(previous) = &self.previous {
            pem.push_str(&previous.pem());
        }
        pem
    }
}

/// Where a node's peer TLS files go under its directory
///
/// # Arguments
///
/// * `dir` - The node's directory
fn tls_paths(dir: &std::path::Path) -> node::StagedTls {
    let tls = dir.join("tls");
    node::StagedTls {
        cert: tls.join("node.pem").to_string_lossy().into_owned(),
        key: tls.join("node.key").to_string_lossy().into_owned(),
        ca: tls.join("ca.pem").to_string_lossy().into_owned(),
        bind_identity: true,
    }
}

impl Cluster {
    /// Describe a cluster
    pub fn builder() -> ClusterBuilder {
        ClusterBuilder {
            nodes: Vec::new(),
            links: false,
            driver: CoreClaim::Shared,
            ready_timeout: DEFAULT_READY_TIMEOUT,
            membership: false,
            lane_links: false,
            trace: false,
            replication_factor: 1,
            control_voters: 3,
            admins: Vec::new(),
            auth: Vec::new(),
            detector_interval_ms: None,
            initialize: true,
            deferred_from: None,
            failover_ms: 1000,
            durability: Vec::new(),
            write_timeout_ms: None,
            pending_bytes: None,
            checkpoint_entries: None,
            retained_entries: None,
            segment_bytes: None,
            retained_bytes: None,
            scrub_interval_ms: None,
            repair_timeout_ms: None,
            retire_after_ms: None,
            catchup_lag: None,
            migration_timeout_ms: None,
            retry_window_ms: None,
            auto_remove_after_ms: None,
            weights: Vec::new(),
            stream_budgets: Vec::new(),
            disk_reserve: None,
            moves_per_node: None,
            plan_interval_ms: None,
            slots: Vec::new(),
            wire_versions: Vec::new(),
            peer_tls: false,
            exe: None,
            snapshot_timeout_ms: None,
            snapshot_chunk_bytes: None,
            bulk_queue_bytes: None,
            read_consistency: None,
            query_deadline_ms: None,
        }
    }

    /// A node
    ///
    /// # Arguments
    ///
    /// * `id` - Its id, from zero in the order the builder added it
    pub fn node(&self, id: usize) -> &Node {
        self.nodes[id]
            .as_ref()
            .unwrap_or_else(|| panic!("node {id} is not started"))
    }

    /// A node, to signal
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    pub fn node_mut(&mut self, id: usize) -> &mut Node {
        self.nodes[id]
            .as_mut()
            .unwrap_or_else(|| panic!("node {id} is not started"))
    }

    /// Whether a node is running
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    pub fn is_started(&self, id: usize) -> bool {
        self.nodes[id].as_ref().is_some_and(Node::is_alive)
    }

    /// How many nodes
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether there are none
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The directed client link from one node to another
    ///
    /// # Arguments
    ///
    /// * `from` - The sending node
    /// * `to` - The receiving node
    pub fn link(&self, from: usize, to: usize) -> &Link {
        self.links.get(&(from, to)).unwrap_or_else(|| {
            panic!("no link from {from} to {to}; was the cluster built with links?")
        })
    }

    /// The proxy one node dials another's data lane through, for a test to delay or cut
    ///
    /// # Arguments
    ///
    /// * `from` - The dialling node
    /// * `to` - The node whose data lane it dials
    pub fn data_link(&self, from: usize, to: usize) -> &Link {
        self.data_links.get(&(from, to)).unwrap_or_else(|| {
            panic!("no data link from {from} to {to}; was the cluster built with lane links?")
        })
    }

    /// The proxy one node dials another's control lane through
    ///
    /// # Arguments
    ///
    /// * `from` - The dialling node
    /// * `to` - The node whose control lane it dials
    pub fn control_link(&self, from: usize, to: usize) -> &Link {
        self.control_links.get(&(from, to)).unwrap_or_else(|| {
            panic!("no control link from {from} to {to}; was the cluster built with lane links?")
        })
    }

    /// Every proxy into a node's data lane, from every other node
    ///
    /// # Arguments
    ///
    /// * `to` - The node
    pub fn data_links_into(&self, to: usize) -> Vec<&Link> {
        self.data_links
            .iter()
            .filter(|((_, target), _)| *target == to)
            .map(|(_, link)| link)
            .collect()
    }

    /// Every proxy into a node's control lane, from every other node
    ///
    /// # Arguments
    ///
    /// * `to` - The node
    pub fn control_links_into(&self, to: usize) -> Vec<&Link> {
        self.control_links
            .iter()
            .filter(|((_, target), _)| *target == to)
            .map(|(_, link)| link)
            .collect()
    }

    /// Cut every lane in both directions between a node and every other, isolating it
    ///
    /// # Arguments
    ///
    /// * `id` - The node to isolate
    pub fn isolate(&self, id: usize) {
        for ((from, to), link) in self.control_links.iter().chain(self.data_links.iter()) {
            if *from == id || *to == id {
                link.cut();
            }
        }
    }

    /// Heal every lane in both directions between a node and every other
    ///
    /// # Arguments
    ///
    /// * `id` - The node to reconnect
    pub fn heal(&self, id: usize) {
        for ((from, to), link) in self.control_links.iter().chain(self.data_links.iter()) {
            if *from == id || *to == id {
                link.heal();
            }
        }
    }

    /// What was allocated and bound
    pub fn plan(&self) -> &ClusterPlan {
        &self.plan
    }

    /// Every running child's pid, for a test to check they are gone afterwards
    pub fn pids(&self) -> Vec<u32> {
        self.nodes.iter().flatten().map(|node| node.pid).collect()
    }

    /// A node's storage directory
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    pub fn dir(&self, id: usize) -> &std::path::Path {
        self._dirs[id].path()
    }

    /// What a node was staged with
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    pub fn staged(&self, id: usize) -> &StagedCluster {
        &self.staged[id]
    }

    /// Kill a node and start it again on the same directory, with the same allocation
    ///
    /// The kill is `SIGKILL`, so nothing the node had not made durable survives; what does is
    /// what the restart reports. A cluster node comes back with everything it was staged with -
    /// its ports, its seeds, its proxies - so it is the same node to its peers.
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `kind` - What to restart it as, which may differ from what it was
    pub fn restart(&mut self, id: usize, kind: NodeKind) -> Result<(), FixtureError> {
        let staged = self.staged.get(id).cloned();
        self.restart_with(id, kind, staged)
    }

    /// Kill a node and start it again with a changed staging
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `kind` - What to restart it as
    /// * `staged` - What to stage it with, or none for a plain server
    pub fn restart_with(
        &mut self,
        id: usize,
        kind: NodeKind,
        staged: Option<StagedCluster>,
    ) -> Result<(), FixtureError> {
        self.restart_with_overrides(id, kind, staged, ChildOverrides::default())
    }

    /// Kill a node and start it again with a changed staging and a changed core count or crash point
    ///
    /// The node keeps its core lease and runs `overrides.cores` executors on it, which is how
    /// a test drives the rehome ([F47](../../../docs/src/features/local-rehome.md)); a rehome
    /// crash point is armed before the pool starts. A node that dies at the point is left dead
    /// rather than waited for, so the caller waits for its death and starts it again.
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `kind` - What to restart it as
    /// * `staged` - What to stage it with, or none for a plain server
    /// * `overrides` - The core count and the crash point
    pub fn restart_with_overrides(
        &mut self,
        id: usize,
        kind: NodeKind,
        staged: Option<StagedCluster>,
        overrides: ChildOverrides,
    ) -> Result<(), FixtureError> {
        if let Some(node) = self.nodes[id].as_mut() {
            node.kill()?;
        }
        let allocation = self.plan.nodes[id].1.clone();
        let armed = overrides.rehome_crash_at.is_some();
        let mut node = Node::spawn_with(
            id,
            kind,
            allocation,
            self._dirs[id].path(),
            None,
            None,
            if kind == NodeKind::Server {
                staged
            } else {
                None
            },
            None,
            overrides,
        )?;
        // a node armed to die never reports ready, and is the caller's to wait for
        if !armed {
            node.wait_ready(self.ready_timeout)?;
            self.plan.endpoints[id] = node.endpoints.clone();
        }
        self.nodes[id] = Some(node);
        Ok(())
    }

    /// Kill a node and start it again at another core count on the same lease
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `kind` - What to restart it as
    /// * `cores` - How many executors to run
    pub fn restart_with_cores(
        &mut self,
        id: usize,
        kind: NodeKind,
        cores: usize,
    ) -> Result<(), FixtureError> {
        let staged = self.staged.get(id).cloned();
        self.restart_with_overrides(
            id,
            kind,
            staged,
            ChildOverrides {
                cores: Some(cores),
                ..ChildOverrides::default()
            },
        )
    }

    /// Kill a node and start it again pinned at a wire version, or with its pin lifted
    /// ([F48](../../../docs/src/features/rolling-compatibility.md))
    ///
    /// The pin is kept in the staging, so a later plain restart keeps it.
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `version` - The newest version to advertise, or none for the build's newest
    pub fn restart_with_wire(
        &mut self,
        id: usize,
        version: Option<u8>,
    ) -> Result<(), FixtureError> {
        self.staged[id].wire_version = version;
        let staged = self.staged.get(id).cloned();
        self.restart_with_overrides(id, NodeKind::Server, staged, ChildOverrides::default())
    }

    /// Kill a node and start it again from another build of the test binary
    /// ([F48](../../../docs/src/features/rolling-compatibility.md))
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `exe` - The binary, or none for this one
    pub fn restart_with_binary(
        &mut self,
        id: usize,
        exe: Option<std::path::PathBuf>,
    ) -> Result<(), FixtureError> {
        let staged = self.staged.get(id).cloned();
        self.restart_with_overrides(
            id,
            NodeKind::Server,
            staged,
            ChildOverrides {
                exe,
                ..ChildOverrides::default()
            },
        )
    }

    /// Kill a node and start it again with its seeds pointing somewhere else
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `seeds` - The control addresses to give it
    pub fn restart_with_seeds(
        &mut self,
        id: usize,
        seeds: Vec<String>,
    ) -> Result<(), FixtureError> {
        let mut staged = self.staged[id].clone();
        staged.seeds = seeds;
        self.restart_with(id, NodeKind::Server, Some(staged))
    }

    /// Start a node the builder deferred
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    pub fn start_deferred(&mut self, id: usize) -> Result<(), FixtureError> {
        assert!(self.nodes[id].is_none(), "node {id} is already started");
        let allocation = self.plan.nodes[id].1.clone();
        let staged = self.staged.get(id).cloned();
        let mut node = Node::spawn_with(
            id,
            NodeKind::Server,
            allocation,
            self._dirs[id].path(),
            None,
            None,
            staged,
            None,
            ChildOverrides::default(),
        )?;
        node.wait_ready(self.ready_timeout)?;
        self.plan.endpoints[id] = node.endpoints.clone();
        self.nodes[id] = Some(node);
        Ok(())
    }

    /// Kill a node without restarting it
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    pub fn kill(&mut self, id: usize) -> Result<(), FixtureError> {
        if let Some(node) = self.nodes[id].as_mut() {
            node.kill()?;
        }
        Ok(())
    }

    /// Stop a node the way a supervisor's `SIGTERM` stops a deployed one, and reap it
    ///
    /// The child stops its pool, which is what `node::serve` does on the signal, so a group it
    /// leads is handed off first ([Resolved #139](../../../docs/src/appendix/resolved/leadership-handoff-on-stop.md)).
    ///
    /// # Arguments
    ///
    /// * `id` - The node to stop
    pub fn stop(&mut self, id: usize) -> Result<serde_json::Value, FixtureError> {
        let node = self.nodes[id]
            .as_mut()
            .ok_or_else(|| FixtureError::NotReady(format!("node {id} is not running")))?;
        let answer = node.command("EXIT")?;
        node.kill()?;
        Ok(answer)
    }

    /// Copy a node's directory, for a clone of it to start on
    ///
    /// # Arguments
    ///
    /// * `id` - The node to copy
    pub fn clone_dir(&self, id: usize) -> Result<TempDir, FixtureError> {
        let copy = crate::utils::test_dir();
        copy_dir(self._dirs[id].path(), copy.path())?;
        Ok(copy)
    }

    /// Reissue a node's leaf under the current authority, naming a node or none, and write it
    /// ([F50](../../../docs/src/features/cluster-operations.md))
    ///
    /// The node keeps serving on its old material until it is asked to reload.
    ///
    /// # Arguments
    ///
    /// * `id` - The node whose files are rewritten
    /// * `node` - The node the leaf names, or none for a leaf with no node in it
    pub fn reissue_leaf(&self, id: usize, node: Option<&str>) -> Result<(), FixtureError> {
        let pki = self
            .pki
            .as_ref()
            .ok_or_else(|| FixtureError::ChildFailed("the peer lanes are plaintext".to_string()))?;
        let paths = self.staged[id].tls.clone().ok_or_else(|| {
            FixtureError::ChildFailed(format!("node {id} was staged with no tls"))
        })?;
        let (cert, key) = pki.issue(node);
        std::fs::write(&paths.cert, cert)?;
        std::fs::write(&paths.key, key)?;
        Ok(())
    }

    /// Where a node's peer TLS files are, if the lanes are encrypted
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    pub fn staged_tls(&self, id: usize) -> Option<node::StagedTls> {
        self.staged[id].tls.clone()
    }

    /// The identity the fixture minted for a node, as its leaf names it
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    pub fn minted_node(&self, id: usize) -> &str {
        &self.staged[id].node
    }

    /// Mint a new authority and keep the old one beside it, so a bundle names both
    /// ([F50](../../../docs/src/features/cluster-operations.md))
    pub fn rotate_authority(&mut self) -> Result<(), FixtureError> {
        let pki = self
            .pki
            .as_mut()
            .ok_or_else(|| FixtureError::ChildFailed("the peer lanes are plaintext".to_string()))?;
        let fresh = Pki::mint();
        let old = std::mem::replace(&mut pki.ca, fresh.ca);
        pki.ca_key = fresh.ca_key;
        pki.previous = Some(old);
        Ok(())
    }

    /// Forget the previous authority, so the bundle names the current one alone
    pub fn retire_previous_authority(&mut self) -> Result<(), FixtureError> {
        let pki = self
            .pki
            .as_mut()
            .ok_or_else(|| FixtureError::ChildFailed("the peer lanes are plaintext".to_string()))?;
        pki.previous = None;
        Ok(())
    }

    /// Write the authority bundle as it is now under a node's directory
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    pub fn write_authorities(&self, id: usize) -> Result<(), FixtureError> {
        let pki = self
            .pki
            .as_ref()
            .ok_or_else(|| FixtureError::ChildFailed("the peer lanes are plaintext".to_string()))?;
        let paths = self.staged[id].tls.clone().ok_or_else(|| {
            FixtureError::ChildFailed(format!("node {id} was staged with no tls"))
        })?;
        std::fs::write(&paths.ca, pki.bundle())?;
        Ok(())
    }

    /// Restart a node at fresh peer ports: the same directory and identity at another address
    /// ([F50](../../../docs/src/features/cluster-operations.md))
    ///
    /// Returns the ports it left, so a clone can be started at them.
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    pub fn restart_at_new_address(&mut self, id: usize) -> Result<(u16, u16), FixtureError> {
        let mut staged = self.staged[id].clone();
        let old = (staged.data_port, staged.control_port);
        // fresh ports from the block, so the old address is free for a clone
        let (data_port, control_port) = ports::next_pair()?;
        staged.data_port = data_port;
        staged.control_port = control_port;
        self.staged[id] = staged.clone();
        self.restart_with(id, NodeKind::Server, Some(staged))?;
        Ok(old)
    }

    /// Start a second process of a node on a copy of its directory, at the ports given
    ///
    /// # Arguments
    ///
    /// * `id` - The node to clone
    /// * `dir` - The copy of its directory
    /// * `ports` - The data and control ports the clone binds
    pub fn spawn_clone_at(
        &self,
        id: usize,
        dir: &std::path::Path,
        ports: (u16, u16),
    ) -> Result<Node, FixtureError> {
        let mut staged = self.staged[id].clone();
        staged.data_port = ports.0;
        staged.control_port = ports.1;
        let allocation = self.plan.nodes[id].1.clone();
        Node::spawn_with(
            id,
            NodeKind::Server,
            allocation,
            dir,
            None,
            None,
            Some(staged),
            None,
            ChildOverrides::default(),
        )
    }

    /// Start a second process of a node on a copy of its directory, with fresh ports
    ///
    /// The clone is staged as the original was but binds new peer ports, so both can run at
    /// once and the cluster has to tell them apart by what it commits, which is the fencing
    /// rule's whole subject.
    ///
    /// # Arguments
    ///
    /// * `id` - The node to clone
    /// * `dir` - The copy of its directory
    pub fn spawn_clone(&self, id: usize, dir: &std::path::Path) -> Result<Node, FixtureError> {
        self.spawn_clone_with(id, dir, ChildOverrides::default())
    }

    /// Start a second process of a node on a copy of its directory, with overrides
    ///
    /// # Arguments
    ///
    /// * `id` - The node to clone
    /// * `dir` - The copy of its directory
    /// * `overrides` - What the clone runs with beyond its staging
    pub fn spawn_clone_with(
        &self,
        id: usize,
        dir: &std::path::Path,
        overrides: ChildOverrides,
    ) -> Result<Node, FixtureError> {
        let mut staged = self.staged[id].clone();
        // the clone's own ports, from the block
        let (data_port, control_port) = ports::next_pair()?;
        staged.data_port = data_port;
        staged.control_port = control_port;
        // the clone runs on the original's allocation, so it runs the shard count the
        // directory was written by; the two share those cores, which a fencing test can afford
        let allocation = self.plan.nodes[id].1.clone();
        let node = Node::spawn_with(
            id,
            NodeKind::Server,
            allocation,
            dir,
            None,
            None,
            Some(staged),
            None,
            overrides,
        )?;
        Ok(node)
    }

    /// Wait until every one of these nodes reports it has joined its group
    ///
    /// # Arguments
    ///
    /// * `ids` - The nodes
    pub fn wait_joined(&mut self, ids: &[usize]) -> Result<(), FixtureError> {
        let deadline = Instant::now() + self.ready_timeout;
        for id in ids {
            loop {
                let readiness = self.node_mut(*id).command("READINESS")?;
                let status = readiness["ok"]["control"].as_str().unwrap_or("");
                if status == "joined" {
                    // a joiner reported ready before it had a cluster; now that it has one, its
                    // endpoints say so, as a bootstrapper's did from the start
                    let members = self.node_mut(*id).command("MEMBERS")?;
                    let node = self.node_mut(*id);
                    node.endpoints.cluster = members["ok"]["cluster"].as_str().map(str::to_string);
                    node.endpoints.topology_version = members["ok"]["version"].as_u64();
                    node.endpoints.control_status = Some("joined".to_string());
                    self.plan.endpoints[*id] = self.node(*id).endpoints.clone();
                    break;
                }
                if Instant::now() > deadline {
                    return Err(FixtureError::NotReady(format!(
                        "node {id} did not join within {:?}: {readiness}",
                        self.ready_timeout
                    )));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        Ok(())
    }

    /// Place the tablets over these nodes, in this order, and wait until every one holds the map
    ///
    /// # Arguments
    ///
    /// * `ids` - The nodes, in placement order
    pub fn initialize(&mut self, ids: &[usize]) -> Result<u64, FixtureError> {
        let list: Vec<String> = ids.iter().map(usize::to_string).collect();
        let reply = self
            .node_mut(ids[0])
            .command(&format!("INITIALIZE {}", list.join(" ")))?;
        let version = reply["ok"]["version"].as_u64().ok_or_else(|| {
            FixtureError::ChildFailed(format!("the initialization was not applied: {reply}"))
        })?;
        self.wait_map_version(ids, version)?;
        Ok(version)
    }

    /// Wait until every one of these nodes holds a map at or past a version
    ///
    /// # Arguments
    ///
    /// * `ids` - The nodes
    /// * `version` - The version
    pub fn wait_map_version(&mut self, ids: &[usize], version: u64) -> Result<(), FixtureError> {
        let deadline = Instant::now() + self.ready_timeout;
        for id in ids {
            loop {
                let map = self.node_mut(*id).command("MAP")?;
                if map["ok"]["version"].as_u64().unwrap_or(0) >= version {
                    break;
                }
                if Instant::now() > deadline {
                    return Err(FixtureError::NotReady(format!(
                        "node {id} did not reach map version {version}: {map}"
                    )));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        Ok(())
    }

    /// The cluster as a node sees it, as the `MEMBERS` command answers
    ///
    /// # Arguments
    ///
    /// * `id` - The node to ask
    pub fn members(&mut self, id: usize) -> Result<serde_json::Value, FixtureError> {
        let reply = self.node_mut(id).command("MEMBERS")?;
        reply.get("ok").cloned().ok_or_else(|| {
            FixtureError::ChildFailed(format!("node {id} answered MEMBERS with {reply}"))
        })
    }

    /// The node id a node names as the control leader, if it names one
    ///
    /// # Arguments
    ///
    /// * `id` - The node to ask
    pub fn leader_of(&mut self, id: usize) -> Result<Option<String>, FixtureError> {
        Ok(self.members(id)?["leader"].as_str().map(str::to_string))
    }

    /// The index of the node a node names as the leader, if it names one this fixture knows
    ///
    /// # Arguments
    ///
    /// * `id` - The node to ask
    pub fn leader_index(&mut self, id: usize) -> Result<Option<usize>, FixtureError> {
        let leader = self.leader_of(id)?;
        let ids = self.node_ids();
        Ok(leader.and_then(|leader| ids.iter().position(|node| *node == leader)))
    }

    /// Wait until a node sees this many voters, and no membership change half way through
    ///
    /// A joint configuration - the old voters and the new - still needs a majority of the old,
    /// so a test that stops a node while one is in progress may leave the rest unable to elect;
    /// this waits until the change has committed, which is when the count means what it says.
    ///
    /// # Arguments
    ///
    /// * `id` - The node to ask
    /// * `voters` - How many
    pub fn wait_voters(&mut self, id: usize, voters: usize) -> Result<(), FixtureError> {
        let deadline = Instant::now() + self.ready_timeout;
        loop {
            let members = self.members(id)?;
            if members["voters"]
                .as_array()
                .is_some_and(|list| list.len() == voters)
                && members["joint"] == false
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(FixtureError::NotReady(format!(
                    "node {id} never saw {voters} voters: {members}"
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Wait until a node names a leader among these nodes, and return which
    ///
    /// # Arguments
    ///
    /// * `id` - The node to ask
    /// * `among` - The indices a leader may be
    /// * `timeout` - How long to wait
    pub fn wait_leader_among(
        &mut self,
        id: usize,
        among: &[usize],
        timeout: Duration,
    ) -> Result<usize, FixtureError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(leader) = self.leader_index(id)? {
                if among.contains(&leader) {
                    return Ok(leader);
                }
            }
            if Instant::now() > deadline {
                let members = self.members(id)?;
                return Err(FixtureError::NotReady(format!(
                    "node {id} never named a leader among {among:?}: {members}"
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Wait until a node's topology reaches a version
    ///
    /// # Arguments
    ///
    /// * `id` - The node to ask
    /// * `version` - The version
    pub fn wait_version(&mut self, id: usize, version: u64) -> Result<(), FixtureError> {
        let deadline = Instant::now() + self.ready_timeout;
        loop {
            let members = self.members(id)?;
            if members["version"].as_u64().unwrap_or(0) >= version {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(FixtureError::NotReady(format!(
                    "node {id} never reached version {version}: {members}"
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Change the seeds a node is staged with, before it starts or restarts
    ///
    /// # Arguments
    ///
    /// * `id` - The node
    /// * `seeds` - The control addresses
    pub fn set_seeds(&mut self, id: usize, seeds: Vec<String>) {
        self.staged[id].seeds = seeds;
    }

    /// Wait until a node's child reports a failure, and return it
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `timeout` - How long to wait
    pub fn wait_failure(node: &Node, timeout: Duration) -> Option<String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(reason) = node.failure() {
                return Some(reason);
            }
            if Instant::now() > deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// The client endpoint of every running node
    pub fn client_endpoints(&self) -> Vec<SocketAddr> {
        self.nodes
            .iter()
            .flatten()
            .map(|node| node.endpoints.client)
            .collect()
    }

    /// The node ids, in index order, as the children know them
    pub fn node_ids(&self) -> Vec<String> {
        self.staged
            .first()
            .map(|staged| staged.peers.clone())
            .unwrap_or_default()
    }
}

/// Copy a directory tree
///
/// # Arguments
///
/// * `from` - The tree
/// * `to` - Where to copy it
fn copy_dir(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// Whether a process is still there
///
/// # Arguments
///
/// * `pid` - The process
pub fn is_alive(pid: u32) -> bool {
    // signal zero checks the pid without sending anything; ESRCH is "no such process". a
    // zombie still answers, which is why the fixture reaps what it kills
    // SAFETY: `kill` with signal 0 has no effect on the target and is always sound to call
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

/// The per-node staging of a membership cluster
///
/// The ports each node binds are numbers from the fixture's block
/// ([`ports`](ports::next_pair)), ours by number below the ephemeral floor, so nothing is
/// reserved and nothing has to be held until a deferred node starts
/// ([Resolved #102](../../../docs/src/appendix/resolved/fixture-port-block.md)).
struct StagedPlan {
    /// One `StagedCluster` per node, in node order
    per_node: Vec<StagedCluster>,
    /// The authority the peer lanes trust, if they are encrypted
    pki: Option<Pki>,
}

/// Mint the identities, reserve the ports, stage the markers, and point every joiner at node zero
///
/// # Arguments
///
/// * `builder` - What was asked for
/// * `dirs` - The nodes' directories, to stage a marker into each
/// * `plan` - The core allocation, to know each node's shard count
fn build_membership_cluster(
    builder: &ClusterBuilder,
    dirs: &[TempDir],
    plan: &ClusterPlan,
) -> Result<StagedPlan, FixtureError> {
    use shoal::server::StorageMeta;
    use shoal::shared::identity::{ClusterId, NodeId};

    let specs = &builder.nodes;
    // one cluster, minted for node zero, and a node id per server
    let cluster = ClusterId::mint();
    let mut ids = Vec::with_capacity(specs.len());
    let mut data_ports = Vec::with_capacity(specs.len());
    let mut control_ports = Vec::with_capacity(specs.len());
    let mut shard_counts = Vec::with_capacity(specs.len());
    for (id, _spec) in specs.iter().enumerate() {
        ids.push(NodeId::mint());
        // the shard count is the data cores the allocator gave this node, or the slots the
        // test asked it to claim above them ([F47](../../../docs/src/features/local-rehome.md))
        let cores = plan.nodes[id].1.data.len().max(1);
        let shards = builder
            .slots
            .iter()
            .find(|(node, _)| *node == id)
            .map_or(cores, |(_, slots)| *slots);
        shard_counts.push(shards);
        // the node's two ports, from the block below the ephemeral floor
        let (data_port, control_port) = ports::next_pair()?;
        data_ports.push(data_port);
        control_ports.push(control_port);
    }
    let peers: Vec<String> = ids.iter().map(NodeId::to_string).collect();
    // the peer lanes' authority and every node's leaf, written under each directory
    // ([F50](../../../docs/src/features/cluster-operations.md))
    let pki = builder.peer_tls.then(Pki::mint);
    if let Some(pki) = &pki {
        for (id, dir) in dirs.iter().enumerate() {
            let paths = tls_paths(dir.path());
            std::fs::create_dir_all(dir.path().join("tls"))?;
            let (cert, key) = pki.issue(Some(&ids[id].to_string()));
            std::fs::write(&paths.cert, cert)?;
            std::fs::write(&paths.key, key)?;
            std::fs::write(&paths.ca, pki.bundle())?;
        }
    }
    // stage a marker naming each node: node zero's names the cluster, every other's is joining
    let mut per_node = Vec::with_capacity(specs.len());
    for (id, dir) in dirs.iter().enumerate() {
        let mut marker = if id == 0 {
            StorageMeta::new(shard_counts[id], ids[id], Some(cluster))
        } else {
            StorageMeta::joining(shard_counts[id], ids[id])
        };
        // a node claiming more slots than cores is laid out on its cores
        let cores = plan.nodes[id].1.data.len().max(1);
        if shard_counts[id] != cores {
            marker.physical = Some(cores);
        }
        std::fs::create_dir_all(dir.path())?;
        std::fs::write(
            StorageMeta::path(dir.path()),
            serde_json::to_vec_pretty(&marker).expect("a marker serializes"),
        )?;
        per_node.push(StagedCluster {
            index: id,
            bootstrap: id == 0,
            node: ids[id].to_string(),
            data_port: data_ports[id],
            control_port: control_ports[id],
            seeds: if id == 0 {
                Vec::new()
            } else {
                vec![format!("127.0.0.1:{}", control_ports[0])]
            },
            peers: peers.clone(),
            dial: Vec::new(),
            slots: builder
                .slots
                .iter()
                .find(|(node, _)| *node == id)
                .map(|(_, slots)| *slots),
            wire_version: builder
                .wire_versions
                .iter()
                .find(|(node, _)| *node == id)
                .map(|(_, version)| *version),
            replication_factor: builder.replication_factor,
            control_voters: builder.control_voters,
            admins: builder.admins.clone(),
            auth: builder.auth.clone(),
            detector_interval_ms: builder.detector_interval_ms,
            trace_file: builder.trace.then(|| {
                dir.path()
                    .join("trace.jsonl")
                    .to_string_lossy()
                    .into_owned()
            }),
            failover_ms: Some(builder.failover_ms),
            write_timeout_ms: builder.write_timeout_ms,
            pending_bytes: builder.pending_bytes,
            checkpoint_entries: builder.checkpoint_entries,
            retained_entries: builder.retained_entries,
            segment_bytes: builder.segment_bytes,
            retained_bytes: builder.retained_bytes,
            snapshot_chunk_bytes: builder.snapshot_chunk_bytes,
            bulk_queue_bytes: builder.bulk_queue_bytes,
            crash_at: None,
            install_hold_ms: None,
            read_consistency: builder.read_consistency.clone(),
            query_deadline_ms: builder.query_deadline_ms,
            scrub_interval_ms: builder.scrub_interval_ms,
            repair_timeout_ms: builder.repair_timeout_ms,
            snapshot_timeout_ms: builder.snapshot_timeout_ms,
            retire_after_ms: builder.retire_after_ms,
            catchup_lag: builder.catchup_lag,
            migration_timeout_ms: builder.migration_timeout_ms,
            retry_window_ms: builder.retry_window_ms,
            move_crash_at: None,
            auto_remove_after_ms: builder.auto_remove_after_ms,
            weight: builder
                .weights
                .iter()
                .find(|(node, _)| *node == id)
                .map(|(_, weight)| *weight),
            stream_bytes_per_sec: builder
                .stream_budgets
                .iter()
                .find(|(node, _, _)| *node == id)
                .map(|(_, bytes, _)| *bytes),
            concurrent_streams: builder
                .stream_budgets
                .iter()
                .find(|(node, _, _)| *node == id)
                .map(|(_, _, streams)| *streams),
            disk_reserve: builder.disk_reserve,
            moves_per_node: builder.moves_per_node,
            plan_interval_ms: builder.plan_interval_ms,
            tls: pki.as_ref().map(|_| tls_paths(dir.path())),
        });
    }
    Ok(StagedPlan { per_node, pki })
}
