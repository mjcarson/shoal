//! The cluster fixture: real Shoal servers as child processes, with the handles a test needs
//!
//! [C11](../../../docs/src/distributed/testing.md) asks for nodes run as child processes with
//! independent directories, explicit core allocation, actual bound endpoints, readiness and
//! failure reported separately, cleanup that runs even when a test fails, and directed fault
//! controls. This is that, at M0's scope: the children are isolated servers and mock peers,
//! because there is no peer transport, membership or node identity yet for them to speak.
//!
//! Every test binary that uses this declares `mod cluster;` next to `mod utils;`, since the
//! children are re-executions of the test binary itself (the shape `ack_survives_sigkill` uses)
//! and the temp directories come from `utils::test_dir`.
//!
//! ```text
//! let cluster = Cluster::builder()
//!     .server(CoreClaim::Count(1))
//!     .mock_peer(CoreClaim::Shared)
//!     .links(true)
//!     .start()
//!     .await?;
//! let addr = cluster.node(0).endpoints.client;          // what the child actually bound
//! cluster.link(1, 0).cut();                              // every path from 1 to 0, reconnects included
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
pub mod schema;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use tempfile::TempDir;

pub use cores::{Allocation, ClusterPlan, CoreClaim, Topology};
pub use link::{Link, LinkState};
pub use node::{ChildRequest, Endpoints, Node, NodeKind, CHILD_ENV, FAILED_LINE, READY_LINE};

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
}

/// How long a cluster waits for every child to report ready
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Describes a cluster before it is started
#[derive(Debug, Clone)]
pub struct ClusterBuilder {
    /// The nodes, in the order they get ids
    nodes: Vec<NodeSpec>,
    /// Whether to put a directed proxy between every ordered pair of nodes
    links: bool,
    /// The cores the test's own driver claims
    driver: CoreClaim,
    /// How long to wait for readiness
    ready_timeout: Duration,
}

impl ClusterBuilder {
    /// Add a real server
    ///
    /// # Arguments
    ///
    /// * `cores` - The cores it should own
    pub fn server(mut self, cores: CoreClaim) -> Self {
        self.nodes.push(NodeSpec {
            kind: NodeKind::Server,
            cores,
        });
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
        });
        self
    }

    /// Whether to build directed links between every ordered pair of nodes
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

    /// Start every child, wait for all of them, and build the links
    pub async fn start(self) -> Result<Cluster, FixtureError> {
        // allocate cores first, so a claim that cannot be met fails before a process exists
        let topology = Topology::detect();
        let mut plan = self.plan(&topology)?;
        // one directory per node, alive as long as the cluster
        let dirs: Vec<TempDir> = self.nodes.iter().map(|_| crate::utils::test_dir()).collect();
        // spawn everything, then wait for everything, so the children start in parallel
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for (id, (spec, dir)) in self.nodes.iter().zip(&dirs).enumerate() {
            let allocation = plan.nodes[id].1.clone();
            nodes.push(Node::spawn(id, spec.kind, allocation, dir.path())?);
        }
        for node in &mut nodes {
            // a node that never comes up takes the whole cluster down with it, and the
            // evidence: `Node`'s drop kills the rest
            node.wait_ready(self.ready_timeout)?;
            plan.endpoints.push(node.endpoints.clone());
        }
        // a directed link per ordered pair, each pointing at the target's client endpoint
        let mut links = BTreeMap::new();
        if self.links {
            for from in 0..nodes.len() {
                for to in 0..nodes.len() {
                    if from != to {
                        let link = Link::start(nodes[to].endpoints.client).await?;
                        plan.proxies.push(((from, to), link.addr()));
                        links.insert((from, to), link);
                    }
                }
            }
        }
        Ok(Cluster {
            nodes,
            links,
            plan,
            _dirs: dirs,
        })
    }
}

/// A running cluster
///
/// Dropping it kills and reaps every child and stops every proxy, whether the test passed or
/// panicked: cleanup is what a `Drop` is for.
pub struct Cluster {
    /// The nodes, by id
    nodes: Vec<Node>,
    /// The directed links, by (from, to)
    links: BTreeMap<(usize, usize), Link>,
    /// What was allocated and bound
    plan: ClusterPlan,
    /// The storage directories, dropped last
    _dirs: Vec<TempDir>,
}

impl Cluster {
    /// Describe a cluster
    pub fn builder() -> ClusterBuilder {
        ClusterBuilder {
            nodes: Vec::new(),
            links: false,
            driver: CoreClaim::Shared,
            ready_timeout: DEFAULT_READY_TIMEOUT,
        }
    }

    /// A node
    ///
    /// # Arguments
    ///
    /// * `id` - Its id, from zero in the order the builder added it
    pub fn node(&self, id: usize) -> &Node {
        &self.nodes[id]
    }

    /// A node, to signal
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    pub fn node_mut(&mut self, id: usize) -> &mut Node {
        &mut self.nodes[id]
    }

    /// How many nodes
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether there are none
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The directed link from one node to another
    ///
    /// Anything `from` sends to `to` goes through this proxy's address, so a fault on it covers
    /// every connection in that direction, including ones opened after the fault.
    ///
    /// # Arguments
    ///
    /// * `from` - The sending node
    /// * `to` - The receiving node
    pub fn link(&self, from: usize, to: usize) -> &Link {
        self.links
            .get(&(from, to))
            .unwrap_or_else(|| panic!("no link from {from} to {to}; was the cluster built with links?"))
    }

    /// What was allocated and bound
    pub fn plan(&self) -> &ClusterPlan {
        &self.plan
    }

    /// Every child's pid, for a test to check they are gone afterwards
    pub fn pids(&self) -> Vec<u32> {
        self.nodes.iter().map(|node| node.pid).collect()
    }

    /// The client endpoint of every node
    pub fn client_endpoints(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|node| node.endpoints.client).collect()
    }
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
