//! Staging a static cluster for one workload: identities, markers, cores, ports and peers
//!
//! # The measured process owns the placement
//!
//! A cluster arm with more than one node needs a static placement
//! ([F38](../../../../docs/src/features/inter-node-transport.md)): a map that names every node's
//! identity, endpoints and shard count, which every node reads and none of them elects. Only
//! something that has minted the identities can write that map, and minting one means writing a
//! storage marker into a directory before the server that owns it starts. The runner half of
//! `shoal-bench` builds with no engine and cannot do that, so the staging happens here, in the
//! process that runs the workload: it mints the identities, writes the markers, works out which
//! physical cores each node may have, starts every other node as a `shoal-workload serve` child
//! of itself, and hosts node zero in process the way the overhead arm always has.
//!
//! The runner's plan is untouched by any of this. An arm is still one `run` command, and a
//! capture still runs one workload process at a time; what changed is that the one process may
//! have children, which is [C10](../../../../docs/src/distributed/performance.md)'s "simultaneous
//! processes inside that arm are intentional, independent benchmark arms remain serialized".
//!
//! # Cores are decided once, in this process
//!
//! Two servers on one machine that each picked their own cores would pick the same ones. Every
//! node is therefore handed an `exclude_cores` list naming every physical core some other node
//! took, and a `cores` count, and [`Resources::cpus_reserving`] does the rest: it walks the
//! machine in a fixed order and takes one cpu per physical core, so a node that can see only its
//! own cores lands on exactly them. [`allocate`] walks the same order, which is what makes the
//! claim it records the truth rather than a hope - and node zero's claim is checked against the
//! cores its pool actually took, so a drift between the two orders fails the run instead of
//! recording a placement that did not happen.

use std::collections::BTreeSet;
use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use shoal::Conf;
use shoal::server::StorageMeta;
use shoal::server::conf::Resources;
use shoal::server::conf::cluster::Cluster;
use shoal::server::{AdminKind, AdminRequest};
use shoal::shared::identity::{ClusterId, NodeId};

use crate::model::macro_layer::{
    ClusterFacts, HopFacts, LinkFacts, NodeCores, PlacedNodeFacts, TransportFacts,
};
use crate::run::plan::cluster_ports;
use crate::workloads::harness::ready;
use crate::workloads::workload::ConfOverrides;

/// The line `serve` prints once its shards answer, followed by the bound address
///
/// Lives here rather than in the binary because both halves read it: the binary prints it and
/// [`spawn_peers`] waits for it.
pub const SERVE_READY_LINE: &str = "SHOAL_WORKLOAD_SERVING";

/// One node of a staged cluster, as the file that starts it describes it
///
/// Written by the measured process, read by a `serve --staged` child. Everything a node needs
/// to become the node the placement names is here, so the child resolves the workload's
/// configuration exactly as `run` does and then applies this on top.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedNode {
    /// This node's position in the placement; zero is the node the driver runs beside
    pub index: u32,
    /// The identity minted for it, which its marker already carries
    pub node: String,
    /// The cluster every node was minted into
    pub cluster: String,
    /// Where its clients connect
    pub client_port: u16,
    /// Where its data lane listens
    pub data_port: u16,
    /// Where its control lane listens
    pub control_port: u16,
    /// How many shards it runs
    pub shards: u16,
    /// The physical cores its shards are expected to land on
    pub cores: Vec<usize>,
    /// The physical cores its configuration excludes, which is every core another node took
    pub exclude_cores: Vec<usize>,
    /// The cpu its control thread is pinned to
    pub control_cpu: usize,
    /// The physical core that cpu is on
    pub control_core: usize,
    /// What is appended to the workload's storage directory name to make this node's own
    ///
    /// Empty for node zero, which keeps the directory a one node arm would have, and `-node<k>`
    /// for a peer, whose directory sits beside it rather than under it: the runner's wipe
    /// recognizes a Shoal store by a marker at most one level down, and a nested node would
    /// hide its marker from that check.
    pub suffix: String,
    /// The placement the arm expects once node zero has initialized it, in node order
    ///
    /// Recorded on the artifact and used to choose keys; the nodes route against the map the
    /// control plane commits, which `initialize` places in exactly this order
    /// ([F39](../../../../docs/src/features/membership.md)).
    pub placement: Vec<PlacedNodeFacts>,
    /// The replication factor node zero's bootstrap records
    pub replication_factor: u32,
    /// The control addresses a peer joins through, which is node zero's; empty for node zero
    #[serde(default)]
    pub seeds: Vec<String>,
}

/// A staged cluster, ready to start
#[derive(Debug, Clone)]
pub struct Staged {
    /// Every node in placement order; node zero is this process's
    pub nodes: Vec<StagedNode>,
    /// Where each node's json was written, in the same order
    pub files: Vec<PathBuf>,
    /// The hop the arm was built to take, if it is a hop arm
    pub hop: Option<HopFacts>,
}

/// One cpu the machine offers, in the order the server walks them
///
/// The four fields are the sort key [`Resources::cpus_reserving`] uses, so a list of these sorted
/// the same way is the list the server will pick from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Candidate {
    /// The NUMA node
    pub numa: usize,
    /// The package
    pub package: usize,
    /// The physical core
    pub core: usize,
    /// The logical cpu
    pub cpu: usize,
}

/// What one node was allotted
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    /// The physical cores its shards take, one cpu each
    pub data: Vec<usize>,
    /// The cpu its control thread takes and the physical core that cpu is on
    pub control: (usize, usize),
}

/// Stages a static cluster for a workload: identities, markers, cores and ports
///
/// Nothing is started. The markers are written, so the directories are claimed for the
/// identities the placement names, and each node's description is written beside them for
/// [`spawn_peers`] to hand to a child.
///
/// # Arguments
///
/// * `base` - The configuration `conf::resolve` produced for the workload, before any node's
///   own changes; its storage path already carries the workload's subdirectory
/// * `id` - The workload
/// * `overrides` - What the workload asked of its server, which names the peers
/// * `port` - The client port the runner gave node zero
pub fn stage(base: &Conf, id: &str, overrides: &ConfOverrides, port: u16) -> Result<Staged> {
    // a placement needs a cluster block with peers in it
    let Some(cluster) = &overrides.cluster else {
        bail!("{id} stages a cluster without a cluster override");
    };
    if cluster.peers.is_empty() {
        bail!("{id} stages a cluster with no peers, which the one node path serves");
    }
    // and node zero's own shard count, which is the only thing the placement cannot infer
    let Some(shards) = overrides.shards else {
        bail!("{id} places peers without naming its own shard count in `shards`");
    };
    let shards = u16::try_from(shards).context("a node runs fewer shards than a u16 holds")?;
    // every node's shard count, node zero first
    let mut counts = vec![shards];
    counts.extend(cluster.peers.iter().copied());
    let nodes = counts.len();
    // the cores, decided once here
    let claims = allocate(&candidates(base)?, &counts)?;
    // the ports, from the block above the single node range; node zero keeps the client port
    // the runner gave it, so the arm binds the port every other arm's rule says it does
    let ports = cluster_ports(id, u16::try_from(nodes).context("too many nodes")?)?;
    // the identities, minted here and nowhere else
    let cluster_id = ClusterId::mint();
    let ids: Vec<NodeId> = (0..nodes).map(|_| NodeId::mint()).collect();
    let interface = base.networking.interface.clone();
    let placement: Vec<PlacedNodeFacts> = (0..nodes)
        .map(|index| PlacedNodeFacts {
            node: ids[index].to_string(),
            shards: counts[index],
            data: format!("{interface}:{}", ports[index].data),
            control: format!("{interface}:{}", ports[index].control),
        })
        .collect();
    // the physical core cpu 0 is on, which no node's shards may take: node zero's control
    // thread is there, and the pool reserves it for that node alone
    let core_zero = core_of(0)?;
    let mut staged = Vec::with_capacity(nodes);
    let mut files = Vec::with_capacity(nodes);
    for (index, claim) in claims.iter().enumerate() {
        // every core some other node took, data and control, plus cpu 0's for a peer
        let mut exclude: BTreeSet<usize> = base.resources.exclude_cores.iter().copied().collect();
        for (other, claimed) in claims.iter().enumerate() {
            if other != index {
                exclude.extend(claimed.data.iter().copied());
                exclude.insert(claimed.control.1);
            }
        }
        if index != 0 {
            exclude.insert(core_zero);
        }
        let node = StagedNode {
            index: u32::try_from(index).context("too many nodes")?,
            node: ids[index].to_string(),
            cluster: cluster_id.to_string(),
            client_port: if index == 0 { port } else { ports[index].client },
            data_port: ports[index].data,
            control_port: ports[index].control,
            shards: counts[index],
            cores: claim.data.clone(),
            exclude_cores: exclude.into_iter().collect(),
            control_cpu: claim.control.0,
            control_core: claim.control.1,
            suffix: if index == 0 { String::new() } else { format!("-node{index}") },
            placement: placement.clone(),
            replication_factor: cluster.replication_factor,
            seeds: if index == 0 {
                Vec::new()
            } else {
                vec![format!("{interface}:{}", ports[0].control)]
            },
        };
        // the marker, written before the node can claim the directory for itself: node zero's
        // names the cluster it creates, a peer's names only itself and joins
        let dir = node_dir(base, &node);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let marker = if index == 0 {
            StorageMeta::new(usize::from(node.shards), ids[index], Some(cluster_id))
        } else {
            StorageMeta::joining(usize::from(node.shards), ids[index])
        };
        std::fs::write(
            StorageMeta::path(&dir),
            serde_json::to_vec_pretty(&marker).context("failed to serialize a marker")?,
        )
        .with_context(|| format!("failed to stage a marker in {}", dir.display()))?;
        // and the node's own description, for the child that becomes it
        let file = workload_dir(base).join(format!("node{index}.json"));
        std::fs::write(
            &file,
            serde_json::to_vec_pretty(&node).context("failed to serialize a staged node")?,
        )
        .with_context(|| format!("failed to write {}", file.display()))?;
        staged.push(node);
        files.push(file);
    }
    Ok(Staged {
        nodes: staged,
        files,
        hop: cluster.hop.clone(),
    })
}

/// Applies a staged node's description to a resolved configuration
///
/// `conf::resolve` has already given the workload its own storage subdirectory and the port the
/// runner chose; this moves the storage down one level to the node's own directory, replaces the
/// port with the node's, and writes the cluster block the placement needs.
///
/// # Arguments
///
/// * `conf` - The configuration `conf::resolve` produced for this workload
/// * `node` - The node this process is becoming
pub fn apply(mut conf: Conf, node: &StagedNode) -> Result<Conf> {
    // the node's storage, beside the workload's own directory for a peer and that directory
    // itself for node zero
    let filesystem = &mut conf.storage.default.filesystem;
    filesystem.latency_sensitive.path = with_suffix(&filesystem.latency_sensitive.path, &node.suffix);
    filesystem.throughput_sensitive.path =
        with_suffix(&filesystem.throughput_sensitive.path, &node.suffix);
    // its cores: a count, and every core another node took kept out of reach
    conf.resources.cores = Some(usize::from(node.shards));
    conf.resources.exclude_cores = node.exclude_cores.clone();
    // its client port
    conf.networking.port = node.client_port;
    // and its own lanes and control core: node zero creates the cluster, a peer joins through it
    conf.cluster = Some(
        Cluster::default()
            .bootstrap(node.index == 0)
            .seeds(node.seeds.clone())
            .control_core(node.control_cpu)
            .replication_factor(node.replication_factor)
            .port(node.data_port)
            .control_port(node.control_port),
    );
    Ok(conf)
}

/// Places the tablets over every node, once all of them have joined
///
/// What an operator does once a cluster's nodes are up: wait until every one is a member, then
/// initialize the placement in the order the arm expects. Node zero's pool is the operator
/// here, through its in-process admin seam.
///
/// # Arguments
///
/// * `staged` - The cluster
/// * `pool` - Node zero's running pool
pub fn initialize(staged: &Staged, pool: &shoal::ShoalPool<crate::workloads::schema::Bench>) -> Result<()> {
    let ids = staged
        .nodes
        .iter()
        .map(|node| {
            node.node
                .parse()
                .map(NodeId)
                .with_context(|| format!("{} is not a node id", node.node))
        })
        .collect::<Result<Vec<NodeId>>>()?;
    // every node up, which is every joiner admitted and observed
    let deadline = std::time::Instant::now() + ready::TIMEOUT;
    let version = loop {
        let view = pool.topology().map_err(|error| anyhow::anyhow!("the control plane did not answer: {error}"))?;
        let up = ids
            .iter()
            .all(|id| view.members.iter().any(|member| member.record.node == *id && member.health == shoal::server::control::types::MemberHealth::Up));
        if up {
            break view.version;
        }
        if std::time::Instant::now() > deadline {
            bail!("not every node joined within {:?}: {view:?}", ready::TIMEOUT);
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    // then the one explicit placement, retried if the version moves under it, which is what an
    // operator's tool does when a member observes itself between the read and the proposal
    let op = uuid::Uuid::new_v4();
    let mut expected_version = version;
    let placed = loop {
        let response = pool
            .admin(AdminRequest {
                op,
                expected_version,
                kind: AdminKind::Initialize { nodes: ids.clone() },
            })
            .map_err(|error| anyhow::anyhow!("the initialization was not answered: {error}"))?;
        match response.outcome {
            Ok(shoal::shared::protocol::admin::AdminOutcome::Applied { version })
            | Ok(shoal::shared::protocol::admin::AdminOutcome::Repeated { version }) => break version,
            Err(error)
                if error.code() == shoal::shared::protocol::error::ErrorCode::StaleVersion
                    && std::time::Instant::now() < deadline =>
            {
                expected_version = pool
                    .topology()
                    .map_err(|error| anyhow::anyhow!("the control plane did not answer: {error}"))?
                    .version;
            }
            other => bail!("the initialization was refused: {other:?}"),
        }
    };
    // and node zero's shards holding the map before anything is measured against them
    loop {
        let map = pool.map().map_err(|error| anyhow::anyhow!("the control plane did not answer: {error}"))?;
        if map.version >= placed && map.placement == ids {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            bail!("the placement did not reach node zero's shards");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Reads a staged node's description back
///
/// # Arguments
///
/// * `path` - Where [`stage`] wrote it
pub fn load(path: &Path) -> Result<StagedNode> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("failed to parse {}", path.display()))
}

/// The machine's cpus in the order the server walks them, less cpu 0's whole core
///
/// # Arguments
///
/// * `base` - The configuration, for the cores it already excludes
pub fn candidates(base: &Conf) -> Result<Vec<Candidate>> {
    // the same filter the server applies, with cpu 0's core reserved the way a cluster node
    // reserves its control core
    let online = Resources::default()
        .exclude_cores(base.resources.exclude_cores.clone())
        .cpus_reserving(&[core_of(0)?])
        .map_err(|error| anyhow::anyhow!("failed to read the machine's cpus: {error:?}"))?;
    let mut candidates: Vec<Candidate> = online
        .into_iter()
        .map(|location| Candidate {
            numa: location.numa_node,
            package: location.package,
            core: location.core,
            cpu: location.cpu,
        })
        .collect();
    // the derived `Ord` is the server's sort key, field for field
    candidates.sort_unstable();
    Ok(candidates)
}

/// Allots physical cores to nodes, in the order the server would take them
///
/// Node zero's shards take the first distinct cores. Every node after it takes a core for its
/// control thread and then one per shard. Node zero's control thread is cpu 0, which is not a
/// candidate at all, so it takes none here. A machine without enough distinct cores is refused
/// rather than shared: [C10](../../../../docs/src/distributed/performance.md) allows sharing only
/// when it is recorded, and a benchmark that recorded it would be measuring the sharing.
///
/// # Arguments
///
/// * `candidates` - The cpus on offer, sorted as [`candidates`] sorts them
/// * `shards` - How many shards each node runs, node zero first
pub fn allocate(candidates: &[Candidate], shards: &[u16]) -> Result<Vec<Claim>> {
    // one cpu per physical core, first seen first, which is the server's own rule
    let mut seen = BTreeSet::new();
    let mut cores = candidates
        .iter()
        .filter(|candidate| seen.insert(candidate.core))
        .copied();
    // every shard's core, plus a control core for every node but zero
    let needed: usize =
        shards.iter().map(|count| usize::from(*count)).sum::<usize>() + shards.len().saturating_sub(1);
    let mut claims = Vec::with_capacity(shards.len());
    for (index, count) in shards.iter().enumerate() {
        // node zero's control thread is cpu 0, outside the candidates; every other node's takes
        // the next core before its shards do
        let control = if index == 0 {
            (0, core_of(0).unwrap_or(0))
        } else {
            let Some(next) = cores.next() else {
                bail!(too_small(candidates, needed));
            };
            (next.cpu, next.core)
        };
        let mut data = Vec::with_capacity(usize::from(*count));
        for _ in 0..*count {
            let Some(next) = cores.next() else {
                bail!(too_small(candidates, needed));
            };
            data.push(next.core);
        }
        claims.push(Claim { data, control });
    }
    Ok(claims)
}

/// The refusal for a machine with too few distinct cores
///
/// # Arguments
///
/// * `candidates` - What was on offer
/// * `needed` - How many distinct cores the placement wanted
fn too_small(candidates: &[Candidate], needed: usize) -> String {
    let distinct: BTreeSet<usize> = candidates.iter().map(|candidate| candidate.core).collect();
    format!(
        "this machine offers {} physical cores after exclusions and the placement needs {needed}; \
         a cluster arm does not share cores between nodes, since a number measured that way would \
         be a measurement of the sharing",
        distinct.len()
    )
}

/// The physical core a cpu is on, read the way the fixture and `cluster_facts` read it
///
/// # Arguments
///
/// * `cpu` - The logical cpu
pub fn core_of(cpu: usize) -> Result<usize> {
    let path = format!("/sys/devices/system/cpu/cpu{cpu}/topology/core_id");
    std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {path}"))?
        .trim()
        .parse()
        .with_context(|| format!("{path} did not hold a core id"))
}

/// The workload's own storage directory, under the latency sensitive path
///
/// `conf::resolve` has already put the workload's slug on the path, so this is the path as the
/// resolved configuration carries it and the marker lands where the node will look for it.
///
/// # Arguments
///
/// * `resolved` - The configuration `conf::resolve` produced for the workload
fn workload_dir(resolved: &Conf) -> PathBuf {
    resolved
        .storage
        .default
        .filesystem
        .latency_sensitive
        .path
        .clone()
}

/// A node's storage directory, where its marker lives
///
/// # Arguments
///
/// * `resolved` - The configuration `conf::resolve` produced for the workload
/// * `node` - The node
fn node_dir(resolved: &Conf, node: &StagedNode) -> PathBuf {
    with_suffix(&workload_dir(resolved), &node.suffix)
}

/// A path with a suffix appended to its last component
///
/// # Arguments
///
/// * `path` - The workload's directory
/// * `suffix` - What to append; nothing leaves the path as it is
fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    // node zero's directory is the workload's own
    if suffix.is_empty() {
        return path.to_path_buf();
    }
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!("{name}{suffix}"))
}

/// A peer node this process started, killed when dropped
///
/// `serve` holds its server up until it is killed, so a child that outlived this process would
/// hold its cores and its port block until somebody noticed. Dropping this kills and reaps it,
/// on the failing path as well as the succeeding one.
#[derive(Debug)]
pub struct PeerChild {
    /// The node it is
    pub index: u32,
    /// The process
    child: Child,
}

impl Drop for PeerChild {
    fn drop(&mut self) {
        // a child that already exited is reaped; one still serving is killed first
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts every node but zero as a `serve --staged` child of this process
///
/// Returns once every child has printed [`SERVE_READY_LINE`], so the placement is answering on
/// every data and control lane before node zero starts and dials any of them.
///
/// # Arguments
///
/// * `staged` - The cluster, with node zero's description first
/// * `id` - The workload
/// * `conf` - The base configuration file the children resolve from
/// * `scale` - The scale they resolve at, which decides the overrides
pub fn spawn_peers(staged: &Staged, id: &str, conf: &Path, scale: &str) -> Result<Vec<PeerChild>> {
    // the binary this process is, which carries the same workloads
    let exe = std::env::current_exe().context("failed to find this binary")?;
    let mut children = Vec::with_capacity(staged.nodes.len().saturating_sub(1));
    for (node, file) in staged.nodes.iter().zip(&staged.files).skip(1) {
        let mut child = Command::new(&exe)
            .arg("serve")
            .arg("--id")
            .arg(id)
            .arg("--conf")
            .arg(conf)
            .arg("--scale")
            .arg(scale)
            .arg("--staged")
            .arg(file)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("failed to start node {} of {id}", node.index))?;
        let stdout = child.stdout.take().context("a child's stdout was not piped")?;
        // a thread reads the child's stdout for the ready line, then keeps draining it so the
        // pipe can never fill and stall the child
        let (tx, rx) = mpsc::channel::<Option<String>>();
        std::thread::spawn(move || {
            let mut ready_sent = false;
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if !ready_sent && line.starts_with(SERVE_READY_LINE) {
                    ready_sent = true;
                    let _ = tx.send(Some(line));
                }
            }
            // end of stream, which before the ready line means the child gave up
            if !ready_sent {
                let _ = tx.send(None);
            }
        });
        let index = node.index;
        let mut peer = PeerChild { index, child };
        // wait for it to answer, and say which node it was if it does not
        match rx.recv_timeout(ready::TIMEOUT + Duration::from_secs(5)) {
            Ok(Some(_)) => {}
            Ok(None) => {
                let status = peer.child.try_wait().ok().flatten();
                bail!("node {index} of {id} exited before it was serving: {status:?}");
            }
            Err(_) => bail!(
                "node {index} of {id} did not report serving within {:?}",
                ready::TIMEOUT
            ),
        }
        children.push(peer);
    }
    Ok(children)
}

/// The cluster record for a staged placement, from what node zero's pool reports
///
/// Node zero's cores are read back from the pool and checked against the claim, so the record
/// never says a node had cores it did not. The peers' cores are the claim: their pools are in
/// other processes, and the exclusion list they were given leaves them nothing else to take.
///
/// # Arguments
///
/// * `staged` - The cluster
/// * `pool` - Node zero's running pool
/// * `conf` - The configuration node zero was started with
/// * `mut facts` - The record `cluster_facts` built for a node alone, to fill in
pub fn placed_facts(
    staged: &Staged,
    pool: &shoal::ShoalPool<crate::workloads::schema::Bench>,
    conf: &Conf,
    mut facts: ClusterFacts,
) -> Result<ClusterFacts> {
    // node zero's actual cores, which the one node record already read from the pool
    let actual = facts.cores.first().cloned().unwrap_or(NodeCores {
        data: Vec::new(),
        control: None,
    });
    let claimed = &staged.nodes[0];
    if actual.data != claimed.cores {
        bail!(
            "node 0 was allotted cores {:?} and its shards took {:?}; the allocation no longer \
             walks the machine the way the server does",
            claimed.cores,
            actual.data
        );
    }
    // every node's cores, node zero's as read and the rest as claimed
    facts.cores = staged
        .nodes
        .iter()
        .map(|node| NodeCores {
            data: node.cores.clone(),
            control: Some(node.control_core),
        })
        .collect();
    facts.nodes = u32::try_from(staged.nodes.len()).unwrap_or(u32::MAX);
    facts.placement = claimed.placement.clone();
    facts.hop = staged.hop.clone();
    facts.transport = Some(transport_facts(pool, conf)?);
    Ok(facts)
}

/// The transport's bounds and what node zero's links did
///
/// # Arguments
///
/// * `pool` - Node zero's running pool
/// * `conf` - The configuration it was started with
pub fn transport_facts(
    pool: &shoal::ShoalPool<crate::workloads::schema::Bench>,
    conf: &Conf,
) -> Result<TransportFacts> {
    let Some(cluster) = &conf.cluster else {
        bail!("a transport record was asked of a standalone node");
    };
    // what the pool can see, which at M2 is shard zero's links
    let views = pool
        .transport()
        .map_err(|error| anyhow::anyhow!("failed to read the transport: {error:?}"))?;
    let links = views
        .into_iter()
        .flat_map(|view| view.links)
        .map(|link| LinkFacts {
            node: link.node.to_string(),
            lane: link.lane,
            state: link.state,
            sent_frames: link.sent_frames,
            sent_bytes: link.sent_bytes,
            shed_frames: link.shed_frames,
            dropped_frames: link.dropped_frames,
            dials: link.dials,
            queued_bytes: link.queued_bytes as u64,
            bound: link.bound as u64,
        })
        .collect();
    Ok(TransportFacts {
        data_queue_bytes: cluster.transport.data_queue_bytes as u64,
        inflight_bytes: cluster.transport.inflight_bytes as u64,
        forward_timeout_ms: u64::try_from(cluster.transport.forward_timeout.duration().as_millis())
            .unwrap_or(u64::MAX),
        links,
    })
}

#[cfg(test)]
mod tests {
    use super::{Candidate, StagedNode, allocate};
    use crate::model::macro_layer::PlacedNodeFacts;

    /// A machine shaped like the benchmark host: two threads per core, sorted as the server sorts
    ///
    /// # Arguments
    ///
    /// * `cores` - How many physical cores, each with a sibling at `core + cores`
    fn machine(cores: usize) -> Vec<Candidate> {
        let mut candidates = Vec::new();
        for core in 1..cores {
            for thread in 0..2 {
                candidates.push(Candidate {
                    numa: 0,
                    package: 0,
                    core,
                    cpu: core + thread * cores,
                });
            }
        }
        candidates.sort_unstable();
        candidates
    }

    /// Every node's shards and control thread land on physical cores no other node touches
    #[test]
    fn allocate_places_nodes_on_disjoint_physical_cores() {
        let claims = allocate(&machine(16), &[4, 1]).expect("sixteen cores is enough");
        assert_eq!(claims.len(), 2);
        // node zero's shards take the first four cores, one cpu each, and its control is cpu 0
        assert_eq!(claims[0].data, vec![1, 2, 3, 4]);
        assert_eq!(claims[0].control.0, 0);
        // node one's control thread takes the next core and its shard the one after
        assert_eq!(claims[1].control, (5, 5));
        assert_eq!(claims[1].data, vec![6]);
        // nothing shared
        let mut all: Vec<usize> = claims.iter().flat_map(|claim| claim.data.clone()).collect();
        all.push(claims[1].control.1);
        let distinct: std::collections::BTreeSet<usize> = all.iter().copied().collect();
        assert_eq!(distinct.len(), all.len());
    }

    /// A machine too small to isolate every node is refused rather than shared
    #[test]
    fn allocate_refuses_a_machine_too_small_to_isolate() {
        // five cores after cpu 0's: node zero wants four and node one wants two more
        let error = allocate(&machine(6), &[4, 1]).expect_err("five cores is one short");
        assert!(error.to_string().contains("needs 6"), "{error}");
        assert!(error.to_string().contains("offers 5"), "{error}");
    }

    /// A staged node survives the trip through the file its child reads it from
    #[test]
    fn a_staged_node_round_trips_through_json() {
        let node = StagedNode {
            index: 1,
            node: "00000000-0000-0000-0000-000000000001".to_string(),
            cluster: "00000000-0000-0000-0000-000000000002".to_string(),
            seeds: vec!["127.0.0.1:44070".to_string()],
            client_port: 44_072,
            data_port: 44_073,
            control_port: 44_074,
            shards: 1,
            cores: vec![6],
            exclude_cores: vec![0, 1, 2, 3, 4, 12, 13, 14, 15],
            control_cpu: 5,
            control_core: 5,
            suffix: "-node1".to_string(),
            placement: vec![PlacedNodeFacts {
                node: "00000000-0000-0000-0000-000000000001".to_string(),
                shards: 1,
                data: "127.0.0.1:44073".to_string(),
                control: "127.0.0.1:44074".to_string(),
            }],
            replication_factor: 1,
        };
        let text = serde_json::to_string(&node).expect("serializes");
        let back: StagedNode = serde_json::from_str(&text).expect("parses");
        assert_eq!(back, node);
    }
}
