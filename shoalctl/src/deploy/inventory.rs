//! What a deployment is asked to build: the hosts, the binary and the cluster's shape
//!
//! An inventory is a YAML file the operator writes once per cluster. It names the server
//! program to deploy, the hosts to deploy it to and how to reach them over ssh, and the policy
//! the cluster is bootstrapped with. Everything else - node ids, certificates, the admin
//! password - is minted by the deployment and kept in its [state](super::state).

use color_eyre::eyre::{bail, eyre, WrapErr};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, ToSocketAddrs};
use std::path::{Component, Path, PathBuf};

/// The default client port
fn default_client_port() -> u16 {
    12000
}

/// The default data peer port
fn default_peer_port() -> u16 {
    12001
}

/// The default control port
fn default_control_port() -> u16 {
    12002
}

/// The default replication factor
fn default_replication_factor() -> u32 {
    3
}

/// The default number of control voters
fn default_control_voters() -> u32 {
    3
}

/// The default memory limit for every node
fn default_memory() -> String {
    "4Gi".to_string()
}

/// The default tracing level for every node
fn default_tracing() -> String {
    "Info".to_string()
}

/// The default admin principal
fn default_admin() -> String {
    "admin".to_string()
}

/// The ports every node of a deployment listens on
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Ports {
    /// Where clients connect
    #[serde(default = "default_client_port")]
    pub client: u16,
    /// Where data peers connect
    #[serde(default = "default_peer_port")]
    pub peer: u16,
    /// Where control peers connect, and so what a seed names
    #[serde(default = "default_control_port")]
    pub control: u16,
}

impl Default for Ports {
    /// The ports every other Shoal page writes down
    fn default() -> Self {
        Ports {
            client: default_client_port(),
            peer: default_peer_port(),
            control: default_control_port(),
        }
    }
}

/// The resources a node is given on its host
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    /// How many cores run shards, or every core the control plane leaves if none
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cores: Option<usize>,
    /// Physical cores to keep the node off
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_cores: Vec<usize>,
    /// The memory limit, in the units `shoal.yml` takes
    #[serde(default = "default_memory")]
    pub memory: String,
    /// Whether the control thread may share its physical core with a shard
    #[serde(default)]
    pub control_core_shared: bool,
}

impl Default for Resources {
    /// Every core and four gibibytes
    fn default() -> Self {
        Resources {
            cores: None,
            exclude_cores: Vec::new(),
            memory: default_memory(),
            control_core_shared: false,
        }
    }
}

/// Where a node keeps its data, at any of the three levels an inventory sets it
///
/// Each field is looked up on the node, then its group, then the deployment, on its own
/// ([F53](../../../docs/src/features/inventory-wizard.md)): a group that names only a
/// `throughput` directory keeps whatever `latency` the deployment gives.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StorageSpec {
    /// Where the intent logs, archive maps and the node's marker live (`latency_sensitive`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<String>,
    /// Where the archives live (`throughput_sensitive`), the latency directory if none is set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput: Option<String>,
}

impl StorageSpec {
    /// Whether this sets nothing, so it is left out of a written inventory
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.latency.is_none() && self.throughput.is_none()
    }
}

/// Settings every node naming this group shares, so they are written once
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GroupSpec {
    /// The resources its nodes are given unless a node says otherwise, replacing the deployment's
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    /// Where its nodes keep their data, field by field over the deployment's
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSpec>,
}

/// Where a resolved node keeps its data
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStorage {
    /// The latency sensitive directory, which holds the marker and so is the primary root
    pub latency: String,
    /// The throughput sensitive directory
    pub throughput: String,
}

impl NodeStorage {
    /// Every distinct directory this node writes under, the primary first
    ///
    /// The same order `Storage::roots` in shoal-core gives the rendered file.
    #[must_use]
    pub fn roots(&self) -> Vec<String> {
        // the primary always, and the other only when it is another directory
        let mut roots = vec![self.latency.clone()];
        if self.throughput != self.latency {
            roots.push(self.throughput.clone());
        }
        roots
    }
}

/// Where a setting a node resolved came from, so the wizard can say so
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// The node set it itself
    Node,
    /// The node's group set it
    Group(String),
    /// The deployment set it
    Deployment,
    /// Nothing set it, so it is the default
    Default,
}

impl std::fmt::Display for Source {
    /// Name the level a setting came from
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::Node => write!(f, "node"),
            Source::Group(group) => write!(f, "group {group}"),
            Source::Deployment => write!(f, "deployment"),
            Source::Default => write!(f, "default"),
        }
    }
}

/// One host of a deployment
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    /// The name the operator calls this node by, which is also the ssh target unless `ssh` is set
    pub name: String,
    /// What ssh and scp are given: `user@host` or `host`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh: Option<String>,
    /// The address peers and clients reach this node at, resolved from the name if not given
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<IpAddr>,
    /// The group this node takes its resources and storage from, where it sets none itself
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// This node's resources, if they differ from its group's or the deployment's
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    /// Where this node keeps its data, field by field over its group's and the deployment's
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSpec>,
}

impl NodeSpec {
    /// What ssh and scp are given for this node
    #[must_use]
    pub fn target(&self) -> &str {
        self.ssh.as_deref().unwrap_or(&self.name)
    }
}

/// A cluster to deploy
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    /// The cluster's name, which names its unit, its remote directory and its local state
    pub name: String,
    /// The server program to deploy: a build of `shoal::server::node::main` for the schema
    pub server: PathBuf,
    /// Where on every host the node's binary, configuration and data live
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_dir: Option<String>,
    /// The ports every node listens on
    #[serde(default)]
    pub ports: Ports,
    /// How many replicas each tablet is meant to have
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    /// How many nodes vote in the control group
    #[serde(default = "default_control_voters")]
    pub control_voters: u32,
    /// The resources every node is given unless it or its group says otherwise
    #[serde(default)]
    pub resources: Resources,
    /// Where every node keeps its data unless it or its group says otherwise; under
    /// `<remote_dir>/data` if nothing does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSpec>,
    /// Named sets of resources and storage a node takes by naming one
    /// ([F53](../../../docs/src/features/inventory-wizard.md))
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub groups: BTreeMap<String, GroupSpec>,
    /// The tracing level every node logs at
    #[serde(default = "default_tracing")]
    pub tracing: String,
    /// The system user every node runs as, created on a host that lacks it; the ssh user if absent
    ///
    /// io_uring charges the memory a ring locks to the user that created it, against the
    /// caller's `RLIMIT_MEMLOCK`, so a node running as the operator's own user spends that
    /// user's budget, and on a host the operator also develops on, every glommio test then fails
    /// at its io_uring probe. A user of its own keeps the two apart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// The principal the deployment authenticates as, and the cluster's one admin
    #[serde(default = "default_admin")]
    pub admin: String,
    /// How long a moved group's retired copy is kept before it is reclaimed, as `shoal.yml`
    /// writes a duration (`15s`, `5m`); the engine's default of five minutes if absent
    ///
    /// Rendered as `cluster.migration.retire_after`. A move finishes only once its source has
    /// reclaimed the copy, so this is the floor under every step of a rebalance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retire_after: Option<String>,
    /// The base data election timeout every group derives its timers from, as `shoal.yml` writes
    /// a duration (`1s`, `2500ms`); the engine's default of five seconds if absent
    ///
    /// Rendered as `cluster.primary_failover_after`. A crashed leader's groups refuse writes for
    /// its lease and an election, three to four times this, so it is the unplanned failover
    /// window ([cluster testing](../../../docs/src/cluster-testing/performance.md#failover-time-against-primary_failover_after)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failover: Option<String>,
    /// The nodes `bootstrap` forms the cluster from, in placement order; every node if absent
    ///
    /// The first is the node that mints the cluster. A node listed in `nodes` but not here is
    /// one `add` can join later.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<Vec<String>>,
    /// Every host this deployment may place a node on
    pub nodes: Vec<NodeSpec>,
}

/// A node of an inventory with everything the deployment needs resolved
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// The name the operator calls it by
    pub name: String,
    /// What ssh and scp are given
    pub target: String,
    /// The address peers and clients reach it at
    pub address: IpAddr,
    /// The group it named, if any
    pub group: Option<String>,
    /// The resources it is given
    pub resources: Resources,
    /// Where it keeps its data
    pub storage: NodeStorage,
}

impl Node {
    /// Where clients reach this node
    #[must_use]
    pub fn client_addr(&self, ports: &Ports) -> String {
        socket(self.address, ports.client)
    }

    /// Where control peers reach this node, which is what a seed names
    #[must_use]
    pub fn control_addr(&self, ports: &Ports) -> String {
        socket(self.address, ports.control)
    }
}

/// Write an address and a port the way a socket address parses
///
/// # Arguments
///
/// * `address` - The address
/// * `port` - The port
#[must_use]
pub fn socket(address: IpAddr, port: u16) -> String {
    std::net::SocketAddr::new(address, port).to_string()
}

impl Inventory {
    /// Read and validate an inventory
    ///
    /// # Arguments
    ///
    /// * `path` - The inventory file
    ///
    /// # Errors
    ///
    /// When the file cannot be read or parsed, or describes a cluster that cannot exist.
    pub fn load(path: &Path) -> color_eyre::Result<Self> {
        // the file as a cluster, then the program it deploys
        let inventory = Self::read(path)?;
        inventory.validate()?;
        Ok(inventory)
    }

    /// Read an inventory that describes a cluster without looking for its server program
    ///
    /// For a program that only talks to a deployed cluster, such as a client or a loader,
    /// which has no reason to hold the node binary it was deployed with.
    ///
    /// # Arguments
    ///
    /// * `path` - The inventory file
    ///
    /// # Errors
    ///
    /// When the file cannot be read or parsed, or describes a cluster that cannot exist.
    pub fn read(path: &Path) -> color_eyre::Result<Self> {
        // read and parse the file
        let raw = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read the inventory {}", path.display()))?;
        let mut inventory: Inventory = serde_yaml::from_str(&raw)
            .wrap_err_with(|| format!("{} is not an inventory", path.display()))?;
        // a relative server path is relative to the inventory, not to wherever we were run
        if inventory.server.is_relative() {
            if let Some(parent) = path.parent() {
                inventory.server = parent.join(&inventory.server);
            }
        }
        inventory.validate_shape()?;
        Ok(inventory)
    }

    /// Refuse an inventory that describes a cluster that cannot exist, or names a program that
    /// has not been built
    ///
    /// # Errors
    ///
    /// Names the first thing wrong with it.
    pub fn validate(&self) -> color_eyre::Result<()> {
        // everything the file itself says
        self.validate_shape()?;
        // the server binary is copied from here, so it has to be here
        if !self.server.is_file() {
            bail!(
                "the server program {} does not exist; build it first",
                self.server.display()
            );
        }
        // and it has to be a program: a source file passes the check above and dies on every host
        if !is_executable(&self.server) {
            bail!(
                "the server program {} is not executable; name the built node program, not its source",
                self.server.display()
            );
        }
        Ok(())
    }

    /// Refuse an inventory that describes a cluster that cannot exist, without looking for the
    /// server program
    ///
    /// The wizard judges a draft with this, since an inventory is commonly written before the
    /// program it names is built.
    ///
    /// # Errors
    ///
    /// Names the first thing wrong with it.
    pub fn validate_shape(&self) -> color_eyre::Result<()> {
        // the name is part of a unit name and a path on every host
        if !is_plain_name(&self.name) {
            bail!(
                "the cluster name {:?} has to be non-empty ascii letters, digits, '-' or '_'",
                self.name
            );
        }
        // the control group is one, three or five voters and nothing else
        if !matches!(self.control_voters, 1 | 3 | 5) {
            bail!(
                "control_voters is {}; a control group has 1, 3 or 5 voters",
                self.control_voters
            );
        }
        // a factor of zero places nothing
        if self.replication_factor == 0 {
            bail!("replication_factor has to be at least 1");
        }
        // every name is unique, since it keys the local state
        let mut names = BTreeSet::new();
        for node in &self.nodes {
            if !names.insert(node.name.as_str()) {
                bail!("the node {:?} is listed twice", node.name);
            }
        }
        // every address that is given is unique, since one host runs one node of a cluster
        let mut addresses = BTreeSet::new();
        for address in self.nodes.iter().filter_map(|node| node.address) {
            if !addresses.insert(address) {
                bail!("the address {address} is listed twice");
            }
        }
        // the bootstrap set names nodes that exist, once each
        let bootstrap = self.bootstrap_names();
        if bootstrap.is_empty() {
            bail!("a cluster needs at least one node to bootstrap");
        }
        let mut seen = BTreeSet::new();
        for name in &bootstrap {
            if !names.contains(name.as_str()) {
                bail!("bootstrap names {name:?}, which is not in nodes");
            }
            if !seen.insert(name.as_str()) {
                bail!("bootstrap names {name:?} twice");
            }
        }
        // initialize places every tablet at the factor over the bootstrap set, so it has to fit
        if self.replication_factor as usize > bootstrap.len() {
            bail!(
                "replication_factor is {} but bootstrap forms the cluster from {} node{}",
                self.replication_factor,
                bootstrap.len(),
                if bootstrap.len() == 1 { "" } else { "s" }
            );
        }
        // the three ports are three listeners on the same host
        let ports = [self.ports.client, self.ports.peer, self.ports.control];
        if ports.iter().collect::<BTreeSet<_>>().len() != ports.len() {
            bail!("the client, peer and control ports have to differ");
        }
        // a user name that is one word a shell and useradd both take
        if let Some(user) = &self.user {
            if user.is_empty()
                || user.starts_with('-')
                || !user
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
            {
                bail!("user {user:?} has to be lowercase ascii letters, digits, '-' or '_'");
            }
        }
        // a duration the engine can read: a whole number and one of its four units
        if let Some(retire_after) = &self.retire_after {
            let split = retire_after
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(retire_after.len());
            let (number, unit) = retire_after.split_at(split);
            if number.is_empty() || !matches!(unit, "ms" | "s" | "m" | "h") {
                bail!("retire_after is {retire_after:?}; write it as 500ms, 15s, 5m or 1h");
            }
        }
        // the failover base the same way, and at least the 100ms the engine requires
        if let Some(failover) = &self.failover {
            let split = failover
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(failover.len());
            let (number, unit) = failover.split_at(split);
            let millis = number.parse::<u64>().ok().and_then(|number| match unit {
                "ms" => Some(number),
                "s" => number.checked_mul(1_000),
                "m" => number.checked_mul(60_000),
                _ => None,
            });
            match millis {
                Some(millis) if millis >= 100 => {}
                Some(_) => bail!("failover is {failover:?}; the engine needs at least 100ms"),
                None => bail!("failover is {failover:?}; write it as 1500ms, 2s or 1m"),
            }
        }
        // a group name is a key an operator types, so it is held to the cluster name's alphabet
        for name in self.groups.keys() {
            if !is_plain_name(name) {
                bail!("the group name {name:?} has to be non-empty ascii letters, digits, '-' or '_'");
            }
        }
        // the remote directory is removed whole by destroy, so it is held to the storage rules
        let remote_dir = self.remote_dir();
        check_dir("remote_dir", &remote_dir)?;
        for spec in &self.nodes {
            // a group named has to exist
            if let Some(group) = &spec.group {
                if !self.groups.contains_key(group) {
                    bail!("the node {:?} names the group {group:?}, which is not in groups", spec.name);
                }
            }
            // and every directory it resolves is one destroy and --wipe can delete safely
            let (storage, _) = self.resolve_storage(spec);
            for (field, path) in [("latency", &storage.latency), ("throughput", &storage.throughput)] {
                let what = format!("the node {:?}'s {field} storage", spec.name);
                check_dir(&what, path)?;
                check_clear_of(&what, path, &remote_dir)?;
            }
            // two roots one inside the other would be deleted and locked twice
            if storage.latency != storage.throughput
                && (is_within(&storage.latency, &storage.throughput)
                    || is_within(&storage.throughput, &storage.latency))
            {
                bail!(
                    "the node {:?}'s latency storage {} and throughput storage {} are nested; \
                     give them the same directory or two apart",
                    spec.name,
                    storage.latency,
                    storage.throughput
                );
            }
        }
        Ok(())
    }

    /// The directory a node's data lives under when nothing names one
    #[must_use]
    pub fn default_data_dir(&self) -> String {
        format!("{}/data", self.remote_dir())
    }

    /// The group a node names, if it names one that exists
    ///
    /// # Arguments
    ///
    /// * `spec` - The node
    fn group_of(&self, spec: &NodeSpec) -> Option<(&String, &GroupSpec)> {
        spec.group
            .as_ref()
            .and_then(|name| self.groups.get_key_value(name))
    }

    /// Resolve where a node keeps its data, field by field over its group and the deployment
    ///
    /// Needs no address, so validation and destroy use it as well as [`Inventory::node`].
    ///
    /// # Arguments
    ///
    /// * `spec` - The node
    #[must_use]
    pub fn resolve_storage(&self, spec: &NodeSpec) -> (NodeStorage, [Source; 2]) {
        // the three levels, most specific first, each with the source it would report
        let group = self.group_of(spec);
        let levels: [(Option<&StorageSpec>, Source); 3] = [
            (spec.storage.as_ref(), Source::Node),
            (
                group.and_then(|(_, group)| group.storage.as_ref()),
                Source::Group(group.map(|(name, _)| name.clone()).unwrap_or_default()),
            ),
            (self.storage.as_ref(), Source::Deployment),
        ];
        // the first level that sets a field wins it
        let pick = |field: fn(&StorageSpec) -> Option<&String>| {
            levels.iter().find_map(|(level, source)| {
                level
                    .and_then(|level| field(level))
                    .map(|path| (trim_dir(path), source.clone()))
            })
        };
        // latency falls back to the remote directory, and throughput to latency
        let (latency, latency_source) = pick(|level| level.latency.as_ref())
            .unwrap_or_else(|| (self.default_data_dir(), Source::Default));
        let (throughput, throughput_source) = pick(|level| level.throughput.as_ref())
            .unwrap_or_else(|| (latency.clone(), latency_source.clone()));
        (
            NodeStorage {
                latency,
                throughput,
            },
            [latency_source, throughput_source],
        )
    }

    /// Resolve the resources a node is given: its own, its group's or the deployment's, whole
    ///
    /// Whole rather than field by field because `memory` has a default, so a level that names
    /// no memory cannot be told from one that names four gibibytes.
    ///
    /// # Arguments
    ///
    /// * `spec` - The node
    #[must_use]
    pub fn resolve_resources(&self, spec: &NodeSpec) -> (Resources, Source) {
        // the node's own, then its group's, then the deployment's
        if let Some(resources) = &spec.resources {
            return (resources.clone(), Source::Node);
        }
        if let Some((name, GroupSpec {
            resources: Some(resources),
            ..
        })) = self.group_of(spec)
        {
            return (resources.clone(), Source::Group(name.clone()));
        }
        (self.resources.clone(), Source::Deployment)
    }

    /// The names bootstrap forms the cluster from, in placement order
    #[must_use]
    pub fn bootstrap_names(&self) -> Vec<String> {
        match &self.bootstrap {
            Some(names) => names.clone(),
            None => self.nodes.iter().map(|node| node.name.clone()).collect(),
        }
    }

    /// Where the node's files live on every host
    #[must_use]
    pub fn remote_dir(&self) -> String {
        self.remote_dir
            .clone()
            .unwrap_or_else(|| format!("/opt/shoal-deploy/{}", self.name))
    }

    /// The systemd unit every node of this cluster runs as
    #[must_use]
    pub fn unit_name(&self) -> String {
        format!("shoal-{}.service", self.name)
    }

    /// The file name the server program is copied to on every host
    ///
    /// # Errors
    ///
    /// When the server path names no file.
    pub fn server_name(&self) -> color_eyre::Result<String> {
        self.server
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .ok_or_else(|| eyre!("{} names no file", self.server.display()))
    }

    /// Resolve one node by name
    ///
    /// # Arguments
    ///
    /// * `name` - The node's name
    ///
    /// # Errors
    ///
    /// When no node has that name, or its address cannot be resolved.
    pub fn node(&self, name: &str) -> color_eyre::Result<Node> {
        // find the node the operator named
        let spec = self
            .nodes
            .iter()
            .find(|node| node.name == name)
            .ok_or_else(|| eyre!("the inventory lists no node named {name:?}"))?;
        // an address given wins, otherwise resolve the name here
        let address = match spec.address {
            Some(address) => address,
            None => resolve(&spec.name)?,
        };
        // what it runs with and where it keeps its data, over its group and the deployment
        let (resources, _) = self.resolve_resources(spec);
        let (storage, _) = self.resolve_storage(spec);
        Ok(Node {
            name: spec.name.clone(),
            target: spec.target().to_string(),
            address,
            group: spec.group.clone(),
            resources,
            storage,
        })
    }

    /// Resolve every node bootstrap forms the cluster from, in placement order
    ///
    /// # Errors
    ///
    /// When a node's address cannot be resolved, or two resolve to the same address.
    pub fn bootstrap_nodes(&self) -> color_eyre::Result<Vec<Node>> {
        // resolve each one in order
        let nodes = self
            .bootstrap_names()
            .iter()
            .map(|name| self.node(name))
            .collect::<color_eyre::Result<Vec<_>>>()?;
        // two names on one address would be two nodes on one host sharing ports
        let mut addresses = BTreeSet::new();
        for node in &nodes {
            if !addresses.insert(node.address) {
                bail!("{} resolves to {}, which another node already has", node.name, node.address);
            }
        }
        Ok(nodes)
    }
}

/// Whether a name is non-empty ascii letters, digits, '-' or '_'
///
/// # Arguments
///
/// * `name` - The name
#[must_use]
pub fn is_plain_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// A directory as it is written into a node's file, without a trailing slash
///
/// # Arguments
///
/// * `path` - The directory as the inventory wrote it
fn trim_dir(path: &str) -> String {
    // "/mnt/nvme/shoal/" and "/mnt/nvme/shoal" are one root, and "/" stays itself for the check
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        path.to_string()
    } else {
        trimmed.to_string()
    }
}

/// Refuse a directory `destroy` and `--wipe` could not delete safely
///
/// Every directory a node is given is removed whole with `rm -rf`, so it has to be absolute,
/// free of `..`, and at least two components deep: `/` and `/mnt` are refused by name.
///
/// # Arguments
///
/// * `what` - What the directory is, for the message
/// * `path` - The directory
///
/// # Errors
///
/// Names the rule it breaks.
pub fn check_dir(what: &str, path: &str) -> color_eyre::Result<()> {
    // relative to what? every host starts ssh somewhere else
    let parsed = Path::new(path);
    if !parsed.is_absolute() {
        bail!("{what} is {path:?}; it has to be an absolute path");
    }
    // a `..` would put the directory somewhere its name does not say
    if parsed
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        bail!("{what} is {path:?}; it may not contain '.' or '..'");
    }
    // destroy deletes it whole, so it may not be a whole filesystem's top
    let depth = parsed
        .components()
        .filter(|component| matches!(component, Component::Normal(_)))
        .count();
    if depth < 2 {
        bail!("{what} is {path:?}; it has to be at least two directories deep, like /mnt/shoal, since destroy deletes it");
    }
    Ok(())
}

/// Refuse a storage directory that overlaps the node's program or its keys
///
/// # Arguments
///
/// * `what` - What the directory is, for the message
/// * `path` - The directory
/// * `remote_dir` - The node's remote directory
///
/// # Errors
///
/// When the directory is the remote directory, holds it, or is inside `bin` or `tls`.
fn check_clear_of(what: &str, path: &str, remote_dir: &str) -> color_eyre::Result<()> {
    // the remote directory itself, or anything that holds it: wiping the data would take the
    // program, the configuration and the keys with it
    if is_within(remote_dir, path) {
        bail!("{what} is {path}, which holds {remote_dir}; give the data a directory of its own");
    }
    // the program's and the keys' directories are rewritten on every deploy
    for sub in ["bin", "tls"] {
        let reserved = format!("{remote_dir}/{sub}");
        if is_within(path, &reserved) || is_within(&reserved, path) {
            bail!("{what} is {path}, which overlaps {reserved}");
        }
    }
    Ok(())
}

/// Whether one directory is another or inside it, by whole components
///
/// # Arguments
///
/// * `inner` - The directory that may be inside
/// * `outer` - The directory that may hold it
#[must_use]
pub fn is_within(inner: &str, outer: &str) -> bool {
    Path::new(inner).starts_with(Path::new(outer))
}

/// Whether a file is one a host could run
///
/// Only the owner, group and other execute bits are read, which is what `install -m 0755` keeps
/// on every host; off unix every file is taken as runnable.
///
/// # Arguments
///
/// * `path` - The file
#[must_use]
pub fn is_executable(path: &Path) -> bool {
    // a file with any execute bit set, on unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
    }
    // anywhere else, a file
    #[cfg(not(unix))]
    {
        path.is_file()
    }
}

/// Every address a host name resolves to here, loopback included
///
/// # Arguments
///
/// * `host` - The name to resolve
///
/// # Errors
///
/// When the resolver has no answer for the name.
pub fn lookup(host: &str) -> color_eyre::Result<Vec<IpAddr>> {
    // ask the resolver for every address of the name
    let addresses = (host, 0)
        .to_socket_addrs()
        .wrap_err_with(|| format!("failed to resolve {host}"))?
        .map(|addr| addr.ip())
        .collect();
    Ok(addresses)
}

/// The address peers should dial out of everything a name resolves to, if any is not loopback
///
/// # Arguments
///
/// * `addresses` - What the name resolved to
#[must_use]
pub fn dialable(addresses: &[IpAddr]) -> Option<IpAddr> {
    // loopback is unreachable from every other host
    let remote: Vec<IpAddr> = addresses.iter().copied().filter(|ip| !ip.is_loopback()).collect();
    // prefer an IPv4 address, since that is what a lab network usually routes
    remote
        .iter()
        .find(|ip| ip.is_ipv4())
        .or_else(|| remote.first())
        .copied()
}

/// Resolve a host name to the address its peers should dial
///
/// A loopback answer is refused: `/etc/hosts` commonly maps a machine's own name to
/// `127.0.1.1`, and a node advertising that would be unreachable from every other host.
///
/// # Arguments
///
/// * `host` - The name to resolve
///
/// # Errors
///
/// When the name resolves to nothing but loopback addresses.
pub fn resolve(host: &str) -> color_eyre::Result<IpAddr> {
    // everything the name resolves to, then the one a peer can dial
    let addresses = lookup(host)?;
    dialable(&addresses).ok_or_else(|| {
        eyre!("{host} resolves only to loopback addresses; give it an address in the inventory")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse an inventory from YAML with a server path that exists
    ///
    /// # Arguments
    ///
    /// * `yaml` - The inventory, without its `server` line
    fn parse(yaml: &str) -> Inventory {
        // this test binary always exists, so it stands in for a server program
        let server = std::env::current_exe().expect("the test binary");
        let yaml = format!("server: {}\n{yaml}", server.display());
        serde_yaml::from_str(&yaml).expect("an inventory")
    }

    /// A three node inventory with every default
    const THREE: &str = "name: lab\nnodes:\n  - {name: a, address: 10.0.0.1}\n  - {name: b, address: 10.0.0.2}\n  - {name: c, address: 10.0.0.3}\n";

    /// An inventory that says nothing gets the cluster every other page documents
    #[test]
    fn an_inventory_defaults_to_the_documented_cluster() {
        // every default is the one the configuration page writes down
        let inventory = parse(THREE);
        inventory.validate().expect("a valid inventory");
        assert_eq!(inventory.ports, Ports::default());
        assert_eq!(inventory.replication_factor, 3);
        assert_eq!(inventory.control_voters, 3);
        assert_eq!(inventory.remote_dir(), "/opt/shoal-deploy/lab");
        assert_eq!(inventory.unit_name(), "shoal-lab.service");
        assert_eq!(inventory.bootstrap_names(), vec!["a", "b", "c"]);
        // an address given is used as is, and the name is the ssh target
        let node = inventory.node("b").expect("node b");
        assert_eq!(node.target, "b");
        assert_eq!(node.control_addr(&inventory.ports), "10.0.0.2:12002");
        assert_eq!(node.client_addr(&inventory.ports), "10.0.0.2:12000");
    }

    /// Every inventory that describes an impossible cluster is refused by name
    #[test]
    fn an_inventory_that_cannot_be_a_cluster_is_refused() {
        // a control group of two cannot elect through the loss of either
        let mut inventory = parse(THREE);
        inventory.control_voters = 2;
        assert!(inventory.validate().unwrap_err().to_string().contains("1, 3 or 5"));
        // a factor above the bootstrap set cannot be placed by initialize
        let mut inventory = parse(THREE);
        inventory.bootstrap = Some(vec!["a".into(), "b".into()]);
        assert!(inventory
            .validate()
            .unwrap_err()
            .to_string()
            .contains("replication_factor is 3"));
        // at a factor that fits, the same bootstrap set is fine and c is left for add
        inventory.replication_factor = 2;
        inventory.validate().expect("a two node bootstrap at factor two");
        // a failover base is a duration of at least the engine's hundred milliseconds
        let mut inventory = parse(THREE);
        inventory.failover = Some("2s".into());
        inventory.validate().expect("a two second failover base");
        inventory.failover = Some("1500ms".into());
        inventory.validate().expect("a millisecond failover base");
        inventory.failover = Some("50ms".into());
        assert!(inventory.validate().unwrap_err().to_string().contains("100ms"));
        inventory.failover = Some("2".into());
        assert!(inventory.validate().unwrap_err().to_string().contains("failover"));
        // a bootstrap set naming a node that is not listed
        inventory.bootstrap = Some(vec!["a".into(), "z".into()]);
        assert!(inventory.validate().unwrap_err().to_string().contains("\"z\""));
        // a name listed twice
        let inventory = parse("name: lab\nreplication_factor: 1\nnodes:\n  - {name: a}\n  - {name: a}\n");
        assert!(inventory.validate().unwrap_err().to_string().contains("twice"));
        // an address listed twice
        let inventory = parse("name: lab\nreplication_factor: 1\nnodes:\n  - {name: a, address: 10.0.0.1}\n  - {name: b, address: 10.0.0.1}\n");
        assert!(inventory.validate().unwrap_err().to_string().contains("twice"));
        // a user name useradd would refuse, or a shell would split
        let mut inventory = parse(THREE);
        inventory.user = Some("Shoal Node".into());
        assert!(inventory.validate().unwrap_err().to_string().contains("user"));
        inventory.user = Some("shoal".into());
        inventory.validate().expect("a system user name");
        // a duration the engine could not read
        let mut inventory = parse(THREE);
        inventory.retire_after = Some("15".into());
        assert!(inventory.validate().unwrap_err().to_string().contains("retire_after"));
        inventory.retire_after = Some("15s".into());
        inventory.validate().expect("a duration with a unit");
        // a name that cannot be part of a unit name
        let mut inventory = parse(THREE);
        inventory.name = "my lab".into();
        assert!(inventory.validate().is_err());
        // two listeners on one port
        let mut inventory = parse(THREE);
        inventory.ports.peer = inventory.ports.client;
        assert!(inventory.validate().unwrap_err().to_string().contains("ports"));
        // a server program that has not been built
        let mut inventory = parse(THREE);
        inventory.server = PathBuf::from("/nonexistent/shoal-node");
        assert!(inventory.validate().unwrap_err().to_string().contains("build it first"));
        // a source file where the program belongs, as the wizard once wrote (item 127)
        let source = tempfile::NamedTempFile::new().expect("a temp file");
        let mut inventory = parse(THREE);
        inventory.server = source.path().to_path_buf();
        assert!(inventory.validate().unwrap_err().to_string().contains("not executable"));
        // an unknown key is a typo, not a setting
        let server = std::env::current_exe().expect("the test binary");
        let yaml = format!("server: {}\nname: lab\nreplication_facter: 1\nnodes: []\n", server.display());
        assert!(serde_yaml::from_str::<Inventory>(&yaml).is_err());
    }

    /// A node never advertises a loopback address, and its own resources win
    #[test]
    fn a_node_resolves_off_the_loopback() {
        // localhost has nothing but loopback addresses, which no peer could dial
        assert!(resolve("localhost").unwrap_err().to_string().contains("loopback"));
        // which the lookup the wizard judges with still sees, so it can say why
        let addresses = lookup("localhost").expect("localhost resolves");
        assert!(!addresses.is_empty() && addresses.iter().all(IpAddr::is_loopback));
        assert_eq!(dialable(&addresses), None);
        // and a routable address wins over loopback, IPv4 over IPv6
        let v4: IpAddr = "10.0.0.1".parse().unwrap();
        let v6: IpAddr = "fd00::1".parse().unwrap();
        let loopback: IpAddr = "127.0.1.1".parse().unwrap();
        assert_eq!(dialable(&[loopback, v6, v4]), Some(v4));
        assert_eq!(dialable(&[loopback, v6]), Some(v6));
        // a node's own resources replace the deployment's whole
        let inventory = parse("name: lab\nreplication_factor: 1\nresources: {cores: 4}\nnodes:\n  - {name: a, address: 10.0.0.1, ssh: ops@a.lab, resources: {cores: 2, memory: 1Gi}}\n");
        let node = inventory.node("a").expect("node a");
        assert_eq!(node.target, "ops@a.lab");
        assert_eq!(node.resources.cores, Some(2));
        assert_eq!(node.resources.memory, "1Gi");
        assert!(inventory.node("b").is_err());
    }
    /// A group gives a node what the node does not set, and the deployment what the group does not
    #[test]
    fn a_group_supplies_what_a_node_does_not_set() {
        // the deployment splits nothing, the group names fast logs, one node names its own archives
        let inventory = parse(
            "name: lab\nreplication_factor: 1\nstorage: {latency: /srv/shoal/logs, throughput: /srv/shoal/archive}\n\
             groups:\n  small: {storage: {latency: /mnt/nvme/shoal}}\n\
             nodes:\n  - {name: a, address: 10.0.0.1, group: small}\n  - {name: b, address: 10.0.0.2, group: small, storage: {throughput: /mnt/hdd/shoal/}}\n  - {name: c, address: 10.0.0.3}\n",
        );
        inventory.validate().expect("a valid inventory");
        // a takes the group's latency and, field by field, the deployment's throughput
        let a = inventory.node("a").unwrap();
        assert_eq!(a.group.as_deref(), Some("small"));
        assert_eq!(a.storage.latency, "/mnt/nvme/shoal");
        assert_eq!(a.storage.throughput, "/srv/shoal/archive");
        let (_, sources) = inventory.resolve_storage(&inventory.nodes[0]);
        assert_eq!(sources, [Source::Group("small".into()), Source::Deployment]);
        // b's own throughput wins, written without its trailing slash
        let b = inventory.node("b").unwrap();
        assert_eq!(b.storage.latency, "/mnt/nvme/shoal");
        assert_eq!(b.storage.throughput, "/mnt/hdd/shoal");
        assert_eq!(b.storage.roots(), vec!["/mnt/nvme/shoal", "/mnt/hdd/shoal"]);
        // c is in no group and takes the deployment's two
        let c = inventory.node("c").unwrap();
        assert_eq!(c.storage.latency, "/srv/shoal/logs");
        // with nothing set anywhere, both are the remote directory's data, as before F53
        let plain = parse(THREE);
        let a = plain.node("a").unwrap();
        assert_eq!(a.storage.latency, "/opt/shoal-deploy/lab/data");
        assert_eq!(a.storage.throughput, "/opt/shoal-deploy/lab/data");
        assert_eq!(a.storage.roots(), vec!["/opt/shoal-deploy/lab/data"]);
        // and a latency set alone carries the archives with it
        let one = parse("name: lab\nreplication_factor: 1\nstorage: {latency: /mnt/nvme/shoal}\nnodes:\n  - {name: a, address: 10.0.0.1}\n");
        let (storage, sources) = one.resolve_storage(&one.nodes[0]);
        assert_eq!(storage.throughput, "/mnt/nvme/shoal");
        assert_eq!(sources, [Source::Deployment, Source::Deployment]);
    }

    /// A group's resources replace the deployment's whole, and a node's replace the group's
    #[test]
    fn a_group_replaces_resources_whole() {
        // the deployment names cores and memory, the group only cores
        let inventory = parse(
            "name: lab\nreplication_factor: 1\nresources: {cores: 12, memory: 16Gi}\n\
             groups:\n  small: {resources: {cores: 4}}\n\
             nodes:\n  - {name: a, address: 10.0.0.1, group: small}\n  - {name: b, address: 10.0.0.2, group: small, resources: {cores: 2, memory: 1Gi}}\n  - {name: c, address: 10.0.0.3}\n",
        );
        inventory.validate().expect("a valid inventory");
        // a takes the group's whole: its cores and the default memory, not the deployment's
        let a = inventory.node("a").unwrap();
        assert_eq!(a.resources.cores, Some(4));
        assert_eq!(a.resources.memory, "4Gi");
        assert_eq!(
            inventory.resolve_resources(&inventory.nodes[0]).1,
            Source::Group("small".into())
        );
        // b's own win over its group's
        let b = inventory.node("b").unwrap();
        assert_eq!(b.resources.cores, Some(2));
        assert_eq!(b.resources.memory, "1Gi");
        // c has no group and takes the deployment's
        let c = inventory.node("c").unwrap();
        assert_eq!(c.resources.cores, Some(12));
        assert_eq!(c.resources.memory, "16Gi");
    }

    /// Every storage directory destroy could delete wrongly, and every group that is not there,
    /// is refused by name
    #[test]
    fn a_storage_root_that_destroy_could_misuse_is_refused() {
        // an inventory with the given deployment-wide storage line
        let with = |storage: &str| {
            parse(&format!("name: lab\nreplication_factor: 1\nstorage: {storage}\nnodes:\n  - {{name: a, address: 10.0.0.1}}\n"))
        };
        // a relative directory, a whole filesystem, a mount point and a `..`
        let refused = [
            ("{latency: shoal/data}", "absolute"),
            ("{latency: /}", "two directories deep"),
            ("{latency: /mnt}", "two directories deep"),
            ("{latency: /mnt/../etc/shoal}", "'..'"),
            // the remote directory, what holds it, and the program's and the keys' directories
            ("{latency: /opt/shoal-deploy/lab}", "holds /opt/shoal-deploy/lab"),
            ("{latency: /opt/shoal-deploy}", "holds /opt/shoal-deploy/lab"),
            ("{latency: /opt/shoal-deploy/lab/bin}", "overlaps"),
            ("{throughput: /opt/shoal-deploy/lab/tls/archive}", "overlaps"),
            // one root inside the other
            ("{latency: /mnt/nvme/shoal, throughput: /mnt/nvme/shoal/archive}", "nested"),
        ];
        for (storage, reason) in refused {
            let error = with(storage).validate().unwrap_err().to_string();
            assert!(error.contains(reason), "{storage} was refused with {error:?}, not {reason:?}");
        }
        // a directory beside the remote one's data, and two apart, are fine
        with("{latency: /opt/shoal-deploy/lab/logs}").validate().expect("a sibling of data");
        with("{latency: /mnt/nvme/shoal, throughput: /mnt/nvmeb/shoal}").validate().expect("two roots apart");
        // a group a node names that is not there
        let inventory = parse("name: lab\nreplication_factor: 1\nnodes:\n  - {name: a, address: 10.0.0.1, group: big}\n");
        assert!(inventory.validate().unwrap_err().to_string().contains("\"big\""));
        // a group whose name is no key an operator could type
        let inventory = parse("name: lab\nreplication_factor: 1\ngroups:\n  'big box': {}\nnodes:\n  - {name: a, address: 10.0.0.1}\n");
        assert!(inventory.validate().unwrap_err().to_string().contains("group name"));
        // an unknown key under a group is a typo, not a setting
        let server = std::env::current_exe().expect("the test binary");
        let yaml = format!("server: {}\nname: lab\ngroups:\n  a: {{storge: {{}}}}\nnodes: []\n", server.display());
        assert!(serde_yaml::from_str::<Inventory>(&yaml).is_err());
    }
}
