//! What a deployment is asked to build: the hosts, the binary and the cluster's shape
//!
//! An inventory is a YAML file the operator writes once per cluster. It names the server
//! program to deploy, the hosts to deploy it to and how to reach them over ssh, and the policy
//! the cluster is bootstrapped with. Everything else - node ids, certificates, the admin
//! password - is minted by the deployment and kept in its [state](super::state).

use color_eyre::eyre::{bail, eyre, WrapErr};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};

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
    /// This node's resources, if they differ from the deployment's
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
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
    /// The resources every node is given unless it says otherwise
    #[serde(default)]
    pub resources: Resources,
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
    /// The resources it is given
    pub resources: Resources,
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
        inventory.validate()?;
        Ok(inventory)
    }

    /// Refuse an inventory that describes a cluster that cannot exist
    ///
    /// # Errors
    ///
    /// Names the first thing wrong with it.
    pub fn validate(&self) -> color_eyre::Result<()> {
        // the name is part of a unit name and a path on every host
        if self.name.is_empty()
            || !self
                .name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
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
        // the server binary is copied from here, so it has to be here
        if !self.server.is_file() {
            bail!(
                "the server program {} does not exist; build it first",
                self.server.display()
            );
        }
        Ok(())
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
        Ok(Node {
            name: spec.name.clone(),
            target: spec.target().to_string(),
            address,
            resources: spec
                .resources
                .clone()
                .unwrap_or_else(|| self.resources.clone()),
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
    // ask the resolver for every address of the name
    let addresses: Vec<IpAddr> = (host, 0)
        .to_socket_addrs()
        .wrap_err_with(|| format!("failed to resolve {host}"))?
        .map(|addr| addr.ip())
        .filter(|ip| !ip.is_loopback())
        .collect();
    // prefer an IPv4 address, since that is what a lab network usually routes
    addresses
        .iter()
        .find(|ip| ip.is_ipv4())
        .or_else(|| addresses.first())
        .copied()
        .ok_or_else(|| {
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
        // a node's own resources replace the deployment's whole
        let inventory = parse("name: lab\nreplication_factor: 1\nresources: {cores: 4}\nnodes:\n  - {name: a, address: 10.0.0.1, ssh: ops@a.lab, resources: {cores: 2, memory: 1Gi}}\n");
        let node = inventory.node("a").expect("node a");
        assert_eq!(node.target, "ops@a.lab");
        assert_eq!(node.resources.cores, Some(2));
        assert_eq!(node.resources.memory, "1Gi");
        assert!(inventory.node("b").is_err());
    }
}
