//! The `shoal.yml` each deployed node starts from
//!
//! A mirror of the fields of shoal-core's `Conf` a deployment writes, and nothing else: this
//! crate links the client half alone ([F15](../../../docs/src/features/client-server-split.md)),
//! so it cannot name `Conf`. What keeps the mirror honest is a test in shoal-bench that parses
//! every file rendered here as a `Conf` and validates its cluster block; a field renamed on one
//! side fails it there, because `Conf` refuses unknown keys in every section written here.

use serde::Serialize;
use shoal::shared::auth::{StoredCredential, DEFAULT_ITERATIONS};
use std::collections::BTreeMap;

use super::inventory::{Inventory, Node};

/// The file a node's configuration is written to under its remote directory
pub const CONF_FILE: &str = "shoal.yml";

/// How a node enters its cluster
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    /// This node mints the cluster
    Bootstrap,
    /// This node joins through these control addresses
    Join(Vec<String>),
}

/// A node's whole configuration file
#[derive(Serialize, Debug)]
pub struct NodeConf {
    /// The cores and memory it runs with
    pub resources: ResourcesConf,
    /// Where clients reach it
    pub networking: NetworkingConf,
    /// Who clients have to be
    pub auth: AuthConf,
    /// Where its data lives
    pub storage: StorageConf,
    /// How much it logs
    pub tracing: TracingConf,
    /// Which cluster it is a member of, and how
    pub cluster: ClusterConf,
}

/// The `resources:` section
#[derive(Serialize, Debug)]
pub struct ResourcesConf {
    /// How many cores run shards
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cores: Option<usize>,
    /// Physical cores to keep off
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub exclude_cores: Vec<usize>,
    /// The memory limit
    pub memory: String,
}

/// The `networking:` section
#[derive(Serialize, Debug)]
pub struct NetworkingConf {
    /// Every interface, since peers and clients reach the node from other hosts
    pub interface: String,
    /// The client port
    pub port: u16,
}

/// The `auth:` section
#[derive(Serialize, Debug)]
pub struct AuthConf {
    /// Every client has to authenticate
    pub required: bool,
    /// The one user the deployment adds, the cluster's admin
    pub users: BTreeMap<String, UserConf>,
}

/// One user under `auth.users`
#[derive(Serialize, Debug)]
pub struct UserConf {
    /// A derived credential, so no password is on the host
    pub scram_sha_256: StoredCredential,
}

/// The `storage:` section
#[derive(Serialize, Debug)]
pub struct StorageConf {
    /// The default storage every table uses
    pub default: StorageDefault,
}

/// `storage.default`
#[derive(Serialize, Debug)]
pub struct StorageDefault {
    /// The filesystem engine's paths
    pub filesystem: FilesystemConf,
}

/// `storage.default.filesystem`
#[derive(Serialize, Debug)]
pub struct FilesystemConf {
    /// Where the intent logs live
    pub latency_sensitive: PathConf,
    /// Where the archives live
    pub throughput_sensitive: PathConf,
}

/// One storage path
#[derive(Serialize, Debug)]
pub struct PathConf {
    /// The directory
    pub path: String,
}

/// The `tracing:` section
#[derive(Serialize, Debug)]
pub struct TracingConf {
    /// The level
    pub level: String,
}

/// The `cluster:` section
#[derive(Serialize, Debug)]
pub struct ClusterConf {
    /// Whether this node mints the cluster
    pub bootstrap: bool,
    /// The control addresses a joiner joins through
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub seeds: Vec<String>,
    /// The address peers reach this node at
    pub advertise: String,
    /// The address clients are told to reach this node at
    pub client_advertise: String,
    /// The data peer port
    pub port: u16,
    /// The control port
    pub control_port: u16,
    /// Whether the control thread may share a physical core with a shard
    pub control_core_shared: bool,
    /// How many nodes vote in the control group
    pub control_voters: u32,
    /// How many replicas every tablet is meant to have
    pub replication_factor: u32,
    /// The principals allowed to change the cluster
    pub admins: Vec<String>,
    /// The peer lanes' certificate, key and authority
    pub tls: TlsConf,
    /// The move settings the inventory names, if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migration: Option<MigrationConf>,
}

/// `cluster.migration`, only the keys an inventory can set
#[derive(Serialize, Debug)]
pub struct MigrationConf {
    /// How long a retired copy is kept before it is reclaimed
    pub retire_after: String,
}

/// `cluster.tls`
#[derive(Serialize, Debug)]
pub struct TlsConf {
    /// This node's leaf
    pub cert: String,
    /// Its key
    pub key: String,
    /// The authority every node trusts
    pub ca: String,
    /// A leaf has to name its node
    pub bind_identity: bool,
}

/// Where a node's files live under its remote directory
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    /// The remote directory
    pub dir: String,
}

impl Layout {
    /// The server program
    ///
    /// # Arguments
    ///
    /// * `server` - The program's file name
    #[must_use]
    pub fn binary(&self, server: &str) -> String {
        format!("{}/bin/{server}", self.dir)
    }

    /// The configuration file
    #[must_use]
    pub fn conf(&self) -> String {
        format!("{}/{CONF_FILE}", self.dir)
    }

    /// The storage directory
    #[must_use]
    pub fn data(&self) -> String {
        format!("{}/data", self.dir)
    }

    /// The peer TLS directory
    #[must_use]
    pub fn tls(&self) -> String {
        format!("{}/tls", self.dir)
    }

    /// The node's leaf
    #[must_use]
    pub fn cert(&self) -> String {
        format!("{}/node.pem", self.tls())
    }

    /// The leaf's key
    #[must_use]
    pub fn key(&self) -> String {
        format!("{}/node.key", self.tls())
    }

    /// The authority
    #[must_use]
    pub fn ca(&self) -> String {
        format!("{}/ca.pem", self.tls())
    }
}

/// Build a node's configuration
///
/// # Arguments
///
/// * `inventory` - The deployment
/// * `node` - The node it is for
/// * `entry` - Whether it mints the cluster or joins it
/// * `password` - The admin password, derived here and never written
#[must_use]
pub fn node_conf(inventory: &Inventory, node: &Node, entry: &Entry, password: &str) -> NodeConf {
    // where this node's files go on its host
    let layout = Layout {
        dir: inventory.remote_dir(),
    };
    // bootstrap or seeds, never both
    let (bootstrap, seeds) = match entry {
        Entry::Bootstrap => (true, Vec::new()),
        Entry::Join(seeds) => (false, seeds.clone()),
    };
    // the admin's credential, derived so the host holds no password
    let mut users = BTreeMap::new();
    users.insert(
        inventory.admin.clone(),
        UserConf {
            scram_sha_256: StoredCredential::from_password(password, DEFAULT_ITERATIONS),
        },
    );
    NodeConf {
        resources: ResourcesConf {
            cores: node.resources.cores,
            exclude_cores: node.resources.exclude_cores.clone(),
            memory: node.resources.memory.clone(),
        },
        networking: NetworkingConf {
            interface: "0.0.0.0".to_string(),
            port: inventory.ports.client,
        },
        auth: AuthConf {
            required: true,
            users,
        },
        storage: StorageConf {
            default: StorageDefault {
                filesystem: FilesystemConf {
                    latency_sensitive: PathConf { path: layout.data() },
                    throughput_sensitive: PathConf { path: layout.data() },
                },
            },
        },
        tracing: TracingConf {
            level: inventory.tracing.clone(),
        },
        cluster: ClusterConf {
            bootstrap,
            seeds,
            advertise: node.address.to_string(),
            client_advertise: node.client_addr(&inventory.ports),
            port: inventory.ports.peer,
            control_port: inventory.ports.control,
            control_core_shared: node.resources.control_core_shared,
            control_voters: inventory.control_voters,
            replication_factor: inventory.replication_factor,
            admins: vec![inventory.admin.clone()],
            tls: TlsConf {
                cert: layout.cert(),
                key: layout.key(),
                ca: layout.ca(),
                bind_identity: true,
            },
            migration: inventory
                .retire_after
                .clone()
                .map(|retire_after| MigrationConf { retire_after }),
        },
    }
}

/// Render a node's configuration file
///
/// # Arguments
///
/// * `inventory` - The deployment
/// * `node` - The node it is for
/// * `entry` - Whether it mints the cluster or joins it
/// * `password` - The admin password, derived here and never written
///
/// # Errors
///
/// When the configuration cannot be written as YAML.
pub fn render(
    inventory: &Inventory,
    node: &Node,
    entry: &Entry,
    password: &str,
) -> color_eyre::Result<String> {
    // a header saying where the file came from, then the file
    let body = serde_yaml::to_string(&node_conf(inventory, node, entry, password))?;
    Ok(format!(
        "# written by shoalctl cluster for {} node {}; rewritten on every deploy\n{body}",
        inventory.name, node.name
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deploy::inventory::Resources;

    /// A bootstrap file mints and a join file seeds, and neither holds the password
    #[test]
    fn a_node_file_says_how_it_enters_and_holds_no_password() {
        // a two node inventory
        let server = std::env::current_exe().expect("the test binary");
        let inventory: Inventory = serde_yaml::from_str(&format!(
            "server: {}\nname: lab\nreplication_factor: 2\nnodes:\n  - {{name: a, address: 10.0.0.1}}\n  - {{name: b, address: 10.0.0.2}}\n",
            server.display()
        ))
        .expect("an inventory");
        let a = inventory.node("a").unwrap();
        let b = inventory.node("b").unwrap();
        // the bootstrapper mints and names no seeds
        let first = render(&inventory, &a, &Entry::Bootstrap, "hunter2").expect("a file");
        assert!(first.contains("bootstrap: true"));
        assert!(!first.contains("seeds"));
        assert!(first.contains("advertise: 10.0.0.1"));
        assert!(first.contains("client_advertise: 10.0.0.1:12000"));
        assert!(first.contains("path: /opt/shoal-deploy/lab/data"));
        assert!(first.contains("cert: /opt/shoal-deploy/lab/tls/node.pem"));
        // the joiner names the bootstrapper's control address
        let second = render(
            &inventory,
            &b,
            &Entry::Join(vec![a.control_addr(&inventory.ports)]),
            "hunter2",
        )
        .expect("a file");
        assert!(second.contains("bootstrap: false"));
        assert!(second.contains("- 10.0.0.1:12002"));
        // the password itself is on neither
        assert!(!first.contains("hunter2") && !second.contains("hunter2"));
        // a node's own resources are the ones written
        let mut small = b.clone();
        small.resources = Resources {
            cores: Some(2),
            ..Resources::default()
        };
        let third = render(&inventory, &small, &Entry::Bootstrap, "x").unwrap();
        assert!(third.contains("cores: 2"));
        // the move settings are left to the engine unless the inventory names them
        assert!(!first.contains("migration"));
        let mut quick = inventory.clone();
        quick.retire_after = Some("15s".into());
        let fourth = render(&quick, &a, &Entry::Bootstrap, "x").unwrap();
        assert!(fourth.contains("migration:\n    retire_after: 15s"));
    }
}
