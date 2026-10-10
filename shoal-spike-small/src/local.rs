//! europa's three local nodes: a cluster over loopback, bounded by cores and the Optane
//!
//! Copied from X3's (`shoal-spike-bytes/src/local.rs`). X8's second place is "loopback on europa"
//! ([X8](../../docs/src/object-storage/spikes.md#x8-one-small-write-three-ways)), where a write is
//! bounded by cores and the Optane rather than by 1 GbE. A deployment cannot describe it: an inventory's ports, unit and remote directory are the
//! deployment's, so two nodes on one host would share all three. So this does what
//! `shoaladm bootstrap` does, step for step, for nodes that share a host:
//!
//! - each node's configuration is rendered by shoaladm's own `render`, so it is what a deploy
//!   would write, and then given what only a shared host needs: its own loopback address for
//!   every listener, its own control core, and its own directory for its leaf;
//! - each is claimed by the node program itself, as the user `shoal`, and given a leaf from one
//!   authority naming the id its claim printed, so its peer lanes run the lab's mutual TLS;
//! - each runs in a transient unit with the deployed unit's limits, the first alone until it leads
//!   a control group of one, then the other two, then `Initialize` over all three;
//! - what was started is saved as the inventory's deployment record, so the cluster is opened by
//!   [`crate::cluster::Lab::attach`] exactly as a deployed one is.

use std::path::Path;
use std::time::Instant;

use color_eyre::eyre::{bail, eyre, WrapErr};
use serde_yaml::Value;
use shoal::shared::identity::NodeId;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::uuid::Uuid;
use shoaladm::deploy::inventory::{Inventory, Node};
use shoaladm::deploy::pki::Authority;
use shoaladm::deploy::remote::{quote, Host};
use shoaladm::deploy::render::{self, Entry};
use shoaladm::deploy::state::{ClusterRecord, NodeRecord};
use shoaladm::deploy::Deployment;

use crate::SmallClient;

/// Where every local node's program, configuration, leaf and data live, a directory a node
pub const BASE: &str = "/optane/shoal-x8-local";

/// The user every node runs as, the lab's
pub const USER: &str = "shoal";

/// The control core of each local node, by its place in the inventory
///
/// Each is the first cpu of the four physical cores the node's inventory leaves it, so the
/// control thread has a whole core of its own and the six shards the other three and their
/// siblings, a lab Zen1 node's shape.
pub const CONTROL_CORES: [usize; 3] = [1, 5, 9];

/// The unit a local node runs in
///
/// # Arguments
///
/// * `name` - The node's name in the inventory
#[must_use]
pub fn unit_name(name: &str) -> String {
    format!("shoal-x8-local-{name}")
}

/// A local node's own directory
///
/// # Arguments
///
/// * `name` - The node's name in the inventory
#[must_use]
pub fn node_dir(name: &str) -> String {
    format!("{BASE}/{name}")
}

/// A node's rendered configuration, given what a node sharing a host needs
///
/// # Arguments
///
/// * `rendered` - The configuration shoaladm rendered for the node
/// * `node` - The node
/// * `control_core` - Its control core
///
/// # Errors
///
/// When the rendered configuration does not parse, or lacks a section a node always has.
pub fn localize(rendered: &str, node: &Node, control_core: usize) -> color_eyre::Result<String> {
    // the rendered file, as a document to change
    let mut conf: Value = serde_yaml::from_str(rendered).wrap_err("the rendered configuration")?;
    let address = node.address.to_string();
    let dir = node_dir(&node.name);
    // every listener on the node's own address: the client and peer ones bind the interface,
    // the control one the advertised address, which render already set to it
    set(&mut conf, &["networking", "interface"], Value::String(address))?;
    // a control core of its own, which an inventory has no field for
    set(
        &mut conf,
        &["cluster", "control_core"],
        Value::Number(serde_yaml::Number::from(control_core as u64)),
    )?;
    // and its leaf in its own directory, since the deployment's would be every node's
    for (field, file) in [("cert", "cert.pem"), ("key", "key.pem"), ("ca", "ca.pem")] {
        set(
            &mut conf,
            &["cluster", "tls", field],
            Value::String(format!("{dir}/tls/{file}")),
        )?;
    }
    Ok(serde_yaml::to_string(&conf)?)
}

/// Set a field of a document, under sections that already exist
///
/// # Arguments
///
/// * `doc` - The document
/// * `path` - The sections and then the field
/// * `value` - What it is set to
///
/// # Errors
///
/// When a section on the path is missing.
fn set(doc: &mut Value, path: &[&str], value: Value) -> color_eyre::Result<()> {
    // walk to the field's section
    let (field, sections) = path.split_last().ok_or_else(|| eyre!("an empty path"))?;
    let mut at = doc;
    for section in sections {
        at = at
            .get_mut(*section)
            .ok_or_else(|| eyre!("the configuration has no {section} section"))?;
    }
    let map = at
        .as_mapping_mut()
        .ok_or_else(|| eyre!("{} is not a section", sections.join(".")))?;
    map.insert(Value::String((*field).to_string()), value);
    Ok(())
}

/// Start the local nodes an inventory describes and form them into one cluster
///
/// # Arguments
///
/// * `inventory_path` - The inventory, which names europa as every node's host
/// * `program` - The node program to run, built for europa
///
/// # Errors
///
/// When a node cannot be staged, claimed or started, or the cluster does not form.
pub async fn up(inventory_path: &Path, program: &Path) -> color_eyre::Result<()> {
    // the inventory and the state its record is saved in
    let deployment = Deployment::attach(inventory_path)?;
    let inventory = &deployment.inventory;
    if deployment.state.record()?.initialized {
        bail!("{} is already up; `x8 local down` it first", inventory.name);
    }
    let nodes = inventory.bootstrap_nodes()?;
    if nodes.len() > CONTROL_CORES.len() {
        bail!("{} names {} nodes, and local.rs places {}", inventory.name, nodes.len(), CONTROL_CORES.len());
    }
    // every node on one host, each on an address of its own
    let host = Host {
        target: nodes[0].target.clone(),
    };
    let mut addresses: Vec<_> = nodes.iter().map(|node| node.address).collect();
    addresses.sort();
    addresses.dedup();
    if addresses.len() != nodes.len() || nodes.iter().any(|node| node.target != host.target) {
        bail!("the local nodes have to share one host and have an address each");
    }
    deployment.state.create()?;
    let password = deployment.state.password()?;
    let authority = Authority::mint(&inventory.name)?;
    // the program, where the user it runs as can read it, and the tls module its peer lanes need
    let binary = format!("{BASE}/bin/x8-node");
    host.run(&format!("set -e; sudo -n mkdir -p {BASE}/bin; sudo -n modprobe tls"))?;
    host.copy(program, &format!("/tmp/{}-x8-node", inventory.name))?;
    host.run(&format!(
        "sudo -n install -o {USER} -m 0755 /tmp/{name}-x8-node {binary} && rm -f /tmp/{name}-x8-node",
        name = inventory.name,
    ))?;
    // each node staged, claimed and given its leaf, the first minting the cluster
    let seeds = vec![nodes[0].control_addr(&inventory.ports)];
    let mut record = ClusterRecord {
        program: Some("x8-node".to_string()),
        ..ClusterRecord::default()
    };
    let mut ids = Vec::with_capacity(nodes.len());
    for (place, node) in nodes.iter().enumerate() {
        let entry = if place == 0 {
            Entry::Bootstrap
        } else {
            Entry::Join(seeds.clone())
        };
        let id = stage(&host, inventory, node, &entry, &password, CONTROL_CORES[place], &binary)?;
        // its leaf names the id its claim printed, as provision_tls does
        let dir = node_dir(&node.name);
        let leaf = authority.issue(&id.to_string(), &node.name, node.address)?;
        host.write(&format!("{dir}/tls/cert.pem"), leaf.cert.as_bytes(), 0o644, Some(USER))?;
        host.write(&format!("{dir}/tls/key.pem"), leaf.key.as_bytes(), 0o600, Some(USER))?;
        host.write(&format!("{dir}/tls/ca.pem"), authority.cert_pem().as_bytes(), 0o644, Some(USER))?;
        record.nodes.insert(
            node.name.clone(),
            NodeRecord {
                node: id.to_string(),
                address: node.address.to_string(),
                target: node.target.clone(),
            },
        );
        ids.push(id);
    }
    deployment.state.save(&record)?;
    // the first node alone, until it leads a control group of one
    start(&host, &nodes[0], &binary)?;
    let first = deployment
        .connect::<SmallClient>(
            &nodes[0].client_addr(&inventory.ports),
            Instant::now() + std::time::Duration::from_secs(120),
        )
        .await?;
    shoaladm::deploy::ops::wait_for_members(&first, &ids[..1], 1).await?;
    // then the others, which join through it and are promoted to voters
    for node in &nodes[1..] {
        start(&host, node, &binary)?;
    }
    let voters = (inventory.control_voters as usize).min(nodes.len());
    shoaladm::deploy::ops::wait_for_members(&first, &ids, voters).await?;
    // every tablet placed over them, once, as `initialize` does
    let op = Uuid::new_v4();
    record.initialize_op = Some(op);
    let model = shoaladm::cluster::poll(&first).await.map_err(|error| eyre!(error))?;
    let response = first
        .admin(&AdminRequest {
            op,
            expected_version: model.version,
            kind: AdminKind::Initialize { nodes: ids.clone() },
        })
        .await
        .map_err(|error| eyre!("initialize: {error:?}"))?;
    match response.outcome {
        Ok(AdminOutcome::Applied { .. } | AdminOutcome::Repeated { .. }) => {}
        Ok(other) => return Err(eyre!("initialize answered {other:?}")),
        Err(error) => return Err(eyre!("initialize was refused: {} ({:?})", error.msg, error.code())),
    }
    record.initialized = true;
    deployment.state.save(&record)?;
    // and what a deploy waits for before it opens the cluster to clients
    shoaladm::deploy::ops::wait_for_writes(&first).await?;
    println!("{} is up: {} local nodes at factor {}", inventory.name, nodes.len(), inventory.replication_factor);
    Ok(())
}

/// Put a node's configuration on the host and claim its directory, returning the id it printed
///
/// # Arguments
///
/// * `host` - The host every local node runs on
/// * `inventory` - The inventory
/// * `node` - The node
/// * `entry` - Whether it mints the cluster or joins it
/// * `password` - The admin password
/// * `control_core` - Its control core
/// * `binary` - The node program on the host
///
/// # Errors
///
/// When a directory, the configuration or the claim fails.
fn stage(
    host: &Host,
    inventory: &Inventory,
    node: &Node,
    entry: &Entry,
    password: &str,
    control_core: usize,
    binary: &str,
) -> color_eyre::Result<NodeId> {
    let dir = node_dir(&node.name);
    // its directories, its roots among them, owned by the user it runs as
    let roots: Vec<String> = node.storage.roots().iter().map(|root| quote(root)).collect();
    host.run(&format!(
        "set -e; sudo -n mkdir -p {dir}/tls {roots}; sudo -n chown -R {USER}: {dir} {roots}; sudo -n chmod 700 {dir}/tls",
        roots = roots.join(" "),
    ))?;
    // the configuration shoaladm renders, given what a shared host needs
    let rendered = render::render(inventory, node, entry, password)?;
    let conf = localize(&rendered, node, control_core)?;
    host.write(&format!("{dir}/shoal.yml"), conf.as_bytes(), 0o600, Some(USER))?;
    // the program's own claim, as the user it serves as, whose last line names the node
    let output = host.output(
        &format!("cd {dir} && sudo -n -u {USER} {binary} claim --conf {dir}/shoal.yml"),
        None,
    )?;
    if output.status != Some(0) {
        bail!("claim failed on {} with {:?}: {}", node.name, output.status, output.stderr.trim());
    }
    let line = output
        .stdout
        .lines()
        .last()
        .ok_or_else(|| eyre!("claim on {} printed nothing", node.name))?;
    let report: shoal::serde_json::Value =
        shoal::serde_json::from_str(line).wrap_err_with(|| format!("claim on {} printed {line:?}", node.name))?;
    let id = report["node"]
        .as_str()
        .and_then(|id| id.parse::<Uuid>().ok())
        .ok_or_else(|| eyre!("claim on {} named no node: {line}", node.name))?;
    Ok(NodeId(id))
}

/// Start a local node in a transient unit with the deployed unit's limits
///
/// # Arguments
///
/// * `host` - The host every local node runs on
/// * `node` - The node
/// * `binary` - The node program on the host
///
/// # Errors
///
/// When the unit does not start.
fn start(host: &Host, node: &Node, binary: &str) -> color_eyre::Result<()> {
    let dir = node_dir(&node.name);
    // the same user, limits and restart as `shoaladm/src/deploy/unit.rs` writes for a deployed node
    host.run(&format!(
        "sudo -n systemd-run --unit={unit} --collect -p User={USER} -p WorkingDirectory={dir} \
         -p LimitMEMLOCK=infinity -p LimitNOFILE=1048576 -p KillSignal=SIGTERM -p TimeoutStopSec=60 \
         {binary} serve --conf {dir}/shoal.yml",
        unit = unit_name(&node.name),
    ))?;
    Ok(())
}

/// Stop the local nodes an inventory describes and remove everything they wrote
///
/// # Arguments
///
/// * `inventory_path` - The inventory
///
/// # Errors
///
/// When a node cannot be stopped or its files removed.
pub fn down(inventory_path: &Path) -> color_eyre::Result<()> {
    let deployment = Deployment::attach(inventory_path)?;
    let inventory = &deployment.inventory;
    // every node's unit, and every root and directory, whether or not it got as far as starting
    for spec in &inventory.nodes {
        let node = inventory.node(&spec.name)?;
        let host = Host {
            target: node.target.clone(),
        };
        let roots: Vec<String> = node.storage.roots().iter().map(|root| quote(root)).collect();
        host.run(&format!(
            "sudo -n systemctl stop {unit} 2>/dev/null || true; sudo -n systemctl reset-failed {unit} 2>/dev/null || true; \
             sudo -n rm -rf {roots} {dir}",
            unit = unit_name(&node.name),
            roots = roots.join(" "),
            dir = node_dir(&node.name),
        ))?;
    }
    // the program last, then the record, so `local up` can run again
    if let Some(spec) = inventory.nodes.first() {
        Host {
            target: spec.target().to_string(),
        }
        .run(&format!("sudo -n rm -rf {BASE}"))?;
    }
    deployment.state.delete()?;
    println!("{} is down", inventory.name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The loopback inventory, as the spike commits it
    fn inventory() -> Inventory {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("inventory-loopback.yml");
        Inventory::read(&path).expect("the loopback inventory reads")
    }

    /// Every local node is rendered onto its own address, control core, leaf and roots
    #[test]
    fn local_nodes_never_share_a_listener_core_or_root() {
        let inventory = inventory();
        let nodes = inventory.bootstrap_nodes().expect("nodes");
        assert_eq!(nodes.len(), 3);
        let mut seen_roots = std::collections::BTreeSet::new();
        let mut seen_addresses = std::collections::BTreeSet::new();
        for (place, node) in nodes.iter().enumerate() {
            let entry = if place == 0 {
                Entry::Bootstrap
            } else {
                Entry::Join(vec![nodes[0].control_addr(&inventory.ports)])
            };
            let rendered = render::render(&inventory, node, &entry, "password").expect("renders");
            let conf: Value = serde_yaml::from_str(&localize(&rendered, node, CONTROL_CORES[place]).expect("localizes"))
                .expect("parses");
            // the client and peer listeners bind the interface, the control one the advertised address
            let interface = conf["networking"]["interface"].as_str().expect("interface");
            assert_eq!(interface, node.address.to_string());
            assert_eq!(conf["cluster"]["advertise"].as_str(), Some(interface));
            assert!(seen_addresses.insert(interface.to_string()));
            assert_eq!(conf["cluster"]["control_core"].as_u64(), Some(CONTROL_CORES[place] as u64));
            assert!(conf["cluster"]["tls"]["key"]
                .as_str()
                .expect("key")
                .starts_with(&node_dir(&node.name)));
            // the control core's physical core is one the node's shards are kept off, and no
            // other node's
            let excluded: Vec<u64> = conf["resources"]["exclude_cores"]
                .as_sequence()
                .expect("exclusions")
                .iter()
                .filter_map(Value::as_u64)
                .collect();
            assert!(excluded.contains(&0), "core 0 is the system's");
            assert!(!excluded.contains(&(CONTROL_CORES[place] as u64)));
            for root in node.storage.roots() {
                assert!(root.starts_with(&node_dir(&node.name)), "{root}");
                assert!(seen_roots.insert(root));
            }
        }
    }
}
