//! The bench's own cluster, and what keeps it from ever touching the inventory's
//!
//! A benchmark wipes its cluster between arms, so the cluster it wipes must never be one an
//! operator keeps data in. The bench copies the inventory into a [`BenchInventory`] and that
//! copy is the only thing bench code ever hands to a bootstrap or a destroy. The copy is a
//! cluster of its own on the same hosts:
//!
//! - named `<name>-bench`, so its unit, its state directory and its authority are its own;
//! - under `<remote_dir>-bench`, with every port moved by `--port-offset`;
//! - with **every storage root the inventory names, at any level, moved to `<root>-bench`**. An
//!   inventory that only renamed the cluster would keep a group level root such as
//!   `/optane/shoal-tmdb`, and the bench's first `--wipe` would delete the inventory's data.
//!
//! [`check_disjoint`] refuses a copy whose roots, ports or directory overlap the inventory's in
//! either direction, and a root is wiped only if its marker names the bench's own cluster
//! ([`super::hosts::wipe_guard`]).

use color_eyre::eyre::{bail, eyre, WrapErr};
use serde_yaml::{Mapping, Value};
use sha2::{Digest, Sha256};
use shoal_loadgen::spec::Override;
use std::path::{Path, PathBuf};

use crate::deploy::inventory::{is_within, Source};
use crate::deploy::Inventory;

/// What a bench inventory is derived with
#[derive(Debug, Clone, Default)]
pub struct DeriveOptions {
    /// How far every port is moved
    pub port_offset: u16,
    /// One directory on every host for the bench's storage, rather than each root with `-bench`
    pub bench_storage: Option<String>,
    /// The project to build the node from, rather than the inventory's `server:`
    pub from_project: Option<PathBuf>,
    /// The inventory settings to lay over the copy
    pub overrides: Option<Override>,
    /// A node to leave out of the bootstrap set, for an event to move onto
    pub spare: Option<String>,
}

/// The inventory of the bench's own cluster, and the one it was copied from
#[derive(Debug)]
pub struct BenchInventory {
    /// Where the copy was written
    pub path: PathBuf,
    /// The copy
    pub inventory: Inventory,
    /// The inventory it was copied from
    pub base: Inventory,
}

/// The name a bench cluster derived from an inventory is given
///
/// # Arguments
///
/// * `base` - The inventory's name
#[must_use]
pub fn bench_name(base: &str) -> String {
    format!("{base}-bench")
}

/// Set a value at a dotted path in a YAML mapping, making the mappings on the way
///
/// # Arguments
///
/// * `root` - The document
/// * `path` - The dotted path, such as `resources.memory`
/// * `value` - What to set
///
/// # Errors
///
/// When something on the way is not a mapping.
pub fn set_path(root: &mut Value, path: &str, value: Value) -> color_eyre::Result<()> {
    // walk every key but the last, making mappings that are not there
    let keys: Vec<&str> = path.split('.').collect();
    let mut at = root;
    for key in &keys[..keys.len() - 1] {
        let mapping = at
            .as_mapping_mut()
            .ok_or_else(|| eyre!("{path}: {key} is under something that is not a mapping"))?;
        at = mapping
            .entry(Value::from(*key))
            .or_insert_with(|| Value::Mapping(Mapping::new()));
    }
    let mapping = at
        .as_mapping_mut()
        .ok_or_else(|| eyre!("{path}: the last key is under something that is not a mapping"))?;
    mapping.insert(Value::from(keys[keys.len() - 1]), value);
    Ok(())
}

/// Read an inventory file as a YAML document
///
/// # Arguments
///
/// * `path` - The file
fn document(path: &Path) -> color_eyre::Result<Value> {
    let raw = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read the inventory {}", path.display()))?;
    serde_yaml::from_str(&raw).wrap_err_with(|| format!("{} is not YAML", path.display()))
}

/// Copy an inventory into the inventory of the bench's own cluster, and write it
///
/// # Arguments
///
/// * `base_path` - The inventory
/// * `options` - What the copy is derived with
/// * `out` - Where the copy is written
///
/// # Errors
///
/// When the inventory cannot be read, a port would overflow, a spare is not one of its nodes,
/// an override cannot be laid, or the copy overlaps the inventory anywhere.
pub fn derive(base_path: &Path, options: &DeriveOptions, out: &Path) -> color_eyre::Result<BenchInventory> {
    // the inventory as a cluster, and as a document to copy
    let base = Inventory::read(base_path)?;
    let mut doc = document(base_path)?;
    // its own name, directory and ports
    set_path(&mut doc, "name", Value::from(bench_name(&base.name)))?;
    set_path(&mut doc, "remote_dir", Value::from(format!("{}-bench", base.remote_dir())))?;
    for (key, port) in [
        ("client", base.ports.client),
        ("peer", base.ports.peer),
        ("control", base.ports.control),
    ] {
        let moved = port
            .checked_add(options.port_offset)
            .ok_or_else(|| eyre!("the {key} port {port} cannot be moved by {}", options.port_offset))?;
        set_path(&mut doc, &format!("ports.{key}"), Value::from(moved))?;
    }
    // the program: the project's, or the inventory's own by an absolute path, since the copy is
    // written somewhere else and a relative path would be relative to the wrong file
    let root = doc.as_mapping_mut().ok_or_else(|| eyre!("the inventory is not a mapping"))?;
    match &options.from_project {
        Some(project) => {
            root.remove("server");
            root.insert(Value::from("project"), Value::from(project.display().to_string()));
        }
        None => {
            for (key, path) in [("server", &base.server), ("project", &base.project)] {
                if let Some(path) = path {
                    let absolute = path.canonicalize().unwrap_or_else(|_| path.clone());
                    root.insert(Value::from(key), Value::from(absolute.display().to_string()));
                }
            }
        }
    }
    // storage is set on each node alone, so nothing named at a group or the deployment survives
    root.remove("storage");
    if let Some(groups) = root.get_mut("groups").and_then(Value::as_mapping_mut) {
        for (_, group) in groups.iter_mut() {
            if let Some(group) = group.as_mapping_mut() {
                group.remove("storage");
            }
        }
    }
    let nodes = root
        .get_mut("nodes")
        .and_then(Value::as_sequence_mut)
        .ok_or_else(|| eyre!("the inventory has no nodes"))?;
    for (spec, node) in base.nodes.iter().zip(nodes.iter_mut()) {
        let node = node
            .as_mapping_mut()
            .ok_or_else(|| eyre!("a node of the inventory is not a mapping"))?;
        node.remove("storage");
        let (storage, sources) = base.resolve_storage(spec);
        let mut moved = Mapping::new();
        match &options.bench_storage {
            // one directory named for the bench, on every host
            Some(dir) => {
                moved.insert(Value::from("latency"), Value::from(dir.clone()));
            }
            // every root the inventory named moved beside itself; a default one follows the
            // copy's own directory, which is already its own
            None => {
                if sources[0] != Source::Default {
                    moved.insert(Value::from("latency"), Value::from(format!("{}-bench", storage.latency)));
                }
                if sources[1] != Source::Default && storage.throughput != storage.latency {
                    moved.insert(
                        Value::from("throughput"),
                        Value::from(format!("{}-bench", storage.throughput)),
                    );
                }
            }
        }
        if !moved.is_empty() {
            node.insert(Value::from("storage"), Value::Mapping(moved));
        }
    }
    // a spare is left out of the bootstrap set, so an event has somewhere to move to
    if let Some(spare) = &options.spare {
        let names = base.bootstrap_names();
        if !base.nodes.iter().any(|node| &node.name == spare) {
            bail!("--spare {spare} is not a node of the inventory");
        }
        let bootstrap: Vec<Value> = names
            .iter()
            .filter(|name| *name != spare)
            .map(|name| Value::from(name.clone()))
            .collect();
        if bootstrap.is_empty() {
            bail!("--spare {spare} is the inventory's only bootstrap node");
        }
        set_path(&mut doc, "bootstrap", Value::Sequence(bootstrap))?;
    }
    // the arm's own settings, over everything else
    if let Some(overrides) = &options.overrides {
        for (path, value) in &overrides.set {
            set_path(&mut doc, path, value.clone())
                .wrap_err_with(|| format!("the override {}", overrides.name))?;
        }
    }
    // written, read back as a cluster, and checked against the inventory
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let text = format!(
        "# The bench's own cluster, copied from {} by shoaladm bench. Never edit it: it is\n\
         # written again for every run.\n{}",
        base_path.display(),
        serde_yaml::to_string(&doc)?
    );
    std::fs::write(out, text)?;
    let inventory = Inventory::read(out)?;
    check_disjoint(&base, &inventory)?;
    Ok(BenchInventory {
        path: out.to_path_buf(),
        inventory,
        base,
    })
}

/// Refuse a bench inventory that shares anything a wipe or a port could reach with its base
///
/// # Arguments
///
/// * `base` - The inventory copied
/// * `bench` - The copy
///
/// # Errors
///
/// Every overlap, named.
pub fn check_disjoint(base: &Inventory, bench: &Inventory) -> color_eyre::Result<()> {
    // every root either deploys to, on every node
    let roots = |inventory: &Inventory| -> Vec<String> {
        inventory
            .nodes
            .iter()
            .flat_map(|spec| inventory.resolve_storage(spec).0.roots())
            .collect()
    };
    let base_roots = roots(base);
    let bench_roots = roots(bench);
    let mut overlaps = Vec::new();
    // a bench root inside or around a base root, or inside its directory
    for bench_root in &bench_roots {
        for base_root in &base_roots {
            if is_within(bench_root, base_root) || is_within(base_root, bench_root) {
                overlaps.push(format!("the bench's root {bench_root} overlaps the inventory's {base_root}"));
            }
        }
        if is_within(bench_root, &base.remote_dir()) {
            overlaps.push(format!(
                "the bench's root {bench_root} is inside the inventory's directory {}",
                base.remote_dir()
            ));
        }
    }
    // the two directories
    if is_within(&bench.remote_dir(), &base.remote_dir()) || is_within(&base.remote_dir(), &bench.remote_dir()) {
        overlaps.push(format!(
            "the bench's directory {} overlaps the inventory's {}",
            bench.remote_dir(),
            base.remote_dir()
        ));
    }
    // a base root inside the bench's directory would be deleted by the bench's destroy
    for base_root in &base_roots {
        if is_within(base_root, &bench.remote_dir()) {
            overlaps.push(format!(
                "the inventory's root {base_root} is inside the bench's directory {}",
                bench.remote_dir()
            ));
        }
    }
    // the ports
    let ports = |inventory: &Inventory| [inventory.ports.client, inventory.ports.peer, inventory.ports.control];
    for port in ports(bench) {
        if ports(base).contains(&port) {
            overlaps.push(format!("the bench's port {port} is one of the inventory's"));
        }
    }
    // and the names
    if bench.name == base.name || bench.unit_name() == base.unit_name() {
        overlaps.push(format!("the bench's cluster is named {}, like the inventory's", bench.name));
    }
    if overlaps.is_empty() {
        return Ok(());
    }
    Err(eyre!(
        "the bench's cluster would overlap the inventory's, so its wipe could delete the inventory's data:\n  - {}",
        overlaps.join("\n  - ")
    ))
}

/// A digest of an inventory's shape: everything that changes what a cluster measures, without
/// its name, paths, addresses or ports
///
/// # Arguments
///
/// * `path` - The inventory
///
/// # Errors
///
/// When it cannot be read.
pub fn shape_digest(path: &Path) -> color_eyre::Result<String> {
    // the document without what only says where
    let mut doc = document(path)?;
    if let Some(root) = doc.as_mapping_mut() {
        for key in ["name", "remote_dir", "ports", "server", "project", "storage"] {
            root.remove(key);
        }
        if let Some(groups) = root.get_mut("groups").and_then(Value::as_mapping_mut) {
            for (_, group) in groups.iter_mut() {
                if let Some(group) = group.as_mapping_mut() {
                    group.remove("storage");
                }
            }
        }
        if let Some(nodes) = root.get_mut("nodes").and_then(Value::as_sequence_mut) {
            for node in nodes.iter_mut() {
                if let Some(node) = node.as_mapping_mut() {
                    for key in ["ssh", "address", "storage"] {
                        node.remove(key);
                    }
                }
            }
        }
    }
    // canonical json, so key order in the file does not move it
    let json = serde_json::to_value(&doc)?;
    Ok(hex(&Sha256::digest(canonical(&json).as_bytes())))
}

/// A sha256 of a file, as hex
///
/// # Arguments
///
/// * `path` - The file
///
/// # Errors
///
/// When it cannot be read.
pub fn file_digest(path: &Path) -> color_eyre::Result<String> {
    let bytes = std::fs::read(path).wrap_err_with(|| format!("reading {}", path.display()))?;
    Ok(hex(&Sha256::digest(&bytes)))
}

/// Bytes as lowercase hex
///
/// # Arguments
///
/// * `bytes` - The bytes
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A json value written with every object's keys sorted
///
/// # Arguments
///
/// * `value` - The value
fn canonical(value: &serde_json::Value) -> String {
    // objects sorted by key, everything else as serde writes it
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys
                .into_iter()
                .map(|key| format!("{}:{}", serde_json::Value::from(key.clone()), canonical(&map[key])))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        serde_json::Value::Array(items) => {
            format!("[{}]", items.iter().map(canonical).collect::<Vec<_>>().join(","))
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{check_disjoint, derive, shape_digest, DeriveOptions};
    use shoal_loadgen::spec::Override;
    use std::collections::BTreeMap;

    /// The tmdb inventory's shape: roots at the group level, one group per kind of host
    const TMDB: &str = "\
name: tmdb
ports:
  client: 12000
  peer: 12001
  control: 12002
replication_factor: 3
control_voters: 3
resources:
  cores: 6
  memory: 8Gi
groups:
  bd795i:
    storage:
      latency: /optane/shoal-tmdb
    wal_commit_delay: 3ms
  v1000:
    storage:
      latency: /optane/shoal
tracing: Info
user: shoal
admin: admin
nodes:
- name: europa
  address: 172.16.2.10
  group: bd795i
- name: titan
  address: 172.16.2.11
  group: v1000
- name: hyperion
  address: 172.16.2.12
  group: v1000
";

    /// Write the tmdb inventory and derive the bench's from it
    ///
    /// # Arguments
    ///
    /// * `options` - What to derive it with
    fn tmdb(options: &DeriveOptions) -> (tempfile::TempDir, color_eyre::Result<super::BenchInventory>) {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("tmdb.yml");
        std::fs::write(&base, TMDB).unwrap();
        let out = dir.path().join("run").join("inventory.bench.yml");
        let derived = derive(&base, options, &out);
        (dir, derived)
    }

    /// Every root, port, directory and name of the tmdb shape is moved off the inventory's
    #[test]
    fn the_tmdb_shape_is_moved_off_every_root_and_port() {
        let options = DeriveOptions {
            port_offset: 100,
            ..DeriveOptions::default()
        };
        let (_dir, derived) = tmdb(&options);
        let bench = derived.unwrap();
        let inventory = &bench.inventory;
        assert_eq!(inventory.name, "tmdb-bench");
        assert_eq!(inventory.unit_name(), "shoal-tmdb-bench.service");
        assert_eq!(inventory.remote_dir(), "/opt/shoal-deploy/tmdb-bench");
        assert_eq!((inventory.ports.client, inventory.ports.peer, inventory.ports.control), (12100, 12101, 12102));
        // the group level roots moved, per node, and nothing else of the groups did
        let roots: Vec<String> = inventory
            .nodes
            .iter()
            .map(|spec| inventory.resolve_storage(spec).0.latency)
            .collect();
        assert_eq!(roots, vec!["/optane/shoal-tmdb-bench", "/optane/shoal-bench", "/optane/shoal-bench"]);
        assert_eq!(
            inventory.resolve_wal_commit_delay(&inventory.nodes[0]).as_deref(),
            Some("3ms")
        );
        // and it is disjoint from the inventory it came from
        check_disjoint(&bench.base, inventory).unwrap();
    }

    /// A copy that kept the inventory's roots is refused, naming the overlap
    #[test]
    fn a_copy_sharing_a_root_is_refused() {
        // one directory for the bench's storage that is the inventory's own root
        let options = DeriveOptions {
            port_offset: 100,
            bench_storage: Some("/optane/shoal".to_string()),
            ..DeriveOptions::default()
        };
        let (_dir, derived) = tmdb(&options);
        let refused = derived.unwrap_err().to_string();
        assert!(refused.contains("overlaps the inventory's /optane/shoal"), "{refused}");
        // and a copy with its ports where they were
        let options = DeriveOptions::default();
        let (_dir, derived) = tmdb(&options);
        assert!(derived.unwrap_err().to_string().contains("port 12000"));
    }

    /// A spare leaves the bootstrap set, and an override is laid over the copy
    #[test]
    fn a_spare_and_an_override_are_laid_over_the_copy() {
        let options = DeriveOptions {
            port_offset: 100,
            spare: Some("hyperion".to_string()),
            overrides: Some(Override {
                name: "small".to_string(),
                // two nodes are left to bootstrap, which a factor of three would refuse
                set: BTreeMap::from([
                    ("resources.memory".to_string(), "2Gi".into()),
                    ("replication_factor".to_string(), 2.into()),
                ]),
            }),
            ..DeriveOptions::default()
        };
        let (_dir, derived) = tmdb(&options);
        let bench = derived.unwrap();
        assert_eq!(bench.inventory.bootstrap_names(), vec!["europa", "titan"]);
        assert_eq!(bench.inventory.resources.memory, "2Gi");
        // a spare that is not a node is refused
        let options = DeriveOptions {
            port_offset: 100,
            spare: Some("mars".to_string()),
            ..DeriveOptions::default()
        };
        assert!(tmdb(&options).1.is_err());
    }

    /// The shape ignores where a cluster is and follows what it is
    #[test]
    fn the_shape_digest_ignores_names_and_paths() {
        let options = DeriveOptions {
            port_offset: 100,
            ..DeriveOptions::default()
        };
        let (dir, derived) = tmdb(&options);
        let bench = derived.unwrap();
        let base = dir.path().join("tmdb.yml");
        assert_eq!(shape_digest(&base).unwrap(), shape_digest(&bench.path).unwrap());
        let bigger = dir.path().join("bigger.yml");
        std::fs::write(&bigger, TMDB.replace("memory: 8Gi", "memory: 16Gi")).unwrap();
        assert_ne!(shape_digest(&base).unwrap(), shape_digest(&bigger).unwrap());
    }
}
