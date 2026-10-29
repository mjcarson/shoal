//! The holders: one `x8-holder` beside each node, started, checked and stopped over ssh
//!
//! Each node of an inventory gets a holder on its host, the slice a replicated pool's copy would
//! be on, with:
//!
//! - its journal and chunks in `<the node's latency root>-holder`, beside the node's own files on
//!   the same filesystem, so a host's device counters count the node and its holder together and
//!   the WAL and the pool's device are one disk, as the spike asks;
//! - its executor on the sibling of the node's control cpu, the one cpu of the host no shard runs
//!   on: a cluster node keeps its control thread's whole physical core from its shards;
//! - a leaf from an authority minted for the holders, naming its node's address, as a node's
//!   leaf does; the driver gets a leaf of its own from the same authority, kept in a state
//!   directory beside the deployment's;
//! - a transient unit, `shoal-x8-holder-<node>`, as the user `shoal` with the deployed unit's
//!   locked-memory limit, since its ring registers buffers.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::eyre::{bail, eyre, WrapErr};
use shoal::shared::tls::PeerTlsOptions;
use shoaladm::deploy::inventory::Inventory;
use shoaladm::deploy::pki::Authority;
use shoaladm::deploy::remote::{quote, Host};
use shoaladm::deploy::state::State;

use crate::keys::SLOTS;
use crate::lane::{LaneTls, Pool};
use crate::shape::STRIPE_BYTES;
use crate::wire::{self, Kind, Request, StageHead, WireLabel, PORT};

/// Where the holder program and every holder's leaf live on a host
pub const BASE: &str = "/var/tmp/x8-holder";

/// The user every holder runs as, the lab's
pub const USER: &str = "shoal";

/// The length of every holder's journal ring
pub const RING_BYTES: u64 = 1 << 30;

/// The memory every holder's executor registers for I/O buffers
pub const IO_MEMORY: usize = 128 << 20;

/// The unit a holder runs in
///
/// # Arguments
///
/// * `node` - Its node's name in the inventory
#[must_use]
pub fn unit_name(node: &str) -> String {
    format!("shoal-x8-holder-{node}")
}

/// One holder: where it runs and where it keeps its files
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    /// Its node's name in the inventory
    pub name: String,
    /// What ssh reaches its host through
    pub target: String,
    /// The address it listens on, its node's
    pub address: IpAddr,
    /// Its directory, beside its node's latency root
    pub dir: String,
    /// The cpu of its node's control thread, whose sibling it runs on
    pub control_cpu: usize,
}

impl Spec {
    /// Where the driver dials it
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        SocketAddr::new(self.address, PORT)
    }
}

/// Whether an inventory's nodes all share one host, as europa's local nodes do
///
/// # Arguments
///
/// * `inventory` - The inventory
#[must_use]
pub fn is_local(inventory: &Inventory) -> bool {
    let targets: std::collections::BTreeSet<&str> = inventory.nodes.iter().map(|node| node.target()).collect();
    inventory.nodes.len() > 1 && targets.len() == 1
}

/// The holders an inventory's nodes get, in its bootstrap order
///
/// # Arguments
///
/// * `inventory` - The inventory
///
/// # Errors
///
/// When a node cannot be resolved.
pub fn specs(inventory: &Inventory) -> color_eyre::Result<Vec<Spec>> {
    let local = is_local(inventory);
    let nodes = inventory.bootstrap_nodes()?;
    Ok(nodes
        .iter()
        .enumerate()
        .map(|(place, node)| Spec {
            name: node.name.clone(),
            target: node.target.clone(),
            address: node.address,
            dir: format!("{}-holder", node.storage.latency.trim_end_matches('/')),
            // a deployed node's control thread is on cpu 0; a local node's on the core local.rs gives it
            control_cpu: if local { crate::local::CONTROL_CORES[place] } else { 0 },
        })
        .collect())
}

/// The cpu that shares a physical core with another, from the kernel's sibling list
///
/// # Arguments
///
/// * `list` - `thread_siblings_list`: `0,16` or `0-1`
/// * `cpu` - The cpu whose sibling is wanted
#[must_use]
pub fn sibling_of(list: &str, cpu: usize) -> Option<usize> {
    // every cpu the list names, ranges opened out
    let mut cpus = Vec::new();
    for part in list.trim().split(',') {
        match part.split_once('-') {
            Some((low, high)) => {
                let (Ok(low), Ok(high)) = (low.parse::<usize>(), high.parse::<usize>()) else {
                    continue;
                };
                cpus.extend(low..=high);
            }
            None => cpus.extend(part.parse::<usize>().ok()),
        }
    }
    cpus.into_iter().find(|other| *other != cpu)
}

/// The state directory the driver's leaf and the authority are kept in
///
/// # Arguments
///
/// * `inventory` - The inventory
///
/// # Errors
///
/// When no home is set.
pub fn state_dir(inventory: &Inventory) -> color_eyre::Result<PathBuf> {
    Ok(State::locate(&format!("{}-holders", inventory.name))?.dir().to_path_buf())
}

/// The driver's TLS: its leaf and the holders' authority, as `holders up` left them
///
/// # Arguments
///
/// * `inventory` - The inventory
///
/// # Errors
///
/// When the files are not there or rustls refuses them.
pub fn driver_tls(inventory: &Inventory) -> color_eyre::Result<LaneTls> {
    let dir = state_dir(inventory)?;
    LaneTls::load(&PeerTlsOptions {
        cert: dir.join("driver.pem"),
        key: dir.join("driver-key.pem"),
        ca: dir.join("ca.pem"),
        bind_identity: false,
    })
}

/// Start a holder beside every node of an inventory and wait until each answers
///
/// # Arguments
///
/// * `inventory_path` - The inventory
/// * `program` - The holder program, built for the oldest cpu among the hosts
/// * `in_place_sync` - How an apply or a fold is made durable: `each` or `batch`
///
/// # Errors
///
/// When a holder cannot be installed or started, or does not answer.
pub async fn up(inventory_path: &Path, program: &Path, in_place_sync: &str) -> color_eyre::Result<()> {
    if crate::holder::InPlaceSync::from_name(in_place_sync).is_none() {
        bail!("an in-place sync is each or batch, not {in_place_sync}");
    }
    let inventory = Inventory::read(inventory_path)?;
    let specs = specs(&inventory)?;
    // an authority for this set of holders, and the driver's leaf from it, kept locally
    let authority = Authority::mint(&format!("{} holders", inventory.name))?;
    let dir = state_dir(&inventory)?;
    std::fs::create_dir_all(&dir)?;
    let driver_address = specs.first().map_or(IpAddr::from([127, 0, 0, 1]), |spec| spec.address);
    let driver = authority.issue("x8-driver", "x8-driver", driver_address)?;
    std::fs::write(dir.join("driver.pem"), driver.cert.as_bytes())?;
    std::fs::write(dir.join("driver-key.pem"), driver.key.as_bytes())?;
    std::fs::write(dir.join("ca.pem"), authority.cert_pem().as_bytes())?;
    // the program once a host, where the user it runs as can read it, and the tls module
    let mut installed = Vec::new();
    for spec in &specs {
        if installed.contains(&spec.target) {
            continue;
        }
        let host = Host {
            target: spec.target.clone(),
        };
        host.run(&format!("set -e; sudo -n mkdir -p {BASE}/bin; sudo -n modprobe tls"))?;
        let staged = format!("/tmp/{}-x8-holder", inventory.name);
        host.copy(program, &staged)?;
        host.run(&format!("sudo -n install -o {USER} -m 0755 {staged} {BASE}/bin/x8-holder && rm -f {staged}"))?;
        installed.push(spec.target.clone());
    }
    // each holder's directory, leaf and unit
    for spec in &specs {
        let host = Host {
            target: spec.target.clone(),
        };
        // the sibling of the node's control cpu, which no shard runs on
        let list = host.run(&format!(
            "cat /sys/devices/system/cpu/cpu{}/topology/thread_siblings_list",
            spec.control_cpu
        ))?;
        let cpu = sibling_of(&list, spec.control_cpu)
            .ok_or_else(|| eyre!("cpu {} on {} has no sibling: {list:?}", spec.control_cpu, spec.target))?;
        let tls = format!("{BASE}/{}/tls", spec.name);
        host.run(&format!(
            "set -e; sudo -n rm -rf {dir}; sudo -n mkdir -p {dir} {tls}; sudo -n chown -R {USER}: {dir} {base}/{name}; sudo -n chmod 700 {tls}",
            dir = quote(&spec.dir),
            base = BASE,
            name = spec.name,
        ))?;
        let leaf = authority.issue(&format!("x8-holder-{}", spec.name), &spec.name, spec.address)?;
        host.write(&format!("{tls}/cert.pem"), leaf.cert.as_bytes(), 0o644, Some(USER))?;
        host.write(&format!("{tls}/key.pem"), leaf.key.as_bytes(), 0o600, Some(USER))?;
        host.write(&format!("{tls}/ca.pem"), authority.cert_pem().as_bytes(), 0o644, Some(USER))?;
        host.run(&format!(
            "sudo -n systemctl stop {unit} 2>/dev/null; sudo -n systemctl reset-failed {unit} 2>/dev/null; \
             sudo -n systemd-run --unit={unit} --collect -p User={USER} -p LimitMEMLOCK=infinity -p LimitNOFILE=1048576 \
             {BASE}/bin/x8-holder --listen {addr} --dir {dir} --cpu {cpu} --slots {SLOTS} --chunk-bytes {STRIPE_BYTES} \
             --ring-bytes {RING_BYTES} --io-memory {IO_MEMORY} --in-place-sync {in_place_sync} \
             --cert {tls}/cert.pem --key {tls}/key.pem --ca {tls}/ca.pem",
            unit = unit_name(&spec.name),
            addr = spec.addr(),
            dir = quote(&spec.dir),
        ))?;
        println!(
            "{}: holder on cpu {cpu} at {} in {}, in-place syncs {in_place_sync}",
            spec.name,
            spec.addr(),
            spec.dir
        );
    }
    // every holder answers once its files are written ahead
    let tls = driver_tls(&inventory)?;
    for spec in &specs {
        wait_for(spec, &tls).await?;
    }
    println!("{} holders are up", specs.len());
    Ok(())
}

/// Wait until a holder answers its counters over a lane
///
/// # Arguments
///
/// * `spec` - The holder
/// * `tls` - What the driver presents and trusts
///
/// # Errors
///
/// When it does not answer in two minutes.
async fn wait_for(spec: &Spec, tls: &LaneTls) -> color_eyre::Result<()> {
    let started = Instant::now();
    loop {
        // a lane, and its counters, which a holder answers only once it listens
        let answered = match Pool::open(&spec.name, spec.addr(), 1, tls).await {
            Ok(pool) => Arc::new(pool).stats().await.is_ok(),
            Err(_) => false,
        };
        if answered {
            return Ok(());
        }
        if started.elapsed() > Duration::from_secs(120) {
            bail!("the holder beside {} did not answer at {} in two minutes", spec.name, spec.addr());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Open a pool of lanes to every holder of an inventory, in its bootstrap order
///
/// # Arguments
///
/// * `inventory` - The inventory
/// * `lanes` - How many lanes a holder
///
/// # Errors
///
/// When a holder does not answer.
pub async fn connect(inventory: &Inventory, lanes: usize) -> color_eyre::Result<Vec<Arc<Pool>>> {
    let tls = driver_tls(inventory)?;
    let mut pools = Vec::new();
    for spec in specs(inventory)? {
        let pool = Pool::open(&spec.name, spec.addr(), lanes, &tls)
            .await
            .wrap_err_with(|| format!("the holder beside {}", spec.name))?;
        pools.push(Arc::new(pool));
    }
    Ok(pools)
}

/// Prove every holder stages, applies, folds, probes and counts, over kTLS
///
/// # Arguments
///
/// * `inventory_path` - The inventory
///
/// # Errors
///
/// When a holder refuses or answers wrongly.
pub async fn check(inventory_path: &Path) -> color_eyre::Result<()> {
    let inventory = Inventory::read(inventory_path)?;
    for pool in connect(&inventory, 1).await? {
        let before = pool.stats().await?;
        // a stage of 16 KiB into the last slot, then its apply
        let label = WireLabel { sequence: 1, tag: 0xC4EC };
        let head = StageHead {
            slot: SLOTS - 1,
            offset: 4096,
            len: 16 << 10,
            label,
            expected: WireLabel::default(),
        };
        let started = Instant::now();
        pool.call(&wire::stage_frame(&head, 1, 2), Kind::Staged).await?;
        let staged = started.elapsed();
        pool.call(&Request::Apply { slot: SLOTS - 1, label }.frame(), Kind::Applied).await?;
        // a fold of 4 KiB into the same slot
        pool.call(
            &Request::Fold {
                slot: SLOTS - 1,
                offset: 0,
                len: 4096,
                seed: 3,
                object: 4,
            }
            .frame(),
            Kind::Folded,
        )
        .await?;
        let probe = pool.probe(5).await?;
        let after = pool.stats().await?;
        let did = after.since(&before);
        if did.stages != 1 || did.applies != 1 || did.folds != 1 || after.ktls != 1 {
            bail!("{} counted {did:?}", pool.name);
        }
        println!(
            "{}: staged in {staged:?}, applied, folded; sync floor {} µs (max {}); {} journal syncs; kTLS on",
            pool.name,
            probe.p50_ns / 1000,
            probe.max_ns / 1000,
            did.stage_syncs,
        );
    }
    Ok(())
}

/// Stop every holder of an inventory and remove everything it wrote
///
/// # Arguments
///
/// * `inventory_path` - The inventory
///
/// # Errors
///
/// When a host cannot be reached.
pub fn down(inventory_path: &Path) -> color_eyre::Result<()> {
    let inventory = Inventory::read(inventory_path)?;
    let specs = specs(&inventory)?;
    for spec in &specs {
        Host {
            target: spec.target.clone(),
        }
        .run(&format!(
            "sudo -n systemctl stop {unit} 2>/dev/null || true; sudo -n systemctl reset-failed {unit} 2>/dev/null || true; \
             sudo -n rm -rf {dir} {BASE}/{name}",
            unit = unit_name(&spec.name),
            dir = quote(&spec.dir),
            name = spec.name,
        ))?;
    }
    // the program last, once a host
    let targets: std::collections::BTreeSet<&str> = specs.iter().map(|spec| spec.target.as_str()).collect();
    for target in targets {
        Host {
            target: target.to_string(),
        }
        .run(&format!("sudo -n rm -rf {BASE}"))?;
    }
    println!("{} holders are down", specs.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The inventory the spike commits, by its file name
    fn inventory(name: &str) -> Inventory {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(name);
        Inventory::read(&path).expect("the inventory reads")
    }

    /// A sibling is read from either form the kernel writes
    #[test]
    fn a_sibling_is_read_from_the_kernels_list() {
        assert_eq!(sibling_of("0,16\n", 0), Some(16));
        assert_eq!(sibling_of("1,17", 1), Some(17));
        assert_eq!(sibling_of("0-1", 0), Some(1));
        assert_eq!(sibling_of("5", 5), None);
    }

    /// A loopback holder sits beside its node's control core and in its node's directory; a lab
    /// holder beside a deployed node's control cpu, on the device its node's root is on
    #[test]
    fn holders_sit_beside_their_nodes() {
        let local = inventory("inventory-loopback.yml");
        assert!(is_local(&local));
        let local_specs = specs(&local).expect("specs");
        assert_eq!(local_specs.iter().map(|spec| spec.control_cpu).collect::<Vec<_>>(), vec![1, 5, 9]);
        for spec in &local_specs {
            assert!(spec.dir.starts_with(&crate::local::node_dir(&spec.name)), "{}", spec.dir);
        }
        let lab = inventory("inventory.yml");
        assert!(!is_local(&lab));
        let lab_specs = specs(&lab).expect("specs");
        assert!(lab_specs.iter().all(|spec| spec.control_cpu == 0));
        let dirs: Vec<&str> = lab_specs.iter().map(|spec| spec.dir.as_str()).collect();
        assert!(dirs.contains(&"/optane/shoal-x8-holder"), "{dirs:?}");
        assert!(dirs.contains(&"/xfs/shoal-x8-holder"), "{dirs:?}");
    }

    /// A holder's leaf and the driver's, issued by one authority, build the mutual configs both
    /// ends of a lane run
    #[test]
    fn a_holder_and_the_driver_trust_each_other() {
        let authority = Authority::mint("x8 test holders").expect("an authority");
        let dir = std::env::temp_dir().join(format!("x8-holder-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a directory");
        let mut options = Vec::new();
        for name in ["holder", "driver"] {
            let leaf = authority
                .issue(&format!("x8-{name}"), name, "127.0.0.11".parse().unwrap())
                .expect("a leaf");
            std::fs::write(dir.join(format!("{name}.pem")), leaf.cert).expect("written");
            std::fs::write(dir.join(format!("{name}-key.pem")), leaf.key).expect("written");
            options.push(PeerTlsOptions {
                cert: dir.join(format!("{name}.pem")),
                key: dir.join(format!("{name}-key.pem")),
                ca: dir.join("ca.pem"),
                bind_identity: false,
            });
        }
        std::fs::write(dir.join("ca.pem"), authority.cert_pem()).expect("written");
        assert!(shoal::shared::tls::peer_server_config(&options[0]).is_ok());
        assert!(LaneTls::load(&options[1]).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
