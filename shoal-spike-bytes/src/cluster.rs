//! The cluster as the driver sees it: a client a member, and the figures each member keeps
//!
//! The form of X10's (`shoal-spike-rows/src/cluster.rs`). Everything here is `shoaladm`'s,
//! reached through its library: a connection to each member as the admin, the `Replication` read
//! every member answers with its own groups and WAL counters, the `Stats` read every member
//! answers with its own figures, and the kernel's device counters for every storage root. A
//! cluster is opened from a deployment, as the lab's and the one-node legs' are, or from europa's
//! local nodes, which no deployment can describe ([`crate::local`]). The one thing `shoaladm` does
//! not read, a host's network counters, is read here the way it reads the device counters.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::eyre::{bail, eyre, WrapErr};
use shoal::server::replication::report::NodeReplication;
use shoal::shared::identity::NodeId;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::shared::protocol::stats::NodeStats;
use shoal::uuid::Uuid;
use shoal::Shoal;
use shoal_loadgen::results::HostDevices;
use shoaladm::bench::devices::{self, HostRoots};
use shoaladm::deploy::inventory::Inventory;
use shoaladm::deploy::remote::Host;
use shoaladm::deploy::Deployment;

use crate::StripesAsRowsClient;

/// A client of the spike's schema, connected to one member as the admin
pub type Client = Arc<Shoal<StripesAsRowsClient>>;

/// How long a member is given to answer a connection while it comes up
const CONNECT_DEADLINE: Duration = Duration::from_secs(120);

/// The bytes a second a 1 GbE link carries, which a host's network figures are read against
pub const LINK_BYTES_PER_SEC: f64 = 125_000_000.0;

/// One member of the cluster
pub struct Member {
    /// Its name in the inventory
    pub name: String,
    /// Its node id
    pub node: NodeId,
    /// What ssh reaches its host through
    pub target: String,
    /// A client connected to it as the admin, which the main load drives through
    pub client: Client,
    /// A second client with connections of its own, which the paced stream drives through
    ///
    /// A neighbour is another table's user, so it never shares a connection with the stripe rows:
    /// on a shared one its small answers would wait behind a stripe's, which is X11's finding
    /// about connections and not the node's isolation of tables.
    pub paced: Client,
}

/// What one host's network interfaces moved between two reads
#[derive(Debug, Clone, Copy, Default)]
pub struct Nic {
    /// Bytes received on every physical interface
    pub rx: u64,
    /// Bytes sent on every physical interface
    pub tx: u64,
    /// When it was read
    pub at: Option<Instant>,
}

/// Every member's WAL counters, added up
#[derive(Debug, Clone, Copy, Default)]
pub struct Wal {
    /// Batches written and synced, one `fdatasync` each
    pub syncs: u64,
    /// Bytes those batches held
    pub bytes: u64,
    /// Appends those batches carried
    pub appends: u64,
}

/// One snapshot of everything a cell is read against
pub struct Counters {
    /// The device counters, a host each, in [`Lab::hosts`] order
    pub devices: Vec<devices::Snapshot>,
    /// The network counters, a host each, in the same order
    pub nics: Vec<Nic>,
    /// Every member's WAL counters together
    pub wal: Wal,
    /// Every member's process cpu, in clock ticks, by member name
    pub cpu: BTreeMap<String, u64>,
}

/// What a cell did to every host's devices, network and WAL
#[derive(Debug, Clone, Default)]
pub struct CounterDelta {
    /// Device bytes written, every host's storage devices together
    pub device_written: u64,
    /// Device bytes read, every host's storage devices together
    pub device_read: u64,
    /// Device bytes written on a device holding only WAL roots (`latency`), every host together
    pub wal_device_written: u64,
    /// Device bytes written on a device holding only archive roots (`throughput`)
    pub archive_device_written: u64,
    /// Device bytes written on a device holding both, where the counters cannot split them
    pub shared_device_written: u64,
    /// Device bytes written on each host, by its ssh target
    pub written_by_host: BTreeMap<String, u64>,
    /// Device bytes read on each host, by its ssh target
    pub read_by_host: BTreeMap<String, u64>,
    /// Device flushes, every host together
    pub flushes: u64,
    /// The largest share of a 1 GbE link any host sent or received at over the cell
    pub nic_peak_share: f64,
    /// Bytes every host sent on its network, together
    pub nic_tx: u64,
    /// What the members' WAL writers did
    pub wal: Wal,
    /// Every host's devices as the kernel counted them
    pub hosts: Vec<HostDevices>,
    /// The cpu seconds each member's process spent, by member name
    pub cpu_secs: BTreeMap<String, f64>,
}

/// The cluster: a client a member, and its hosts' storage roots
pub struct Lab {
    /// The inventory the cluster was described by
    pub inventory: Inventory,
    /// Every member, in the inventory's node order
    pub members: Vec<Member>,
    /// Every host with the roots its nodes write under
    pub hosts: Vec<HostRoots>,
}

impl Lab {
    /// Open the deployed cluster an inventory names and connect to every member
    ///
    /// # Arguments
    ///
    /// * `inventory` - The inventory the cluster was deployed from
    ///
    /// # Errors
    ///
    /// When the deployment cannot be read or a member does not answer.
    pub async fn attach(inventory: &Path) -> color_eyre::Result<Lab> {
        // the deployment, without the programs it was built from
        let deployment = Deployment::attach(inventory)?;
        let hosts = devices::host_roots(&deployment.inventory);
        let record = deployment.state.record()?;
        let mut members = Vec::new();
        for (name, node) in &record.nodes {
            // each node's client address, as the deployment advertised it
            let address: std::net::IpAddr = node.address.parse()?;
            let addr = shoaladm::deploy::inventory::socket(address, deployment.inventory.ports.client);
            let client = deployment
                .connect::<StripesAsRowsClient>(&addr, Instant::now() + CONNECT_DEADLINE)
                .await
                .wrap_err_with(|| format!("connecting to {name} at {addr}"))?;
            // and again, for the paced stream's connections
            let paced = deployment
                .connect::<StripesAsRowsClient>(&addr, Instant::now() + CONNECT_DEADLINE)
                .await
                .wrap_err_with(|| format!("connecting to {name} at {addr} for the paced stream"))?;
            let id = Uuid::parse_str(&node.node).wrap_err_with(|| format!("{name}'s node id"))?;
            members.push(Member {
                name: name.clone(),
                node: NodeId(id),
                target: node.target.clone(),
                client,
                paced,
            });
        }
        if members.is_empty() {
            bail!("the deployment records no node");
        }
        Ok(Lab {
            inventory: deployment.inventory.clone(),
            members,
            hosts,
        })
    }

    /// Every member's report of its own groups, in member order
    ///
    /// # Errors
    ///
    /// When a member does not answer or answers something else.
    pub async fn replication(&self) -> color_eyre::Result<Vec<NodeReplication>> {
        let mut reports = Vec::with_capacity(self.members.len());
        for member in &self.members {
            // the node's own report, answered as the json it built
            let value = admin_read(&member.client, AdminKind::Replication).await?;
            let report: NodeReplication = shoal::serde_json::from_value(value)
                .wrap_err_with(|| format!("{}'s replication report", member.name))?;
            reports.push(report);
        }
        Ok(reports)
    }

    /// How many groups of each table each member leads, by member name
    ///
    /// # Arguments
    ///
    /// * `reports` - Every member's replication report, in member order
    #[must_use]
    pub fn leads(&self, reports: &[NodeReplication]) -> BTreeMap<String, usize> {
        let mut leads = BTreeMap::new();
        for (member, report) in self.members.iter().zip(reports) {
            let led = report
                .shards
                .iter()
                .flat_map(|shard| &shard.groups)
                .filter(|group| group.is_leader)
                .count();
            leads.insert(member.name.clone(), led);
        }
        leads
    }

    /// Wait until every group of the spike's tables has a leader and none has moved for a while
    ///
    /// After a bootstrap the balancer moves leaders a few seconds apart, so a leg waits until
    /// they stay where they are.
    ///
    /// # Arguments
    ///
    /// * `quiet` - How long no leader may move
    /// * `limit` - How long to wait at most
    ///
    /// # Errors
    ///
    /// When the leaders do not settle by the limit.
    pub async fn settle_leaders(&self, quiet: Duration, limit: Duration) -> color_eyre::Result<()> {
        let started = Instant::now();
        let mut last: Option<BTreeSet<(u64, usize)>> = None;
        let mut since = Instant::now();
        loop {
            // every group and who leads it, and whether every group a member hosts has one
            let reports = self.replication().await?;
            let mut leaders = BTreeSet::new();
            let mut hosted = BTreeSet::new();
            for (member, report) in reports.iter().enumerate() {
                for group in report.shards.iter().flat_map(|shard| &shard.groups) {
                    hosted.insert(group.group.0);
                    if group.is_leader {
                        leaders.insert((group.group.0, member));
                    }
                }
            }
            let complete = !hosted.is_empty() && leaders.len() == hosted.len();
            // a change starts the quiet period again
            if !complete || last.as_ref() != Some(&leaders) {
                last = Some(leaders);
                since = Instant::now();
            } else if since.elapsed() >= quiet {
                return Ok(());
            }
            if started.elapsed() > limit {
                bail!("the leaders did not settle within {limit:?}");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    /// Wait until every copy has applied what its group committed, every node's compactors
    /// have caught up, and its WAL stopped shrinking
    ///
    /// A merge runs after the write that fills a segment, so a cell's device bytes are read only
    /// once every sealed segment its rows went into is merged. That is when no shard holds a
    /// segment handed to a compactor and the WAL's segments have not fallen for `quiet`. And a
    /// follower can be acknowledged past and still behind in applying, which a read at `One`
    /// through it answers as a missing row, so every copy of every group has to have applied
    /// the same index as every other copy too.
    ///
    /// # Arguments
    ///
    /// * `quiet` - How long the segments have to stay put
    /// * `limit` - How long to wait at most
    ///
    /// Returns how long until the backlog cleared, which leaves out the quiet period it was
    /// then watched for, and whether it settled before the limit.
    ///
    /// # Errors
    ///
    /// When a member cannot be read.
    pub async fn settle_merges(&self, quiet: Duration, limit: Duration) -> color_eyre::Result<(Duration, bool)> {
        let started = Instant::now();
        let mut last: Option<usize> = None;
        let mut since = Instant::now();
        loop {
            let reports = self.replication().await?;
            let segments: usize = reports
                .iter()
                .flat_map(|report| &report.shards)
                .map(|shard| shard.segments)
                .sum();
            // a backlog, a copy behind, or segments still falling, starts the quiet period again
            if !Self::drained(&reports)
                || !Self::applied_alike(&reports)
                || last.is_none_or(|before| segments < before)
            {
                since = Instant::now();
            } else if since.elapsed() >= quiet {
                return Ok((since.duration_since(started), true));
            }
            last = Some(segments);
            if started.elapsed() > limit {
                return Ok((started.elapsed(), false));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Whether every node's compactors have caught up
    ///
    /// # Arguments
    ///
    /// * `reports` - Every member's replication report
    #[must_use]
    pub fn drained(reports: &[NodeReplication]) -> bool {
        reports
            .iter()
            .all(|report| report.shards.iter().all(|shard| shard.compacting.is_empty()))
    }

    /// Whether every copy of every group has applied the same index as every other copy
    ///
    /// # Arguments
    ///
    /// * `reports` - Every member's replication report
    #[must_use]
    pub fn applied_alike(reports: &[NodeReplication]) -> bool {
        // each group's lowest and highest applied index over the copies the members host
        let mut spans: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
        for group in reports.iter().flat_map(|report| &report.shards).flat_map(|shard| &shard.groups) {
            let span = spans.entry(group.group.0).or_insert((u64::MAX, 0));
            span.0 = span.0.min(group.applied);
            span.1 = span.1.max(group.applied);
        }
        spans.values().all(|(low, high)| low == high)
    }

    /// Every member's WAL counters, added up
    ///
    /// # Arguments
    ///
    /// * `reports` - Every member's replication report
    #[must_use]
    pub fn wal(reports: &[NodeReplication]) -> Wal {
        let mut wal = Wal::default();
        for shard in reports.iter().flat_map(|report| &report.shards) {
            wal.syncs += shard.wal_syncs;
            wal.bytes += shard.wal_bytes;
            wal.appends += shard.wal_appends;
        }
        wal
    }

    /// Every member's own figures, in member order, each derived after a moment
    ///
    /// A node derives its figures on a timer, so a read waits until every member's figures were
    /// derived after `after` by its own clock.
    ///
    /// # Arguments
    ///
    /// * `after` - The moment, in milliseconds since the epoch, the figures must be newer than
    ///
    /// # Errors
    ///
    /// When a member does not answer, or its figures do not move past the moment in a minute.
    pub async fn node_stats(&self, after: u64) -> color_eyre::Result<Vec<NodeStats>> {
        let started = Instant::now();
        loop {
            let mut all = Vec::with_capacity(self.members.len());
            for member in &self.members {
                // the member's own figures from its answer, which holds every member's when it leads
                let view = shoaladm::cluster::stats::read_stats(&member.client, None)
                    .await
                    .map_err(|error| eyre!("{}'s figures: {error}", member.name))?;
                let own = view
                    .members
                    .into_iter()
                    .find(|stats| stats.node == member.node)
                    .and_then(|stats| stats.stats)
                    .ok_or_else(|| eyre!("{} did not report its own figures", member.name))?;
                all.push(own);
            }
            // every member's figures are new enough
            if all.iter().all(|stats| stats.at_ms > after) {
                return Ok(all);
            }
            if started.elapsed() > Duration::from_secs(60) {
                bail!("the members' figures did not move past {after} in a minute");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Read every host's device and network counters and every member's WAL counters
    ///
    /// # Errors
    ///
    /// When a host or a member cannot be read.
    pub async fn counters(&self) -> color_eyre::Result<Counters> {
        // the WAL counters first, which are a round trip a member
        let wal = Self::wal(&self.replication().await?);
        // the device counters through shoaladm's own script, every host in parallel
        let devices = devices::read_all(&self.hosts).await.map_err(|error| eyre!(error))?;
        // and the network counters, every host in parallel on blocking threads
        let mut tasks = Vec::with_capacity(self.hosts.len());
        for host in &self.hosts {
            let target = host.target.clone();
            tasks.push(tokio::task::spawn_blocking(move || read_nic(&target)));
        }
        let mut nics = Vec::with_capacity(tasks.len());
        for task in tasks {
            nics.push(task.await.map_err(|error| eyre!("reading a host's network: {error}"))??);
        }
        // and every member's process cpu, through its unit
        let cpu = self.node_cpu().await?;
        Ok(Counters { devices, nics, wal, cpu })
    }

    /// Every member's process cpu so far, in clock ticks, by member name
    ///
    /// A member's process is its unit's main process: `shoal-<cluster>` where a deployment
    /// started it, `shoal-x3-local-<name>` where `x3 local up` did ([`crate::local`]).
    ///
    /// # Errors
    ///
    /// When a host cannot be read.
    pub async fn node_cpu(&self) -> color_eyre::Result<BTreeMap<String, u64>> {
        let mut tasks = Vec::with_capacity(self.members.len());
        for member in &self.members {
            let target = member.target.clone();
            let name = member.name.clone();
            let units = format!(
                "{} {}",
                crate::local::unit_name(&member.name),
                self.inventory.unit_name()
            );
            tasks.push(tokio::task::spawn_blocking(move || -> color_eyre::Result<(String, u64)> {
                // the first of the two units that has a main process, then its utime and stime
                let script = format!(
                    "for unit in {units}; do pid=$(systemctl show -p MainPID --value $unit 2>/dev/null);                      if [ -n \"$pid\" ] && [ \"$pid\" != 0 ]; then cat /proc/$pid/stat; break; fi; done"
                );
                let output = Host { target }.run(&format!("sh -c {}", shoaladm::deploy::remote::quote(&script)))?;
                Ok((name, parse_cpu_ticks(&output).unwrap_or(0)))
            }));
        }
        let mut cpu = BTreeMap::new();
        for task in tasks {
            let (name, ticks) = task.await.map_err(|error| eyre!("reading a member's cpu: {error}"))??;
            cpu.insert(name, ticks);
        }
        Ok(cpu)
    }

    /// What every host's devices and network, and every member's WAL, did between two reads
    ///
    /// # Arguments
    ///
    /// * `before` - The first read
    /// * `after` - The second
    #[must_use]
    pub fn delta(&self, before: &Counters, after: &Counters) -> CounterDelta {
        let mut delta = CounterDelta {
            wal: Wal {
                syncs: after.wal.syncs.saturating_sub(before.wal.syncs),
                bytes: after.wal.bytes.saturating_sub(before.wal.bytes),
                appends: after.wal.appends.saturating_sub(before.wal.appends),
            },
            ..CounterDelta::default()
        };
        // the device counters, every device a root is on, by the roles of the roots on it
        let hosts: Vec<HostDevices> = devices::deltas(&self.hosts, &before.devices, &after.devices);
        for host in &hosts {
            let mut written = 0;
            let mut read = 0;
            for device in &host.devices {
                written += device.written_bytes;
                read += device.read_bytes;
                match role(&device.roots) {
                    Role::Wal => delta.wal_device_written += device.written_bytes,
                    Role::Archives => delta.archive_device_written += device.written_bytes,
                    Role::Shared => delta.shared_device_written += device.written_bytes,
                }
                delta.flushes += device.flushes;
            }
            delta.device_written += written;
            delta.device_read += read;
            delta.written_by_host.insert(host.host.clone(), written);
            delta.read_by_host.insert(host.host.clone(), read);
        }
        delta.hosts = hosts;
        // each member's cpu, ticks at the kernel's hundred a second
        for (name, after_ticks) in &after.cpu {
            let before_ticks = before.cpu.get(name).copied().unwrap_or(0);
            delta
                .cpu_secs
                .insert(name.clone(), after_ticks.saturating_sub(before_ticks) as f64 / 100.0);
        }
        // and the network, each host's busier direction as a share of its link
        for (first, second) in before.nics.iter().zip(&after.nics) {
            let secs = match (first.at, second.at) {
                (Some(start), Some(end)) => end.duration_since(start).as_secs_f64().max(1e-3),
                _ => continue,
            };
            let rx = second.rx.saturating_sub(first.rx) as f64 / secs;
            let tx = second.tx.saturating_sub(first.tx);
            delta.nic_tx += tx;
            let share = (rx.max(tx as f64 / secs)) / LINK_BYTES_PER_SEC;
            delta.nic_peak_share = delta.nic_peak_share.max(share);
        }
        delta
    }

    /// The bytes each member's archives hold on disk, from `du`, every member together
    ///
    /// # Errors
    ///
    /// When a host cannot be read.
    pub fn archive_bytes(&self) -> color_eyre::Result<u64> {
        let mut total = 0;
        for host in &self.hosts {
            for (_, root) in &host.roots {
                // every table's archives directory under the root, in allocated bytes
                let script = format!(
                    "sudo -n sh -c 'du -s -B1 -- {root}/*/archives 2>/dev/null' || true",
                    root = shoaladm::deploy::remote::quote(root)
                );
                let output = Host {
                    target: host.target.clone(),
                }
                .run(&script)?;
                for line in output.lines() {
                    if let Some(bytes) = line.split_whitespace().next().and_then(|raw| raw.parse::<u64>().ok()) {
                        total += bytes;
                    }
                }
            }
        }
        Ok(total)
    }
}

/// What the roots on one device hold
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Only WAL roots, `latency`
    Wal,
    /// Only archive roots, `throughput`
    Archives,
    /// Both, or a root whose role the label does not say
    Shared,
}

/// What the roots on a device hold, from their labels `<node> <role> <path>`
///
/// # Arguments
///
/// * `roots` - The labels of the roots on the device
#[must_use]
pub fn role(roots: &[String]) -> Role {
    // the second word of each label is its role
    let roles: BTreeSet<&str> = roots
        .iter()
        .filter_map(|label| label.split(' ').nth(1))
        .collect();
    match roles.iter().copied().collect::<Vec<_>>().as_slice() {
        ["latency"] => Role::Wal,
        ["throughput"] => Role::Archives,
        _ => Role::Shared,
    }
}

/// Answer an admin read from one member as the json it built
///
/// # Arguments
///
/// * `client` - The member's client
/// * `kind` - The read
///
/// # Errors
///
/// When the member refuses the read or answers something else.
pub async fn admin_read(client: &Client, kind: AdminKind) -> color_eyre::Result<shoal::serde_json::Value> {
    let name = kind.name();
    let response = client
        .admin(&AdminRequest {
            op: Uuid::new_v4(),
            expected_version: 0,
            kind,
        })
        .await
        .map_err(|error| eyre!("{name}: {error:?}"))?;
    match response.outcome {
        Ok(AdminOutcome::Read(value)) => Ok(value),
        Ok(other) => Err(eyre!("{name} answered {other:?}")),
        Err(error) => Err(eyre!("{name}: {} ({:?})", error.msg, error.code())),
    }
}

/// The script that names a host's physical interfaces, then prints the kernel's counters
///
/// Only an interface backed by a device is counted: europa's link is enslaved to a bridge, and
/// the bridge counts every byte its port does, so summing both counts the link twice. Loopback is
/// no device, so europa's local nodes move nothing here.
const NIC_SCRIPT: &str =
    "for i in /sys/class/net/*; do if [ -e \"$i/device\" ]; then echo \"PHYS ${i##*/}\"; fi; done; cat /proc/net/dev";

/// Read a host's network counters: bytes in and out on every physical interface
///
/// # Arguments
///
/// * `target` - What ssh reaches the host through
///
/// # Errors
///
/// When the host cannot be read.
fn read_nic(target: &str) -> color_eyre::Result<Nic> {
    // run under sh so no login shell gives its words a meaning
    let output = Host {
        target: target.to_string(),
    }
    .run(&format!("sh -c {}", shoaladm::deploy::remote::quote(NIC_SCRIPT)))?;
    Ok(parse_nic(&output))
}

/// Sum the bytes in and out of every physical interface from the script's output
///
/// # Arguments
///
/// * `output` - The `PHYS <name>` lines, then `/proc/net/dev`
#[must_use]
pub fn parse_nic(output: &str) -> Nic {
    let mut nic = Nic {
        at: Some(Instant::now()),
        ..Nic::default()
    };
    // the interfaces backed by a device
    let physical: BTreeSet<&str> = output
        .lines()
        .filter_map(|line| line.strip_prefix("PHYS "))
        .map(str::trim)
        .collect();
    for line in output.lines() {
        // an interface's line is `name: rx_bytes … (8 receive figures) tx_bytes …`
        let Some((name, figures)) = line.split_once(':') else {
            continue;
        };
        if !physical.contains(name.trim()) {
            continue;
        }
        let figures: Vec<u64> = figures
            .split_whitespace()
            .filter_map(|figure| figure.parse().ok())
            .collect();
        if figures.len() >= 9 {
            nic.rx += figures[0];
            nic.tx += figures[8];
        }
    }
    nic
}

/// A process's user and system time, in clock ticks, from its `/proc/<pid>/stat` line
///
/// # Arguments
///
/// * `stat` - The line
#[must_use]
pub fn parse_cpu_ticks(stat: &str) -> Option<u64> {
    // utime and stime are fields 14 and 15, counted after the parenthesised command name
    let after = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

/// Milliseconds since the epoch by this host's clock
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only physical interfaces are summed: not loopback, and not a bridge over a port
    #[test]
    fn only_physical_interfaces_are_summed() {
        let output = "PHYS enp4s0
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 1000 10 0 0 0 0 0 0 1000 10 0 0 0 0 0 0
enp4s0: 2000 20 0 0 0 0 0 0 3000 30 0 0 0 0 0 0
   br0: 2000 20 0 0 0 0 0 0 3000 30 0 0 0 0 0 0
";
        let nic = parse_nic(output);
        assert_eq!(nic.rx, 2000);
        assert_eq!(nic.tx, 3000);
    }

    /// A process's cpu is its user and system ticks, past a command name with spaces in it
    #[test]
    fn a_processs_cpu_is_read_from_its_stat() {
        let stat = "1234 (x3 node) S 1 1234 1234 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 7 0 100 0 0";
        assert_eq!(parse_cpu_ticks(stat), Some(300));
        assert_eq!(parse_cpu_ticks(""), None);
    }

    /// A device holding WAL roots alone, archive roots alone, or both, is told apart by label
    #[test]
    fn a_devices_role_is_its_roots_roles() {
        let wal = vec!["titan latency /xfs/shoal-x3".to_string()];
        let archives = vec!["titan throughput /x3-archives/shoal-x3".to_string()];
        let both = vec![
            "a latency /optane/shoal-x3-local/a".to_string(),
            "b throughput /optane/shoal-x3-local/b/archives".to_string(),
        ];
        assert_eq!(role(&wal), Role::Wal);
        assert_eq!(role(&archives), Role::Archives);
        assert_eq!(role(&both), Role::Shared);
        assert_eq!(role(&[]), Role::Shared);
    }
}
