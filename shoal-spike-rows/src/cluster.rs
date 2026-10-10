//! The lab's cluster as the driver sees it: a client a member, and the figures each member keeps
//!
//! Everything here is `shoaladm`'s, reached through its library: the deployment the inventory
//! names, a connection to each member as the admin, a restart over ssh, the `Replication` read
//! every member answers with its own groups, the `Stats` read every member answers with its own
//! figures, and the kernel's device counters for every storage root. The two things `shoaladm`
//! does not read, a host's network counters and the bytes a root holds on disk, are read here
//! the way it reads the device counters: one script a host over ssh.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
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
use shoaladm::deploy::remote::Host;
use shoaladm::deploy::Deployment;

use crate::RowsClient;

/// A client of the spike's schema, connected to one member as the admin
pub type Client = Arc<Shoal<RowsClient>>;

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
    /// A client connected to it as the admin
    pub client: Client,
}

/// One tablet group, as its leader reports it
#[derive(Debug, Clone)]
pub struct Group {
    /// The group's id
    pub id: u64,
    /// The table it serves
    pub table: String,
    /// The tablets it serves
    pub tablets: Vec<u16>,
    /// The member that leads it, by its place in [`Lab::members`]
    pub leader: usize,
    /// The shard of that member that hosts it
    pub shard: u16,
}

/// What one host's network interfaces moved between two reads
#[derive(Debug, Clone, Copy, Default)]
pub struct Nic {
    /// Bytes received on every interface but loopback
    pub rx: u64,
    /// Bytes sent on every interface but loopback
    pub tx: u64,
    /// When it was read
    pub at: Option<Instant>,
}

/// One snapshot of everything a cell is read against: devices and network, a host each
pub struct Counters {
    /// The device counters, a host each, in [`Lab::hosts`] order
    pub devices: Vec<devices::Snapshot>,
    /// The network counters, a host each, in the same order
    pub nics: Vec<Nic>,
}

/// What a cell did to every host's devices and network
#[derive(Debug, Clone, Default)]
pub struct CounterDelta {
    /// Device bytes written, every host's storage devices together
    pub device_written: u64,
    /// Device bytes read, every host's storage devices together
    pub device_read: u64,
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
}

/// The cluster: its deployment, a client a member, and its hosts' storage roots
pub struct Lab {
    /// The inventory the cluster was deployed from
    pub inventory: PathBuf,
    /// The deployment the inventory names
    pub deployment: Deployment,
    /// Every member, in the deployment's node order
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
        let mut lab = Lab {
            inventory: inventory.to_path_buf(),
            deployment,
            members: Vec::new(),
            hosts,
        };
        lab.connect().await?;
        Ok(lab)
    }

    /// Connect to every member again, as after a restart
    ///
    /// # Errors
    ///
    /// When a member does not answer by the deadline.
    pub async fn connect(&mut self) -> color_eyre::Result<()> {
        let record = self.deployment.state.record()?;
        let mut members = Vec::new();
        for (name, node) in &record.nodes {
            // each node's client address, as the deployment advertised it
            let address: std::net::IpAddr = node.address.parse()?;
            let addr = shoaladm::deploy::inventory::socket(address, self.deployment.inventory.ports.client);
            let client = self
                .deployment
                .connect::<RowsClient>(&addr, Instant::now() + CONNECT_DEADLINE)
                .await
                .wrap_err_with(|| format!("connecting to {name} at {addr}"))?;
            let id = Uuid::parse_str(&node.node).wrap_err_with(|| format!("{name}'s node id"))?;
            members.push(Member {
                name: name.clone(),
                node: NodeId(id),
                target: node.target.clone(),
                client,
            });
        }
        if members.is_empty() {
            bail!("the deployment records no node");
        }
        self.members = members;
        Ok(())
    }

    /// The member a node id names, by its place
    ///
    /// # Arguments
    ///
    /// * `node` - The node id
    #[must_use]
    pub fn member_of(&self, node: &NodeId) -> Option<usize> {
        self.members.iter().position(|member| &member.node == node)
    }

    /// Restart every node, connect again, and wait for writes to be admitted
    ///
    /// # Errors
    ///
    /// When a restart fails or the cluster does not take writes again.
    pub async fn restart(&mut self) -> color_eyre::Result<()> {
        // ssh is blocking, so the restarts run off the runtime's threads, on a deployment of
        // their own opened from the same inventory
        let inventory = self.inventory.clone();
        tokio::task::spawn_blocking(move || Deployment::attach(&inventory)?.systemctl("restart", None))
            .await
            .map_err(|error| eyre!("restarting the cluster: {error}"))??;
        // a node still starting refuses, so the connections wait for it
        self.connect().await?;
        shoaladm::deploy::ops::wait_for_writes(&self.members[0].client).await
    }

    /// Activate the newest wire version the members run, which conditional writes need
    ///
    /// # Errors
    ///
    /// When the activation is refused or does not finish.
    pub async fn activate(&self) -> color_eyre::Result<()> {
        // a cluster already at its newest version needs nothing
        let client = &self.members[0].client;
        let model = shoaladm::cluster::poll(client).await.map_err(|error| eyre!(error))?;
        if model.activated_wire < model.wire_range.1 {
            let line = format!("activate {}", model.wire_range.1);
            self.deployment.admin(client, &line, Duration::from_secs(120)).await?;
        }
        Ok(())
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

    /// Every group of a table, with its leader, as each leader reports it
    ///
    /// # Arguments
    ///
    /// * `reports` - Every member's replication report, in member order
    /// * `table` - The table, as the schema spells it
    #[must_use]
    pub fn groups(&self, reports: &[NodeReplication], table: &str) -> Vec<Group> {
        let mut groups = BTreeMap::new();
        for (member, report) in reports.iter().enumerate() {
            for shard in &report.shards {
                for group in &shard.groups {
                    // a group is described by the member that leads it
                    if group.table_name == table && group.is_leader {
                        groups.insert(
                            group.group.0,
                            Group {
                                id: group.group.0,
                                table: table.to_string(),
                                tablets: group.tablet_ids.clone(),
                                leader: member,
                                shard: u16::try_from(shard.shard).unwrap_or(u16::MAX),
                            },
                        );
                    }
                }
            }
        }
        groups.into_values().collect()
    }

    /// Wait until every group of these tables has a leader and none has moved for a while
    ///
    /// After a bootstrap or a restart the balancer moves leaders a few seconds apart, so a cell
    /// aimed at a group led by one host waits until it stays there.
    ///
    /// # Arguments
    ///
    /// * `tables` - The tables whose groups are watched
    /// * `quiet` - How long no leader may move
    /// * `limit` - How long to wait at most
    ///
    /// # Errors
    ///
    /// When the leaders do not settle by the limit.
    pub async fn settle(
        &self,
        tables: &[&str],
        quiet: Duration,
        limit: Duration,
    ) -> color_eyre::Result<BTreeMap<String, Vec<Group>>> {
        let started = Instant::now();
        let mut last: Option<BTreeSet<(u64, usize)>> = None;
        let mut since = Instant::now();
        loop {
            // every table's groups and who leads each
            let reports = self.replication().await?;
            let mut by_table = BTreeMap::new();
            let mut leaders = BTreeSet::new();
            let mut complete = true;
            for table in tables {
                let groups = self.groups(&reports, table);
                // every group of a table has a leader once each member's groups are counted
                let hosted: usize = reports
                    .iter()
                    .map(|report| {
                        report
                            .shards
                            .iter()
                            .flat_map(|shard| &shard.groups)
                            .filter(|group| group.table_name == *table)
                            .count()
                    })
                    .max()
                    .unwrap_or(0);
                if groups.is_empty() || groups.len() < hosted {
                    complete = false;
                }
                leaders.extend(groups.iter().map(|group| (group.id, group.leader)));
                by_table.insert((*table).to_string(), groups);
            }
            // a change starts the quiet period again
            if !complete || last.as_ref() != Some(&leaders) {
                last = Some(leaders);
                since = Instant::now();
            } else if since.elapsed() >= quiet {
                return Ok(by_table);
            }
            if started.elapsed() > limit {
                bail!("the leaders of {tables:?} did not settle within {limit:?}");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    /// Whether every group of these tables has archived and checkpointed all it applied
    ///
    /// A restart applies everything above a group's durable checkpoint again, resident, so a
    /// row is cold after a restart only once its group's checkpoint covers it on every member.
    ///
    /// # Arguments
    ///
    /// * `reports` - Every member's replication report
    /// * `tables` - The tables
    #[must_use]
    pub fn archived(reports: &[NodeReplication], tables: &[&str]) -> bool {
        reports.iter().all(|report| {
            // no compactor backlog on the node
            report.shards.iter().all(|shard| shard.compacting.is_empty())
                && report
                    .shards
                    .iter()
                    .flat_map(|shard| &shard.groups)
                    .filter(|group| tables.contains(&group.table_name.as_str()))
                    .all(|group| {
                        // the archives hold everything in the log, and the file says so
                        group.checkpoint == group.applied
                            && group.applied == group.last_log
                            && group.checkpoint_durable
                    })
        })
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

    /// Every member's own figures, in member order, each derived after a moment
    ///
    /// A node derives its figures on a timer, so a read waits until every member's figures were
    /// derived after `after` by its own clock, which on the lab is the same clock within a
    /// fraction of a second.
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

    /// Read every host's device and network counters
    ///
    /// # Errors
    ///
    /// When a host cannot be read.
    pub async fn counters(&self) -> color_eyre::Result<Counters> {
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
        Ok(Counters { devices, nics })
    }

    /// What every host's devices and network did between two reads
    ///
    /// # Arguments
    ///
    /// * `before` - The first read
    /// * `after` - The second
    #[must_use]
    pub fn delta(&self, before: &Counters, after: &Counters) -> CounterDelta {
        let mut delta = CounterDelta::default();
        // the device counters, every device a root is on
        let hosts: Vec<HostDevices> = devices::deltas(&self.hosts, &before.devices, &after.devices);
        for host in &hosts {
            let written: u64 = host.devices.iter().map(|device| device.written_bytes).sum();
            let read: u64 = host.devices.iter().map(|device| device.read_bytes).sum();
            delta.device_written += written;
            delta.device_read += read;
            delta.read_by_host.insert(host.host.clone(), read);
            delta.flushes += host.devices.iter().map(|device| device.flushes).sum::<u64>();
            delta.written_by_host.insert(host.host.clone(), written);
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

    /// The bytes each storage root holds on disk, by `<node> <directory>`, from `du`
    ///
    /// A root holds `wal/` and a directory a table; both are listed, allocated bytes.
    ///
    /// # Errors
    ///
    /// When a host cannot be read.
    pub fn disk_usage(&self) -> color_eyre::Result<BTreeMap<String, u64>> {
        let mut usage = BTreeMap::new();
        for host in &self.hosts {
            for (label, root) in &host.roots {
                // the node's name is the label's first word
                let node = label.split(' ').next().unwrap_or(label);
                // every directory directly under the root, in allocated bytes
                let script = format!(
                    "sudo -n sh -c 'cd {root} && du -s -B1 -- */ 2>/dev/null' || true",
                    root = shoaladm::deploy::remote::quote(root)
                );
                let output = Host {
                    target: host.target.clone(),
                }
                .run(&script)?;
                for line in output.lines() {
                    let mut fields = line.split_whitespace();
                    if let (Some(bytes), Some(dir)) = (fields.next(), fields.next()) {
                        if let Ok(bytes) = bytes.parse::<u64>() {
                            let dir = dir.trim_end_matches('/');
                            *usage.entry(format!("{node} {dir}")).or_default() += bytes;
                        }
                    }
                }
            }
        }
        Ok(usage)
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
/// the bridge counts every byte its port does, so summing both counts the link twice.
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
PHYS wlan0
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 1000 10 0 0 0 0 0 0 1000 10 0 0 0 0 0 0
enp4s0: 2000 20 0 0 0 0 0 0 3000 30 0 0 0 0 0 0
   br0: 2000 20 0 0 0 0 0 0 3000 30 0 0 0 0 0 0
 wlan0: 5 1 0 0 0 0 0 0 7 1 0 0 0 0 0 0
";
        let nic = parse_nic(output);
        assert_eq!(nic.rx, 2005);
        assert_eq!(nic.tx, 3007);
    }
}
