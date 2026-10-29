//! The cluster as the driver sees it: a client a member, its groups' leaders, and the figures
//! each member keeps
//!
//! X3's (`shoal-spike-bytes/src/cluster.rs`) joined to X10's (`shoal-spike-rows/src/cluster.rs`).
//! Everything here is `shoaladm`'s, reached through its library: a connection to each member as
//! the admin, the `Replication` read every member answers with its own groups and WAL counters,
//! the wire version a conditional write needs, and the kernel's device counters for every storage
//! root. A cluster is opened from a deployment, as the lab's is, or from europa's local nodes,
//! which no deployment can describe ([`crate::local`]). The one thing `shoaladm` does not read, a
//! host's network counters, is read here the way it reads the device counters.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::eyre::{bail, eyre, WrapErr};
use shoal::server::replication::report::NodeReplication;
use shoal::shared::identity::NodeId;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::uuid::Uuid;
use shoal::Shoal;
use shoal_loadgen::results::HostDevices;
use shoaladm::bench::devices::{self, HostRoots};
use shoaladm::deploy::inventory::Inventory;
use shoaladm::deploy::remote::Host;
use shoaladm::deploy::Deployment;

use crate::SmallClient;

/// A client of the spike's schema, connected to one member as the admin
pub type Client = Arc<Shoal<SmallClient>>;

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
    /// The address it serves clients at
    pub address: std::net::IpAddr,
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

/// Which member leads each tablet of a table, so a write is sent to its group's leader
///
/// Every query a cell sends goes to the member that leads the key's group, so no path pays a
/// forward between members that another does not. A client of today's Shoal does not route by
/// topology ([D7](../../docs/src/direction/shard-aware-routing.md)); the hop is the coordinator's
/// and is left out on purpose, and the page says so.
#[derive(Debug, Clone)]
pub struct Route {
    /// The leading member of each tablet, by its place in the lab's members
    by_tablet: Vec<usize>,
}

impl Route {
    /// The route of a table's groups
    ///
    /// # Arguments
    ///
    /// * `groups` - Every group of the table
    #[must_use]
    pub fn of(groups: &[Group]) -> Self {
        // every tablet starts at the first member and is moved to its group's leader
        let mut by_tablet = vec![0; shoal::server::ring::TABLET_COUNT];
        for group in groups {
            for tablet in &group.tablets {
                by_tablet[usize::from(*tablet)] = group.leader;
            }
        }
        Route { by_tablet }
    }

    /// The member leading the group a partition hash lands in
    ///
    /// # Arguments
    ///
    /// * `hash` - The partition hash
    #[must_use]
    pub fn leader_of_hash(&self, hash: u64) -> usize {
        self.by_tablet[usize::from(crate::keys::tablet_of(hash))]
    }
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
    /// The flushes every whole disk under a host's roots completed, a host each
    pub disk_flushes: Vec<u64>,
    /// When it was taken
    pub at: Instant,
}

/// What a cell did to every host's devices, network and WAL
#[derive(Debug, Clone, Default)]
pub struct CounterDelta {
    /// Device bytes written, every host's storage devices together
    pub device_written: u64,
    /// Device bytes read, every host's storage devices together
    pub device_read: u64,
    /// Device bytes written on each host, by its ssh target
    pub written_by_host: BTreeMap<String, u64>,
    /// Flushes the whole disks under each host's roots completed, by its ssh target
    ///
    /// The kernel counts a flush on the whole disk alone: a device mapper volume or a partition
    /// above it counts none, and a disk whose cache writes through, as europa's Optane does, is
    /// sent none to count.
    pub flushes_by_host: BTreeMap<String, u64>,
    /// Those flushes, every host together
    pub flushes: u64,
    /// The largest share of a 1 GbE link any host sent or received at over the span
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

/// The cluster: its deployment, a client a member, and its hosts' storage roots
pub struct Lab {
    /// The inventory's path
    pub path: PathBuf,
    /// The inventory the cluster was described by
    pub inventory: Inventory,
    /// The deployment the inventory names
    pub deployment: Deployment,
    /// Every member, in the deployment record's node order
    pub members: Vec<Member>,
    /// Every host with the roots its nodes write under
    pub hosts: Vec<HostRoots>,
}

impl Lab {
    /// Open the cluster an inventory names and connect to every member
    ///
    /// # Arguments
    ///
    /// * `inventory` - The inventory the cluster was deployed from, or started locally from
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
        let password = deployment.state.password()?;
        for (name, node) in &record.nodes {
            // each node's client address, as the deployment advertised it
            let address: std::net::IpAddr = node.address.parse()?;
            let addr = shoaladm::deploy::inventory::socket(address, deployment.inventory.ports.client);
            let client = connect(&addr, &deployment.inventory.admin, &password)
                .await
                .wrap_err_with(|| format!("connecting to {name} at {addr}"))?;
            let id = Uuid::parse_str(&node.node).wrap_err_with(|| format!("{name}'s node id"))?;
            members.push(Member {
                name: name.clone(),
                node: NodeId(id),
                target: node.target.clone(),
                address,
                client,
            });
        }
        if members.is_empty() {
            bail!("the deployment records no node");
        }
        Ok(Lab {
            path: inventory.to_path_buf(),
            inventory: deployment.inventory.clone(),
            deployment,
            members,
            hosts,
        })
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
    pub fn groups(reports: &[NodeReplication], table: &str) -> Vec<Group> {
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
    /// After a bootstrap the balancer moves leaders a few seconds apart, so a leg waits until
    /// they stay where they are, and is handed each table's groups as they stand.
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
    pub async fn settle_leaders(
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
                let groups = Self::groups(&reports, table);
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

    /// The groups' leaders now, as a set of `(group, member)`, to say whether any moved
    ///
    /// # Errors
    ///
    /// When a member cannot be read.
    pub async fn leaders(&self) -> color_eyre::Result<BTreeSet<(u64, usize)>> {
        let reports = self.replication().await?;
        let mut leaders = BTreeSet::new();
        for (member, report) in reports.iter().enumerate() {
            for group in report.shards.iter().flat_map(|shard| &shard.groups) {
                if group.is_leader {
                    leaders.insert((group.group.0, member));
                }
            }
        }
        Ok(leaders)
    }

    /// Wait until every copy has applied what its group committed, every node's compactors
    /// have caught up, and its WAL stopped shrinking
    ///
    /// X3's settle. A merge runs after the write that fills a segment, so a cell's device bytes
    /// are read only once every sealed segment its rows went into is merged: no shard holds a
    /// segment handed to a compactor and the WAL's segments have not fallen for `quiet`, with
    /// every copy of every group applied to the same index.
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
            tokio::time::sleep(Duration::from_millis(500)).await;
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

    /// Every shard's WAL bytes, by member and shard, which a seal watches each of grow
    ///
    /// # Arguments
    ///
    /// * `reports` - Every member's replication report
    #[must_use]
    pub fn wal_bytes_by_shard(reports: &[NodeReplication]) -> BTreeMap<(usize, usize), u64> {
        let mut bytes = BTreeMap::new();
        for (member, report) in reports.iter().enumerate() {
            for shard in &report.shards {
                bytes.insert((member, shard.shard), shard.wal_bytes);
            }
        }
        bytes
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
        // and the flushes of the whole disks under them, every host in parallel
        let mut tasks = Vec::with_capacity(self.hosts.len());
        for host in &self.hosts {
            let target = host.target.clone();
            let roots: Vec<String> = host.roots.iter().map(|(_, path)| path.clone()).collect();
            tasks.push(tokio::task::spawn_blocking(move || read_disk_flushes(&target, &roots)));
        }
        let mut disk_flushes = Vec::with_capacity(tasks.len());
        for task in tasks {
            disk_flushes.push(task.await.map_err(|error| eyre!("reading a host's disks: {error}"))??);
        }
        // and every member's process cpu, through its unit
        let cpu = self.node_cpu().await?;
        Ok(Counters {
            devices,
            nics,
            wal,
            cpu,
            disk_flushes,
            at: Instant::now(),
        })
    }

    /// Every member's process cpu so far, in clock ticks, by member name
    ///
    /// A member's process is its unit's main process: `shoal-<cluster>` where a deployment
    /// started it, `shoal-x8-local-<name>` where `x8 local up` did ([`crate::local`]).
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
                    "for unit in {units}; do pid=$(systemctl show -p MainPID --value $unit 2>/dev/null); \
                     if [ -n \"$pid\" ] && [ \"$pid\" != 0 ]; then cat /proc/$pid/stat; break; fi; done"
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
        // the device counters, every device a root is on: the node's and its holder's
        let hosts: Vec<HostDevices> = devices::deltas(&self.hosts, &before.devices, &after.devices);
        for host in &hosts {
            let mut written = 0;
            for device in &host.devices {
                written += device.written_bytes;
                delta.device_read += device.read_bytes;
            }
            delta.device_written += written;
            delta.written_by_host.insert(host.host.clone(), written);
        }
        delta.hosts = hosts;
        // the flushes of the whole disks under each host's roots
        for ((host, first), second) in self.hosts.iter().zip(&before.disk_flushes).zip(&after.disk_flushes) {
            let flushes = second.saturating_sub(*first);
            delta.flushes += flushes;
            delta.flushes_by_host.insert(host.target.clone(), flushes);
        }
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
}

/// Connect to one member as the admin, with a pool that never retires a connection
///
/// `Deployment::connect`'s, with one change. Today's client hands a connection back to its pool
/// once a bundle is written, while the answers it owes are still to come, and `bb8` drops a
/// connection past its lifetime, 30 minutes by default, when it comes back: every query in flight
/// on it then fails as `ConnectionLost`, and the node, which sees a clean end of stream, logs
/// nothing ([item 217](../../docs/src/appendix/known-issues.md#217-a-pooled-connection-retired-at-its-lifetime-fails-the-answers-it-still-owes)).
/// A leg runs longer than that, so the spike's pool keeps every connection for its life.
///
/// # Arguments
///
/// * `addr` - The member's client address
/// * `admin` - The admin's name
/// * `password` - The admin's password
///
/// # Errors
///
/// When the member does not accept a connection by the deadline.
pub async fn connect(addr: &str, admin: &str, password: &str) -> color_eyre::Result<Client> {
    let deadline = Instant::now() + CONNECT_DEADLINE;
    loop {
        // the default pool with no lifetime and no idle timeout
        let built = Shoal::<SmallClient>::builder()
            .endpoint(addr)
            .pool(shoal::client::PoolConfig {
                idle_timeout: None,
                max_lifetime: None,
                ..shoal::client::PoolConfig::default()
            })
            .credentials(shoal::shared::auth::Credentials::scram(admin.to_string(), password.to_string()))
            .build()
            .await;
        match built {
            Ok(client) => return Ok(Arc::new(client)),
            // a node that is still starting refuses; keep trying until the deadline
            Err(_) if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(500)).await,
            Err(error) => return Err(eyre!("could not connect to {addr}: {error:?}")),
        }
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

/// The script that names the whole disk under each root, then prints each one's flushes
///
/// `findmnt` names the device a root is mounted from, and `lsblk -s` walks from it down to the
/// disk under it, through a device mapper volume or a partition. A flush is counted on the disk
/// alone, the nineteenth column of `/proc/diskstats`.
///
/// # Arguments
///
/// * `roots` - The roots
#[must_use]
pub fn disk_flushes_script(roots: &[String]) -> String {
    let roots: Vec<String> = roots.iter().map(|root| shoaladm::deploy::remote::quote(root)).collect();
    format!(
        "for r in {}; do d=$(findmnt -no SOURCE -T \"$r\" 2>/dev/null); \
         lsblk -nslo NAME,TYPE \"$d\" 2>/dev/null | awk '$2 == \"disk\" {{ print $1 }}'; done | sort -u | \
         while read n; do awk -v n=\"$n\" '$3 == n {{ print \"FLUSHES\", $3, $19 }}' /proc/diskstats; done",
        roots.join(" ")
    )
}

/// Read the flushes the whole disks under a host's roots completed, together
///
/// # Arguments
///
/// * `target` - What ssh reaches the host through
/// * `roots` - Its roots
///
/// # Errors
///
/// When the host cannot be read.
fn read_disk_flushes(target: &str, roots: &[String]) -> color_eyre::Result<u64> {
    let output = Host {
        target: target.to_string(),
    }
    .run(&format!("sh -c {}", shoaladm::deploy::remote::quote(&disk_flushes_script(roots))))?;
    Ok(parse_disk_flushes(&output))
}

/// Add up the flushes the script printed, one line a disk
///
/// # Arguments
///
/// * `output` - The `FLUSHES <disk> <count>` lines
#[must_use]
pub fn parse_disk_flushes(output: &str) -> u64 {
    output
        .lines()
        .filter_map(|line| line.strip_prefix("FLUSHES "))
        .filter_map(|rest| rest.split_whitespace().nth(1)?.parse::<u64>().ok())
        .sum()
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
        let stat = "1234 (x8 node) S 1 1234 1234 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 7 0 100 0 0";
        assert_eq!(parse_cpu_ticks(stat), Some(300));
        assert_eq!(parse_cpu_ticks(""), None);
    }

    /// A host's disk flushes are the sum of its disks' lines, and nothing else is read
    #[test]
    fn disk_flushes_are_summed_by_disk() {
        let output = "FLUSHES nvme0n1 4227876\nFLUSHES nvme1n1 0\nnoise 7\n";
        assert_eq!(parse_disk_flushes(output), 4_227_876);
        assert_eq!(parse_disk_flushes(""), 0);
        let script = disk_flushes_script(&["/xfs/shoal-x8".to_string()]);
        assert!(script.contains("lsblk -nslo NAME,TYPE"), "{script}");
        assert!(script.contains("/xfs/shoal-x8"), "{script}");
    }

    /// A tablet's leader is its group's, and a tablet no group names falls to the first member
    #[test]
    fn a_route_sends_a_tablet_to_its_groups_leader() {
        let groups = vec![
            Group {
                id: 1,
                table: "t".to_string(),
                tablets: vec![0, 1],
                leader: 2,
                shard: 0,
            },
            Group {
                id: 2,
                table: "t".to_string(),
                tablets: vec![4095],
                leader: 1,
                shard: 3,
            },
        ];
        let route = Route::of(&groups);
        assert_eq!(route.leader_of_hash(0x0010_0000_0000_0000), 2);
        assert_eq!(route.leader_of_hash(u64::MAX), 1);
        assert_eq!(route.leader_of_hash(0x0020_0000_0000_0000), 0);
    }
}
