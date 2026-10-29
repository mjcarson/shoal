//! What each host's block devices did while a run ran ([F71](../../../docs/src/features/bench-device-memory.md))
//!
//! A run's device counters are two reads of `/proc/diskstats` on every host of the inventory,
//! one just before the arm's clock starts and one once its last answer is in, and their
//! difference for each device a node's storage root or pool device is on. The script that reads them also
//! says which device each root is on, so a root moved to another device between two runs is
//! still counted where it is. Like every script the bench runs on a host, it is built by a pure
//! function and its output read by another, so both are tested without a host.

use color_eyre::eyre::eyre;
use shoal_loadgen::results::{
    DeviceCounters, HostCpu, HostDevices, HostMemory, InterfaceCounters, ProcessCounters, RootUsage,
};
use std::collections::BTreeMap;

use crate::deploy::inventory::Inventory;
use crate::deploy::remote::{quote, Host};

/// The bytes in a sector as `/proc/diskstats` counts them, whatever the device's own sector size
const SECTOR_BYTES: u64 = 512;

/// The clock ticks a second `/proc/stat` and `/proc/<pid>/stat` count in, `USER_HZ`, which is 100
/// on every Linux the bench runs on
const TICKS_PER_SEC: f64 = 100.0;

/// One host to read: how it is reached, its nodes, and every storage root and pool device of theirs
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostRoots {
    /// How ssh reaches it
    pub target: String,
    /// The nodes it runs, by their names in the inventory
    pub nodes: Vec<String>,
    /// Every root of theirs, as `<node> <role> <path>`, beside its path: the WAL's, the
    /// archives' and each pool device's
    pub roots: Vec<(String, String)>,
    /// The names the store's processes run under, their programs' file names, whose cpu and
    /// memory are read beside the devices ([F94](../../../docs/src/features/bench-object-endpoints.md))
    pub processes: Vec<String>,
}

/// Every host of an inventory with the roots its nodes write under
///
/// Nodes are grouped by how ssh reaches them, since a device belongs to a host: two nodes on
/// one machine share its counters. A pool device is a root too, labelled `device`: a stripe's
/// bytes are written there, which may be a volume no storage root is on
/// ([F86](../../../docs/src/features/replicated-pools.md)).
///
/// # Arguments
///
/// * `inventory` - The inventory of the cluster driven
#[must_use]
pub fn host_roots(inventory: &Inventory) -> Vec<HostRoots> {
    // each node's roots under its host, in the inventory's order
    let mut hosts: BTreeMap<String, HostRoots> = BTreeMap::new();
    for spec in &inventory.nodes {
        let (storage, _) = inventory.resolve_storage(spec);
        let host = hosts
            .entry(spec.target().to_string())
            .or_insert_with(|| HostRoots {
                target: spec.target().to_string(),
                nodes: Vec::new(),
                roots: Vec::new(),
                processes: Vec::new(),
            });
        host.nodes.push(spec.name.clone());
        // the WAL's root always, and the archives' when it is another directory
        host.roots
            .push((format!("{} latency {}", spec.name, storage.latency), storage.latency.clone()));
        if storage.throughput != storage.latency {
            host.roots.push((
                format!("{} throughput {}", spec.name, storage.throughput),
                storage.throughput.clone(),
            ));
        }
        // and every pool device's, where a pool's stripes are written (F86)
        for device in inventory.resolve_devices(spec) {
            let path = device.path.to_string_lossy().into_owned();
            host.roots.push((format!("{} device {path}", spec.name), path));
        }
    }
    hosts.into_values().collect()
}

/// The script that says which device each root is on, then prints the kernel's counters
///
/// A root is resolved through the nearest path of it that exists, so a root the bench has not
/// made yet is counted on the device it will be made on. A btrfs subvolume's `[/path]` is cut
/// from the mount's source and a device mapper name followed to its `dm-N`, which is how
/// `/proc/diskstats` names it. A root on no block device resolves to a name no line has, and a
/// root that is a block device itself - a raw volume BlueStore writes - resolves to that device
/// ([F94](../../../docs/src/features/bench-object-endpoints.md)), where before F94 it resolved to
/// the devtmpfs `/dev` is.
///
/// Since F94 the same read prints how full each root's filesystem is, the host's cpu, network
/// and memory counters, and each store process's cpu and resident memory, the processes found by
/// their program's file name - the first word of their command line, which a name of sixteen
/// bytes or more is cut from in `comm`.
///
/// ssh hands a command to the host's login shell, so the script is run under `sh` explicitly:
/// a login shell of zsh ties a variable named `path` to `PATH`, which is how the first version
/// lost every command after its first assignment on europa. Its variables are named so that no
/// shell gives them a meaning either.
///
/// # Arguments
///
/// * `paths` - The roots, in the order their lines are numbered
/// * `processes` - The names of the processes to read
#[must_use]
pub fn devices_script(paths: &[String], processes: &[String]) -> String {
    // each root by its place, then the host's clock, then every device's line
    let paths = paths.iter().map(|path| quote(path)).collect::<Vec<_>>().join(" ");
    let mut body = format!(
        "place=0; for root in {paths}; do \
           near=\"$root\"; \
           while [ ! -e \"$near\" ] && [ \"$near\" != / ]; do near=$(dirname \"$near\"); done; \
           if [ -b \"$near\" ]; then device=$(readlink -f \"$near\"); else \
             mounted=$(findmnt -n -o SOURCE -T \"$near\" 2>/dev/null | head -n 1 | sed 's/\\[.*//'); \
             device=$(readlink -f \"$mounted\" 2>/dev/null); \
             df -B1 --output=used,size \"$near\" 2>/dev/null | tail -n 1 | \
               while read -r used size; do printf 'used\\t%s\\t%s\\t%s\\n' \"$place\" \"$used\" \"$size\"; done; \
           fi; \
           printf 'root\\t%s\\t%s\\n' \"$place\" \"${{device##*/}}\"; \
           place=$((place + 1)); \
         done; \
         printf 'uptime\\t%s\\n' \"$(cut -d' ' -f1 /proc/uptime)\"; \
         sed 's/^/disk\\t/' /proc/diskstats; \
         sed -n 's/^cpu  */cpu\\t/p' /proc/stat; \
         tail -n +3 /proc/net/dev | sed 's/^ */net\\t/'; \
         for interface in /sys/class/net/*; do [ -e \"$interface/device\" ] && printf 'nic\\t%s\\n' \"${{interface##*/}}\"; done; \
         sed -n 's/^\\(MemAvailable\\|Cached\\): *\\([0-9]*\\) kB$/mem\\t\\1\\t\\2/p' /proc/meminfo"
    );
    // and every process run under one of the names, by its program's file name
    if !processes.is_empty() {
        let names = processes.iter().map(|name| quote(name)).collect::<Vec<_>>().join(" ");
        body.push_str(&format!(
            "; ps -e -ww -o pid= -o args= | while read -r pid program rest; do \
               base=${{program##*/}}; \
               for want in {names}; do \
                 if [ \"$base\" = \"$want\" ]; then \
                   line=$(cat /proc/$pid/stat 2>/dev/null) || continue; \
                   set -- ${{line##*) }}; \
                   rss=$(sed -n 's/^VmRSS:[^0-9]*\\([0-9]*\\).*/\\1/p' /proc/$pid/status 2>/dev/null); \
                   hwm=$(sed -n 's/^VmHWM:[^0-9]*\\([0-9]*\\).*/\\1/p' /proc/$pid/status 2>/dev/null); \
                   printf 'proc\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' \"$want\" \"$pid\" \"${{12}}\" \"${{13}}\" \"${{rss:-0}}\" \"${{hwm:-0}}\"; \
                 fi; \
               done; \
             done"
        ));
    }
    // whatever shell the host logs in with
    format!("sh -c {}", quote(&body))
}

/// One device's counters as the kernel keeps them, at one moment
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Disk {
    /// Reads completed
    pub reads: u64,
    /// Sectors read
    pub read_sectors: u64,
    /// Writes completed
    pub writes: u64,
    /// Sectors written
    pub written_sectors: u64,
    /// Milliseconds with I/O in flight
    pub busy_ms: u64,
    /// Discards completed
    pub discards: u64,
    /// Sectors discarded
    pub discarded_sectors: u64,
    /// Flush requests completed
    pub flushes: u64,
}

/// One process as a read found it ([F94](../../../docs/src/features/bench-object-endpoints.md))
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Process {
    /// Its id
    pub pid: u64,
    /// The ticks it ran in user mode and in the kernel
    pub ticks: u64,
    /// Its resident memory, in KiB
    pub rss_kib: u64,
    /// The most it ever had resident, in KiB
    pub hwm_kib: u64,
}

/// One read of a host's devices
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Snapshot {
    /// The host's uptime when it was read, in seconds
    pub uptime: f64,
    /// The device each root is on, by the root's place, as the script resolved it
    pub roots: Vec<String>,
    /// Every device the kernel counts, by name
    pub disks: BTreeMap<String, Disk>,
    /// Each root's filesystem's used and whole size in bytes, by the root's place, where it is on
    /// one (F94)
    pub used: Vec<Option<(u64, u64)>>,
    /// The ticks every cpu was busy and the ticks they counted in all, from `/proc/stat` (F94)
    pub cpu: Option<(u64, u64)>,
    /// Each interface's bytes received and sent, by name (F94)
    pub net: BTreeMap<String, (u64, u64)>,
    /// The interfaces with a device under them: a bridge, a veth or a tunnel carries bytes a
    /// device under it carries too, so only these and the loopback are counted
    pub nics: std::collections::BTreeSet<String>,
    /// The memory available and the page cache, in bytes (F94)
    pub memory: (Option<u64>, Option<u64>),
    /// Each process found, by the name it was asked for (F94)
    pub processes: BTreeMap<String, Vec<Process>>,
}

/// Read one line of `/proc/diskstats`
///
/// The fields after the name are reads, reads merged, sectors read, time reading, writes,
/// writes merged, sectors written, time writing, I/O in progress, time doing I/O, weighted
/// time; then from 4.18 four of discards, and from 5.5 two of flushes. A kernel without the
/// later ones reads them as zero.
///
/// # Arguments
///
/// * `line` - The line, without the script's prefix
fn parse_disk(line: &str) -> Option<(String, Disk)> {
    // the major and minor numbers, the name, then the counters
    let fields: Vec<&str> = line.split_whitespace().collect();
    let name = fields.get(2)?.to_string();
    let field = |at: usize| fields.get(at).and_then(|raw| raw.parse::<u64>().ok()).unwrap_or(0);
    // a line too short to carry the writes is not one
    if fields.len() < 14 {
        return None;
    }
    let disk = Disk {
        reads: field(3),
        read_sectors: field(5),
        writes: field(7),
        written_sectors: field(9),
        busy_ms: field(12),
        discards: field(14),
        discarded_sectors: field(16),
        flushes: field(18),
    };
    Some((name, disk))
}

/// Read what [`devices_script`] printed
///
/// # Arguments
///
/// * `output` - What it printed
/// * `roots` - How many roots it was given
#[must_use]
pub fn parse_snapshot(output: &str, roots: usize) -> Snapshot {
    // a root the script printed nothing for resolves to no device
    let mut snapshot = Snapshot {
        roots: vec![String::new(); roots],
        used: vec![None; roots],
        ..Snapshot::default()
    };
    let number = |raw: &str| raw.trim().parse::<u64>().ok();
    for line in output.lines() {
        let Some((kind, rest)) = line.split_once('\t') else {
            continue;
        };
        match kind {
            "root" => {
                let (place, device) = rest.split_once('\t').unwrap_or((rest, ""));
                if let Some(slot) = place.parse::<usize>().ok().and_then(|place| snapshot.roots.get_mut(place)) {
                    *slot = device.trim().to_string();
                }
            }
            "uptime" => snapshot.uptime = rest.trim().parse().unwrap_or(0.0),
            "disk" => {
                if let Some((name, disk)) = parse_disk(rest) {
                    snapshot.disks.insert(name, disk);
                }
            }
            // a root's filesystem: its place, the bytes used and its size (F94)
            "used" => {
                let fields: Vec<&str> = rest.split('\t').collect();
                if let [place, used, size] = fields[..] {
                    if let (Some(slot), Some(used), Some(size)) =
                        (place.parse::<usize>().ok().and_then(|place| snapshot.used.get_mut(place)), number(used), number(size))
                    {
                        *slot = Some((used, size));
                    }
                }
            }
            // user, nice, system, idle, iowait, irq, softirq, steal: busy is all but idle and iowait
            "cpu" => {
                let ticks: Vec<u64> = rest.split_whitespace().take(8).filter_map(number).collect();
                if ticks.len() >= 5 {
                    let total: u64 = ticks.iter().sum();
                    snapshot.cpu = Some((total - ticks[3] - ticks[4], total));
                }
            }
            // `name: received ... sent ...`, the name's colon sometimes against the first count
            "net" => {
                if let Some((name, counts)) = rest.split_once(':') {
                    let counts: Vec<u64> = counts.split_whitespace().filter_map(number).collect();
                    if counts.len() >= 9 {
                        snapshot.net.insert(name.trim().to_string(), (counts[0], counts[8]));
                    }
                }
            }
            // an interface with a device under it, as a bridge or a veth has not
            "nic" => {
                snapshot.nics.insert(rest.trim().to_string());
            }
            "mem" => match rest.split_once('\t') {
                Some(("MemAvailable", kib)) => snapshot.memory.0 = number(kib).map(|kib| kib << 10),
                Some(("Cached", kib)) => snapshot.memory.1 = number(kib).map(|kib| kib << 10),
                _ => (),
            },
            // the name asked for, the pid, user and kernel ticks, resident and high water KiB
            "proc" => {
                let fields: Vec<&str> = rest.split('\t').collect();
                if let [name, pid, user, system, rss, hwm] = fields[..] {
                    if let (Some(pid), Some(user), Some(system)) = (number(pid), number(user), number(system)) {
                        snapshot.processes.entry(name.to_string()).or_default().push(Process {
                            pid,
                            ticks: user + system,
                            rss_kib: number(rss).unwrap_or(0),
                            hwm_kib: number(hwm).unwrap_or(0),
                        });
                    }
                }
            }
            _ => (),
        }
    }
    snapshot
}

/// What one host's devices did between two reads
///
/// A root is counted on the device the second read found it on, and is left unresolved when
/// that device is in neither read: a tmpfs, an overlay, a path on no mount the script could
/// name. A counter that went backwards - a device removed and added again - counts as zero.
///
/// # Arguments
///
/// * `host` - The host and its roots
/// * `before` - The read before the run
/// * `after` - The read after it
#[must_use]
pub fn delta(host: &HostRoots, before: &Snapshot, after: &Snapshot) -> HostDevices {
    // each root under the device it is on, or with the roots on none
    let mut roots: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    let mut unresolved = Vec::new();
    for (place, (label, _)) in host.roots.iter().enumerate() {
        let device = after.roots.get(place).map_or("", String::as_str);
        if device.is_empty() || !before.disks.contains_key(device) || !after.disks.contains_key(device) {
            unresolved.push(label.clone());
            continue;
        }
        roots.entry(device).or_default().push(label.clone());
    }
    // each device's counters, after less before
    let secs = (after.uptime - before.uptime).max(0.0);
    let devices = roots
        .into_iter()
        .map(|(device, roots)| {
            let (old, new) = (before.disks[device], after.disks[device]);
            let less = |pick: fn(&Disk) -> u64| pick(&new).saturating_sub(pick(&old));
            DeviceCounters {
                device: device.to_string(),
                roots,
                secs,
                reads: less(|disk| disk.reads),
                read_bytes: less(|disk| disk.read_sectors) * SECTOR_BYTES,
                writes: less(|disk| disk.writes),
                written_bytes: less(|disk| disk.written_sectors) * SECTOR_BYTES,
                discards: less(|disk| disk.discards),
                discarded_bytes: less(|disk| disk.discarded_sectors) * SECTOR_BYTES,
                flushes: less(|disk| disk.flushes),
                busy_ms: less(|disk| disk.busy_ms),
            }
        })
        .collect();
    // the host's cpus, by the ticks each read counted (F94)
    let cpu = match (before.cpu, after.cpu) {
        (Some((busy_before, total_before)), Some((busy_after, total_after))) => Some(HostCpu {
            busy_secs: busy_after.saturating_sub(busy_before) as f64 / TICKS_PER_SEC,
            total_secs: total_after.saturating_sub(total_before) as f64 / TICKS_PER_SEC,
        }),
        _ => None,
    };
    // every device's interface and the loopback that carried anything, in name order; a read
    // that names no device keeps every interface, as a host without /sys/class/net would
    let interfaces = after
        .net
        .iter()
        .filter(|(name, _)| after.nics.is_empty() || after.nics.contains(*name) || *name == "lo")
        .filter_map(|(name, (received, sent))| {
            let (old_received, old_sent) = before.net.get(name).copied().unwrap_or_default();
            let counted = InterfaceCounters {
                name: name.clone(),
                received_bytes: received.saturating_sub(old_received),
                sent_bytes: sent.saturating_sub(old_sent),
            };
            (counted.received_bytes + counted.sent_bytes > 0).then_some(counted)
        })
        .collect();
    // the memory around it, where both reads had it
    let memory = match (before.memory, after.memory) {
        ((Some(available_before), Some(cached_before)), (Some(available_after), Some(cached_after))) => Some(HostMemory {
            available_before,
            available_after,
            cached_before,
            cached_after,
        }),
        _ => None,
    };
    // each name's processes: the ticks of those alive at both reads, the memory of those at the end
    let processes = after
        .processes
        .iter()
        .map(|(name, now)| {
            let old = before.processes.get(name).map(Vec::as_slice).unwrap_or_default();
            let ticks: u64 = now
                .iter()
                .filter_map(|process| {
                    let earlier = old.iter().find(|earlier| earlier.pid == process.pid)?;
                    Some(process.ticks.saturating_sub(earlier.ticks))
                })
                .sum();
            ProcessCounters {
                name: name.clone(),
                processes: now.len() as u64,
                cpu_secs: ticks as f64 / TICKS_PER_SEC,
                resident_bytes: now.iter().map(|process| process.rss_kib << 10).sum(),
                peak_resident_bytes: now.iter().map(|process| process.hwm_kib << 10).sum(),
            }
        })
        .collect();
    // how full each root was when the run started
    let usage = host
        .roots
        .iter()
        .enumerate()
        .filter_map(|(place, (label, _))| {
            let (used_bytes, size_bytes) = before.used.get(place).copied().flatten()?;
            Some(RootUsage {
                root: label.clone(),
                used_bytes,
                size_bytes,
            })
        })
        .collect();
    HostDevices {
        host: host.target.clone(),
        nodes: host.nodes.clone(),
        devices,
        unresolved,
        cpu,
        interfaces,
        memory,
        processes,
        usage,
    }
}

/// The first root a read found fuller than a share of its filesystem, as a refusal to start an
/// arm on it ([F94](../../../docs/src/features/bench-object-endpoints.md))
///
/// A store measured on a device nearly full is measuring its handling of a full device, so an arm
/// is not started on one: the cleanup between arms is what keeps a device from filling, and a
/// refusal says it did not.
///
/// # Arguments
///
/// * `hosts` - The hosts and their roots
/// * `snapshots` - Each host's read before the arm, in the same order
/// * `max_percent` - The fullest a root's filesystem may be
#[must_use]
pub fn refuse_full(hosts: &[HostRoots], snapshots: &[Snapshot], max_percent: f64) -> Option<String> {
    // every root of every host, in order
    for (host, snapshot) in hosts.iter().zip(snapshots) {
        for (place, (label, _)) in host.roots.iter().enumerate() {
            let Some((used, size)) = snapshot.used.get(place).copied().flatten() else {
                continue;
            };
            let percent = if size == 0 { 0.0 } else { used as f64 * 100.0 / size as f64 };
            if percent > max_percent {
                return Some(format!(
                    "{}: {label} is {percent:.1}% full, past the {max_percent}% an arm starts under",
                    host.target
                ));
            }
        }
    }
    None
}

/// Drop every host's page cache, after writing back what is dirty, so each run's reads start
/// from the devices on every store alike ([F94](../../../docs/src/features/bench-object-endpoints.md))
///
/// # Arguments
///
/// * `hosts` - The hosts
///
/// # Errors
///
/// When any host cannot, naming it: it needs passwordless sudo for `tee`.
pub async fn drop_caches(hosts: &[HostRoots]) -> Result<(), String> {
    // every host at once, each over its own ssh session
    let drops: Vec<_> = hosts
        .iter()
        .map(|host| host.target.clone())
        .map(|target| {
            tokio::task::spawn_blocking(move || {
                let script = "sh -c 'sync && echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null'";
                let output = Host { target: target.clone() }.output(script, None).map_err(|error| format!("{target}: {error}"))?;
                match output.status {
                    Some(0) => Ok(()),
                    status => Err(format!("{target}: dropping the page cache exited {status:?}: {}", output.stderr.trim())),
                }
            })
        })
        .collect();
    for drop in drops {
        drop.await.map_err(|error| format!("a cache drop panicked: {error}"))??;
    }
    Ok(())
}

/// Read one host's devices
///
/// # Arguments
///
/// * `host` - The host and its roots
///
/// # Errors
///
/// When the host cannot be reached or the script fails there, saying what it printed rather
/// than the whole script, since the reason is kept in every run's capture.
pub fn read(host: &HostRoots) -> color_eyre::Result<Snapshot> {
    // the roots by place, in the order the labels are kept
    let paths: Vec<String> = host.roots.iter().map(|(_, path)| path.clone()).collect();
    let output = Host {
        target: host.target.clone(),
    }
    .output(&devices_script(&paths, &host.processes), None)?;
    // 255 is ssh's own failure, any other the script's
    match output.status {
        Some(0) => Ok(parse_snapshot(&output.stdout, paths.len())),
        Some(255) => Err(eyre!("not reached over ssh ({})", output.stderr.trim())),
        status => Err(eyre!("the device read exited {status:?}: {}", output.stderr.trim())),
    }
}

/// Read every host's devices at once, each over its own ssh session
///
/// # Arguments
///
/// * `hosts` - The hosts and their roots
///
/// # Errors
///
/// When any host cannot be read, naming it.
pub async fn read_all(hosts: &[HostRoots]) -> Result<Vec<Snapshot>, String> {
    // every host's read started before any is waited on, since each is a round trip of its own
    let reads: Vec<_> = hosts
        .iter()
        .cloned()
        .map(|host| tokio::task::spawn_blocking(move || read(&host).map_err(|error| format!("{}: {error}", host.target))))
        .collect();
    let mut snapshots = Vec::with_capacity(reads.len());
    for read in reads {
        snapshots.push(read.await.map_err(|error| format!("a device read panicked: {error}"))??);
    }
    Ok(snapshots)
}

/// What every host's devices did between two reads of all of them
///
/// # Arguments
///
/// * `hosts` - The hosts and their roots
/// * `before` - Each host's read before the run, in the same order
/// * `after` - Each host's read after it
#[must_use]
pub fn deltas(hosts: &[HostRoots], before: &[Snapshot], after: &[Snapshot]) -> Vec<HostDevices> {
    // host by host, in the order they were read
    hosts
        .iter()
        .zip(before.iter().zip(after))
        .map(|(host, (before, after))| delta(host, before, after))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{delta, devices_script, parse_snapshot, HostRoots, Snapshot};

    /// A host with a WAL root and an archive root, the second on a device of its own
    fn host() -> HostRoots {
        HostRoots {
            target: "titan".to_string(),
            nodes: vec!["titan".to_string()],
            roots: vec![
                ("titan latency /optane/shoal".to_string(), "/optane/shoal".to_string()),
                ("titan throughput /data/shoal".to_string(), "/data/shoal".to_string()),
                ("titan throughput /dev/shm/shoal".to_string(), "/dev/shm/shoal".to_string()),
            ],
            processes: Vec::new(),
        }
    }

    /// What the script prints on a host: three roots, its clock and three devices, the way a
    /// 6.x kernel spells a partition, a device mapper volume and a whole disk
    ///
    /// # Arguments
    ///
    /// * `uptime` - The host's clock
    /// * `dm_written` - The sectors the device mapper volume has written
    /// * `nvme_written` - The sectors the partition has written
    fn output(uptime: f64, dm_written: u64, nvme_written: u64) -> String {
        format!(
            "root\t0\tdm-0\nroot\t1\tnvme0n1p2\nroot\t2\ttmpfs\nuptime\t{uptime}\n\
             disk\t 259       0 nvme0n1 900 0 7200 10 900 0 7200 10 0 20 20 0 0 0 0 0 0\n\
             disk\t 259       2 nvme0n1p2 100 3 800 4 50 2 {nvme_written} 6 0 70 80 2 0 64 1 5 3\n\
             disk\t 252       0 dm-0 40 0 320 1 30 0 {dm_written} 9 0 33 44 0 0 0 0 0 0\n"
        )
    }

    /// A node's pool devices are read beside its roots, labelled as devices, since a pool's
    /// stripes are written there and the volume may be one no root is on (F86)
    #[test]
    fn pool_devices_are_read_beside_the_roots() {
        let server = std::env::current_exe().expect("the test binary");
        let yaml = format!(
            "server: {}\nname: lab\nnodes:\n  - name: titan\n    storage: {{latency: /optane/shoal}}\n    \
             devices:\n      - {{path: /xfs/pool, class: ssd}}\n  - name: hyperion\n",
            server.display()
        );
        let inventory: crate::deploy::inventory::Inventory = serde_yaml::from_str(&yaml).expect("an inventory");
        let hosts = super::host_roots(&inventory);
        let titan = hosts.iter().find(|host| host.target == "titan").expect("titan");
        assert_eq!(
            titan.roots,
            vec![
                ("titan latency /optane/shoal".to_string(), "/optane/shoal".to_string()),
                ("titan device /xfs/pool".to_string(), "/xfs/pool".to_string()),
            ]
        );
        // a node with no device reads its roots alone
        let hyperion = hosts.iter().find(|host| host.target == "hyperion").expect("hyperion");
        assert!(hyperion.roots.iter().all(|(label, _)| !label.contains(" device ")), "{hyperion:?}");
    }

    /// The script quotes every root, numbers them, and reads the kernel's counters
    #[test]
    fn the_script_names_each_root_by_its_place() {
        let script = devices_script(&["/optane/shoal".to_string(), "/a dir/with space".to_string()], &[]);
        // run under sh whatever the login shell, with each root quoted inside that quoting
        assert!(script.starts_with("sh -c '"), "{script}");
        assert!(script.contains(r#"for root in /optane/shoal '\''/a dir/with space'\''"#), "{script}");
        assert!(script.contains("findmnt -n -o SOURCE -T"), "{script}");
        assert!(script.contains("/proc/diskstats") && script.contains("/proc/uptime"), "{script}");
    }

    /// A read names each root's device, the clock, and every device's counters
    #[test]
    fn a_read_is_parsed() {
        let snapshot = parse_snapshot(&output(100.5, 1000, 2000), 3);
        assert_eq!(snapshot.roots, vec!["dm-0", "nvme0n1p2", "tmpfs"]);
        assert_eq!(snapshot.uptime, 100.5);
        let partition = snapshot.disks["nvme0n1p2"];
        assert_eq!((partition.reads, partition.read_sectors), (100, 800));
        assert_eq!((partition.writes, partition.written_sectors), (50, 2000));
        assert_eq!((partition.busy_ms, partition.discards, partition.discarded_sectors), (70, 2, 64));
        assert_eq!(partition.flushes, 5);
        // a root the script printed nothing for is on no device
        assert_eq!(parse_snapshot("uptime\t1\n", 2).roots, vec!["", ""]);
    }

    /// Two reads become what each device did, a root on no device is named, and a counter that
    /// went backwards counts as nothing
    #[test]
    fn two_reads_become_what_each_device_did() {
        let before = parse_snapshot(&output(100.0, 1000, 2000), 3);
        let after = parse_snapshot(&output(110.0, 3000, 1000), 3);
        let host = delta(&host(), &before, &after);
        assert_eq!(host.host, "titan");
        assert_eq!(host.unresolved, vec!["titan throughput /dev/shm/shoal"]);
        let names: Vec<&str> = host.devices.iter().map(|device| device.device.as_str()).collect();
        assert_eq!(names, vec!["dm-0", "nvme0n1p2"]);
        let dm = &host.devices[0];
        assert_eq!(dm.roots, vec!["titan latency /optane/shoal"]);
        assert_eq!((dm.written_bytes, dm.secs), (2000 * 512, 10.0));
        // the partition's writes went backwards, as a device removed and added again would
        assert_eq!(host.devices[1].written_bytes, 0);
    }

    /// Two roots on one device are one device with both roots, counted once
    #[test]
    fn two_roots_on_one_device_are_counted_once() {
        let shared = "root\t0\tdm-0\nroot\t1\tdm-0\nuptime\t1\n\
                      disk\t 252 0 dm-0 0 0 0 0 0 0 100 0 0 0 0\n";
        let later = shared.replace(" 100 ", " 300 ");
        let mut both = host();
        both.roots.truncate(2);
        let counted = delta(&both, &parse_snapshot(shared, 2), &parse_snapshot(&later, 2));
        assert_eq!(counted.devices.len(), 1);
        assert_eq!(counted.devices[0].roots.len(), 2);
        assert_eq!(counted.devices[0].written_bytes, 200 * 512);
        // a kernel before 4.18 has no discard or flush fields, which read as zero
        assert_eq!(counted.devices[0].discards, 0);
        // and a host the second read could not resolve leaves every root unresolved
        let none = delta(&both, &Snapshot::default(), &Snapshot::default());
        assert!(none.devices.is_empty() && none.unresolved.len() == 2);
    }

    /// The script resolves a root whatever shell a host logs in with: ssh hands it to the login
    /// shell, and zsh, europa's, ties a variable named `path` to `PATH`
    #[test]
    fn the_script_runs_under_every_login_shell() {
        // every shell this machine has, each given the script as ssh would give it
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../target");
        std::fs::create_dir_all(&dir).unwrap();
        let script = devices_script(&[dir.display().to_string()], &["sh".to_string()]);
        let mut ran = 0;
        for shell in ["sh", "bash", "dash", "zsh"] {
            let Ok(output) = std::process::Command::new(shell).arg("-c").arg(&script).output() else {
                continue;
            };
            ran += 1;
            let snapshot = parse_snapshot(&String::from_utf8_lossy(&output.stdout), 1);
            assert!(
                !snapshot.roots[0].is_empty() && snapshot.disks.contains_key(&snapshot.roots[0]),
                "{shell} did not resolve the root: {}{snapshot:?}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(ran > 0, "no shell ran the script");
    }

    /// The script run on this machine resolves a directory under the target dir to a device
    /// the kernel counts, which is what the bench does on every host
    #[test]
    fn the_script_resolves_a_real_directory() {
        // a machine without findmnt cannot resolve anything, so it proves nothing here
        let findmnt = std::process::Command::new("sh").arg("-c").arg("command -v findmnt").output();
        if !findmnt.is_ok_and(|output| output.status.success()) {
            eprintln!("no findmnt on this machine; skipped");
            return;
        }
        // a root that exists, and one under it that does not yet
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../target");
        std::fs::create_dir_all(&dir).unwrap();
        let roots = vec![
            dir.display().to_string(),
            dir.join("shoaladm-devices-test/not/made/yet").display().to_string(),
        ];
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(devices_script(&roots, &[]))
            .output()
            .unwrap();
        let snapshot = parse_snapshot(&String::from_utf8_lossy(&output.stdout), 2);
        assert!(snapshot.uptime > 0.0, "{snapshot:?}");
        // both resolve, to the same device, which the kernel counts
        assert!(!snapshot.roots[0].is_empty(), "{snapshot:?}");
        assert_eq!(snapshot.roots[0], snapshot.roots[1]);
        assert!(snapshot.disks.contains_key(&snapshot.roots[0]), "{snapshot:?}");
    }

    /// What the script prints for the host's own counters on a 7.x kernel, at two moments
    ///
    /// # Arguments
    ///
    /// * `busy` - The ticks the cpus were busy in user mode
    /// * `received` - The bytes enp3s0 received
    /// * `ticks` - The store's process's ticks
    fn counters(busy: u64, received: u64, ticks: u64) -> String {
        format!(
            "used\t0\t1000\t4000\nroot\t0\tdm-0\nuptime\t5\n\
             disk\t 252 0 dm-0 0 0 0 0 0 0 0 0 0 0 0\n\
             cpu\t{busy} 0 100 9000 50 0 10 0 0 0\n\
             net\tlo: 77 1 0 0 0 0 0 0 77 1 0 0 0 0 0 0\n\
             net\tenp3s0:{received} 9 0 0 0 0 0 0 500 4 0 0 0 0 0 0\n\
             net\tbr0: {received} 9 0 0 0 0 0 0 500 4 0 0 0 0 0 0\n\
             nic\tenp3s0\n\
             mem\tMemAvailable\t2048\nmem\tCached\t1024\n\
             proc\tminio\t42\t{ticks}\t50\t4096\t8192\n\
             proc\tminio\t43\t7\t0\t1\t2\n"
        )
    }

    /// A read's host counters parse, and two of them become what the host and its store did:
    /// cpu-seconds busy, bytes each interface carried, the memory around it, and the store's
    /// processes' cpu and memory, a process that started between the reads costing nothing
    #[test]
    fn host_counters_parse_and_subtract() {
        let before = parse_snapshot(&counters(1000, 10_000, 100), 1);
        assert_eq!(before.used, vec![Some((1000, 4000))]);
        assert_eq!(before.cpu, Some((1110, 10_160)));
        assert_eq!(before.net["enp3s0"], (10_000, 500));
        assert_eq!(before.memory, (Some(2 << 20), Some(1 << 20)));
        assert_eq!(before.processes["minio"].len(), 2);
        // pid 43 is gone by the second read, and pid 44 new in it
        let after = parse_snapshot(
            &counters(1500, 60_000, 400).replace("proc\tminio\t43\t7\t0\t1\t2", "proc\tminio\t44\t9\t0\t1\t2"),
            1,
        );
        let mut host = host();
        host.roots.truncate(1);
        let counted = delta(&host, &before, &after);
        let cpu = counted.cpu.expect("both reads had the cpu");
        assert_eq!((cpu.busy_secs, cpu.total_secs), (5.0, 5.0));
        // lo carried nothing between the reads, and br0 bridges enp3s0, so only enp3s0 is kept
        assert_eq!(counted.interfaces.len(), 1);
        assert_eq!((counted.interfaces[0].name.as_str(), counted.interfaces[0].received_bytes), ("enp3s0", 50_000));
        let minio = &counted.processes[0];
        assert_eq!((minio.name.as_str(), minio.processes, minio.cpu_secs), ("minio", 2, 3.0));
        assert_eq!((minio.resident_bytes, minio.peak_resident_bytes), ((4096 + 1) << 10, (8192 + 2) << 10));
        assert_eq!(counted.usage[0].percent(), 25.0);
        // and a root past a share of its filesystem refuses the next arm, by name
        assert!(super::refuse_full(&[host.clone()], &[before.clone()], 20.0).unwrap().contains("25.0% full"));
        assert!(super::refuse_full(&[host], &[before], 30.0).is_none());
    }

    /// A root that is a block device, as a raw volume BlueStore writes is, resolves to that
    /// device rather than to the devtmpfs `/dev` is on (F94)
    #[test]
    fn a_block_device_root_resolves_to_itself() {
        // any block device this machine has, by the name the kernel counts it under
        let Some(device) = std::fs::read_dir("/sys/block")
            .ok()
            .and_then(|mut entries| entries.find_map(|entry| entry.ok()))
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| std::path::Path::new("/dev").join(name).exists())
        else {
            eprintln!("no block device to resolve on this machine; skipped");
            return;
        };
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(devices_script(&[format!("/dev/{device}")], &[]))
            .output()
            .unwrap();
        let snapshot = parse_snapshot(&String::from_utf8_lossy(&output.stdout), 1);
        assert_eq!(snapshot.roots[0], device, "{snapshot:?}");
        assert!(snapshot.disks.contains_key(&device));
        // a device has no filesystem to be full
        assert_eq!(snapshot.used, vec![None]);
    }

    /// A process is found by its program's file name even past the sixteen bytes `comm` keeps,
    /// as `tmdb-dataset-node`'s seventeen are, and its cpu and memory are read (F94)
    #[test]
    fn processes_match_by_argv0() {
        // this test's own binary, whose name is longer than comm keeps
        let exe = std::env::current_exe().unwrap();
        let name = exe.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.len() > 16, "{name}");
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(devices_script(&[], &[name.clone(), "no-such-program".to_string()]))
            .output()
            .unwrap();
        let snapshot = parse_snapshot(&String::from_utf8_lossy(&output.stdout), 0);
        let found = snapshot.processes.get(&name).unwrap_or_else(|| panic!("{snapshot:?}"));
        assert!(found.iter().any(|process| process.pid == u64::from(std::process::id())), "{found:?}");
        assert!(found.iter().all(|process| process.rss_kib > 0 && process.hwm_kib >= process.rss_kib));
        assert!(!snapshot.processes.contains_key("no-such-program"));
        // and the host's own counters came with it
        assert!(snapshot.cpu.is_some() && !snapshot.net.is_empty() && snapshot.memory.0.is_some(), "{snapshot:?}");
    }
}
