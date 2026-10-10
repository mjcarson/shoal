//! What each host's block devices did while a run ran ([F71](../../../docs/src/features/bench-device-memory.md))
//!
//! A run's device counters are two reads of `/proc/diskstats` on every host of the inventory,
//! one just before the arm's clock starts and one once its last answer is in, and their
//! difference for each device a node's storage root is on. The script that reads them also
//! says which device each root is on, so a root moved to another device between two runs is
//! still counted where it is. Like every script the bench runs on a host, it is built by a pure
//! function and its output read by another, so both are tested without a host.

use color_eyre::eyre::eyre;
use shoal_loadgen::results::{DeviceCounters, HostDevices};
use std::collections::BTreeMap;

use crate::deploy::inventory::Inventory;
use crate::deploy::remote::{quote, Host};

/// The bytes in a sector as `/proc/diskstats` counts them, whatever the device's own sector size
const SECTOR_BYTES: u64 = 512;

/// One host to read: how it is reached, its nodes, and every storage root of theirs
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostRoots {
    /// How ssh reaches it
    pub target: String,
    /// The nodes it runs, by their names in the inventory
    pub nodes: Vec<String>,
    /// Every root of theirs, as `<node> <role> <path>`, beside its path
    pub roots: Vec<(String, String)>,
}

/// Every host of an inventory with the roots its nodes write under
///
/// Nodes are grouped by how ssh reaches them, since a device belongs to a host: two nodes on
/// one machine share its counters.
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
    }
    hosts.into_values().collect()
}

/// The script that says which device each root is on, then prints the kernel's counters
///
/// A root is resolved through the nearest path of it that exists, so a root the bench has not
/// made yet is counted on the device it will be made on. A btrfs subvolume's `[/path]` is cut
/// from the mount's source and a device mapper name followed to its `dm-N`, which is how
/// `/proc/diskstats` names it. A root on no block device resolves to a name no line has.
///
/// ssh hands a command to the host's login shell, so the script is run under `sh` explicitly:
/// a login shell of zsh ties a variable named `path` to `PATH`, which is how the first version
/// lost every command after its first assignment on europa. Its variables are named so that no
/// shell gives them a meaning either.
///
/// # Arguments
///
/// * `paths` - The roots, in the order their lines are numbered
#[must_use]
pub fn devices_script(paths: &[String]) -> String {
    // each root by its place, then the host's clock, then every device's line
    let paths = paths.iter().map(|path| quote(path)).collect::<Vec<_>>().join(" ");
    let body = format!(
        "place=0; for root in {paths}; do \
           near=\"$root\"; \
           while [ ! -e \"$near\" ] && [ \"$near\" != / ]; do near=$(dirname \"$near\"); done; \
           mounted=$(findmnt -n -o SOURCE -T \"$near\" 2>/dev/null | head -n 1 | sed 's/\\[.*//'); \
           device=$(readlink -f \"$mounted\" 2>/dev/null); \
           printf 'root\\t%s\\t%s\\n' \"$place\" \"${{device##*/}}\"; \
           place=$((place + 1)); \
         done; \
         printf 'uptime\\t%s\\n' \"$(cut -d' ' -f1 /proc/uptime)\"; \
         sed 's/^/disk\\t/' /proc/diskstats"
    );
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

/// One read of a host's devices
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Snapshot {
    /// The host's uptime when it was read, in seconds
    pub uptime: f64,
    /// The device each root is on, by the root's place, as the script resolved it
    pub roots: Vec<String>,
    /// Every device the kernel counts, by name
    pub disks: BTreeMap<String, Disk>,
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
        ..Snapshot::default()
    };
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
    HostDevices {
        host: host.target.clone(),
        nodes: host.nodes.clone(),
        devices,
        unresolved,
    }
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
    .output(&devices_script(&paths), None)?;
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

    /// The script quotes every root, numbers them, and reads the kernel's counters
    #[test]
    fn the_script_names_each_root_by_its_place() {
        let script = devices_script(&["/optane/shoal".to_string(), "/a dir/with space".to_string()]);
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
        let script = devices_script(&[dir.display().to_string()]);
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
            .arg(devices_script(&roots))
            .output()
            .unwrap();
        let snapshot = parse_snapshot(&String::from_utf8_lossy(&output.stdout), 2);
        assert!(snapshot.uptime > 0.0, "{snapshot:?}");
        // both resolve, to the same device, which the kernel counts
        assert!(!snapshot.roots[0].is_empty(), "{snapshot:?}");
        assert_eq!(snapshot.roots[0], snapshot.roots[1]);
        assert!(snapshot.disks.contains_key(&snapshot.roots[0]), "{snapshot:?}");
    }
}
