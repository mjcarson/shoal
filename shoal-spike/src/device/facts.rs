//! What every table is labelled with, and the probes that say what the counters mean
//!
//! A figure from X6 is worth nothing without the host, cpu, governor, device, firmware,
//! filesystem and mount options it came from, so every table carries them. They are read from
//! the kernel, never typed in. The probes come first in every run: they check that direct I/O
//! is direct, find which device counts a flush, find whether a directory's `fdatasync` makes a
//! rename durable, and find whether the filesystem can clone. The measurements after them lean
//! on those answers.
//!
//! X7 adds what a rotational disk's figures need beside them: its rotation rate from the disk's
//! own characteristics page, its whole model from udev's name for it (the kernel's is cut at
//! sixteen characters), whether it takes forced unit access, its scheduler and both queues. The
//! probes' figures become a record too, so a report can read a disk's idle sync.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use glommio::io::{Directory, DmaFile};

use super::counters::Devices;
use super::io::{self, DirSync, ALIGN};
use super::stats::{fmt, Samples};
use super::sys;
use super::table::Table;
use crate::placement::timing::cpu_model;

/// Where a run is, and on what
#[derive(Debug, Clone)]
pub struct Facts {
    /// The host's name
    pub host: String,
    /// The cpu's model
    pub cpu: String,
    /// The governor cpu 0 runs under
    pub governor: String,
    /// The kernel's release
    pub kernel: String,
    /// The filesystem's type
    pub fs: String,
    /// Where it is mounted
    pub mount: PathBuf,
    /// The mount's options, the per-mount ones then the filesystem's
    pub options: String,
    /// What it is mounted from
    pub source: String,
    /// The two devices a side is counted on
    pub devices: Devices,
    /// The disk's model
    pub model: String,
    /// The disk's firmware
    pub firmware: String,
    /// The disk's cache as the kernel sees it: `write back` or `write through`
    pub write_cache: String,
    /// The disk's logical block size
    pub logical_block: u64,
    /// The disk's physical block size
    pub physical_block: u64,
    /// The filesystem's block size
    pub block: u64,
    /// Whether the kernel calls the disk rotational
    pub rotational: bool,
    /// The disk's rotation rate in rpm, from its block device characteristics page; zero if it
    /// does not spin or does not say
    pub rpm: u32,
    /// Whether the disk takes a write with forced unit access, so a sync need not flush its cache
    pub fua: bool,
    /// The disk's I/O scheduler, the one in brackets
    pub scheduler: String,
    /// Requests the block layer queues for the disk
    pub nr_requests: u64,
    /// Commands the disk itself queues (NCQ on a SATA disk)
    pub queue_depth: u64,
    /// The filesystem device's size in bytes, which a physical offset is a fraction of
    pub fs_bytes: u64,
}

impl Facts {
    /// Read every fact about the filesystem a directory is on, and the host
    ///
    /// # Arguments
    ///
    /// * `dir` - The scratch directory
    #[must_use]
    pub fn gather(dir: &Path) -> Facts {
        // the mount the directory is on, then the device under it, then the disk under that
        let dir = std::fs::canonicalize(dir).expect("the scratch directory exists");
        let (mount, fs, source, options) = mount_of(&dir);
        let fs_dev = device_name(&source);
        let disk = disk_of(&fs_dev);
        let attr = |name: &str| {
            std::fs::read_to_string(format!("/sys/class/block/{disk}/{name}"))
                .map(|text| text.trim().to_string())
                .unwrap_or_else(|_| "unknown".to_string())
        };
        // the filesystem device's size, in the block layer's 512 byte sectors
        let fs_bytes = std::fs::read_to_string(format!("/sys/class/block/{fs_dev}/size"))
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok())
            .map_or(0, |sectors| sectors * 512);
        // SAFETY: a zeroed statvfs is a valid out parameter, filled by the call
        let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
        unsafe { libc::statvfs(sys::cpath(&dir).as_ptr(), &mut vfs) };
        Facts {
            host: crate::hostname(),
            cpu: cpu_model(),
            governor: crate::governor(),
            kernel: std::fs::read_to_string("/proc/sys/kernel/osrelease")
                .map(|text| text.trim().to_string())
                .unwrap_or_default(),
            fs,
            mount,
            options,
            source,
            devices: Devices { fs: fs_dev, disk: disk.clone() },
            model: full_model(&disk).unwrap_or_else(|| attr("device/model")),
            // an NVMe controller names its firmware `firmware_rev`, a SCSI or SATA disk `rev`
            firmware: match attr("device/firmware_rev").as_str() {
                "unknown" => attr("device/rev"),
                known => known.to_string(),
            },
            write_cache: attr("queue/write_cache"),
            logical_block: attr("queue/logical_block_size").parse().unwrap_or(512),
            physical_block: attr("queue/physical_block_size").parse().unwrap_or(512),
            block: vfs.f_bsize,
            rotational: attr("queue/rotational") == "1",
            rpm: rotation_rate(&disk),
            fua: attr("queue/fua") == "1",
            scheduler: bracketed(&attr("queue/scheduler")),
            nr_requests: attr("queue/nr_requests").parse().unwrap_or(0),
            queue_depth: attr("device/queue_depth").parse().unwrap_or(0),
            fs_bytes,
        }
    }

    /// The disk's kind in a few words: its rotation rate, or that it is solid state
    #[must_use]
    pub fn kind(&self) -> String {
        match (self.rotational, self.rpm) {
            (true, 0) => "rotational".to_string(),
            (true, rpm) => format!("{rpm} rpm"),
            (false, _) => "solid state".to_string(),
        }
    }

    /// The line every table carries
    #[must_use]
    pub fn label(&self) -> String {
        // a disk's queue is worth naming when it spins, since the scheduler and NCQ order its seeks
        let queue = if self.rotational {
            format!(
                ", {}, fua {}, {} queued, NCQ {}",
                self.scheduler,
                if self.fua { "yes" } else { "no" },
                self.nr_requests,
                self.queue_depth
            )
        } else {
            String::new()
        };
        format!(
            "{} · {} · governor {} · kernel {} · {} {} ({}, fw {}, cache {}, {}/{} B blocks{queue}) · {} at {} on {} = {} ({}) · fs block {} B",
            self.host,
            self.cpu,
            self.governor,
            self.kernel,
            self.devices.disk,
            self.model,
            self.kind(),
            self.firmware,
            self.write_cache,
            self.logical_block,
            self.physical_block,
            self.fs,
            self.mount.display(),
            self.source,
            self.devices.fs,
            self.options,
            self.block,
        )
    }
}

/// The mount a path is on: its mount point, type, source and options
///
/// The longest mount point that is a prefix of the path, read from `/proc/self/mountinfo`.
///
/// # Arguments
///
/// * `path` - A canonical path
fn mount_of(path: &Path) -> (PathBuf, String, String, String) {
    let text = std::fs::read_to_string("/proc/self/mountinfo").expect("mountinfo is readable");
    let mut best: Option<(PathBuf, String, String, String)> = None;
    for line in text.lines() {
        // the fields before the separator, then the type, source and super options after it
        let Some((before, after)) = line.split_once(" - ") else { continue };
        let before: Vec<&str> = before.split(' ').collect();
        let after: Vec<&str> = after.split(' ').collect();
        if before.len() < 6 || after.len() < 3 {
            continue;
        }
        let point = PathBuf::from(before[4].replace("\\040", " "));
        if !path.starts_with(&point) {
            continue;
        }
        let longer = best
            .as_ref()
            .is_none_or(|(known, ..)| point.components().count() >= known.components().count());
        if longer {
            best = Some((
                point,
                after[0].to_string(),
                after[1].to_string(),
                format!("{},{}", before[5], after[2]),
            ));
        }
    }
    best.expect("the path is on a mount")
}

/// The text in brackets of a sysfs choice, such as the scheduler in `none [mq-deadline]`
///
/// # Arguments
///
/// * `text` - The attribute's text
fn bracketed(text: &str) -> String {
    text.split_once('[')
        .and_then(|(_, rest)| rest.split_once(']'))
        .map_or_else(|| text.to_string(), |(chosen, _)| chosen.to_string())
}

/// A SATA disk's whole model, from its link under `/dev/disk/by-id`
///
/// The kernel's `device/model` is the SCSI inquiry's sixteen characters, which cuts a SATA
/// model short. udev names the disk `ata-<model>_<serial>`, spaces as underscores.
///
/// # Arguments
///
/// * `disk` - The disk as `/sys/class/block` names it
fn full_model(disk: &str) -> Option<String> {
    let entries = std::fs::read_dir("/dev/disk/by-id").ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        // the whole disk's link, not a partition's
        let Some(rest) = name.strip_prefix("ata-") else { continue };
        if rest.contains("-part") {
            continue;
        }
        let target = std::fs::canonicalize(entry.path()).ok()?;
        if target.file_name().is_some_and(|file| file == disk) {
            // the serial follows the last underscore
            let model = rest.rsplit_once('_').map_or(rest, |(model, _)| model);
            return Some(model.replace('_', " "));
        }
    }
    None
}

/// A disk's rotation rate in rpm, from its block device characteristics page (VPD B1)
///
/// Bytes 4 and 5 are the medium rotation rate: 1 for a disk that does not spin, a rate in rpm
/// otherwise, and 0 for one that does not say.
///
/// # Arguments
///
/// * `disk` - The disk as `/sys/class/block` names it
fn rotation_rate(disk: &str) -> u32 {
    let page = std::fs::read(format!("/sys/class/block/{disk}/device/vpd_pgb1")).unwrap_or_default();
    match page.get(4..6) {
        Some(&[high, low]) => match u32::from(u16::from_be_bytes([high, low])) {
            1 => 0,
            rpm => rpm,
        },
        _ => 0,
    }
}

/// The name `/sys/class/block` gives the device a mount's source names
///
/// # Arguments
///
/// * `source` - The mount's source, such as `/dev/mapper/ubuntu--vg-x6--xfs`
fn device_name(source: &str) -> String {
    // a device mapper name resolves to its dm-N
    std::fs::canonicalize(source)
        .ok()
        .and_then(|path| path.file_name().map(|name| name.to_string_lossy().into_owned()))
        .unwrap_or_else(|| source.trim_start_matches("/dev/").to_string())
}

/// The whole disk under a block device
///
/// A device mapper device is followed through its first slave, and a partition to its parent.
///
/// # Arguments
///
/// * `name` - The device
fn disk_of(name: &str) -> String {
    let class = Path::new("/sys/class/block").join(name);
    // a dm device names what it is built on
    if let Ok(mut slaves) = std::fs::read_dir(class.join("slaves")) {
        if let Some(Ok(slave)) = slaves.next() {
            return disk_of(&slave.file_name().to_string_lossy());
        }
    }
    // a partition lives in its disk's directory
    if class.join("partition").exists() {
        if let Ok(real) = std::fs::canonicalize(&class) {
            if let Some(parent) = real.parent().and_then(Path::file_name) {
                return parent.to_string_lossy().into_owned();
            }
        }
    }
    name.to_string()
}

/// Every physical core's cpus, each list sorted, the cores in order of their first cpu
#[must_use]
pub fn physical_cores() -> Vec<Vec<usize>> {
    let mut cores: Vec<Vec<usize>> = Vec::new();
    // every online cpu's sibling list names its core
    let online = std::fs::read_to_string("/sys/devices/system/cpu/online").unwrap_or_default();
    for cpu in parse_list(online.trim()) {
        let siblings = std::fs::read_to_string(format!(
            "/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
        ))
        .unwrap_or_else(|_| cpu.to_string());
        let mut list = parse_list(siblings.trim());
        list.sort_unstable();
        if !cores.contains(&list) {
            cores.push(list);
        }
    }
    cores.sort_by_key(|list| list[0]);
    cores
}

/// Parse a kernel cpu list such as `0-3,8,10-11`
///
/// # Arguments
///
/// * `text` - The list
fn parse_list(text: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in text.split(',').filter(|part| !part.is_empty()) {
        match part.split_once('-') {
            Some((low, high)) => {
                let (low, high): (usize, usize) = (low.parse().unwrap_or(0), high.parse().unwrap_or(0));
                cpus.extend(low..=high);
            }
            None => cpus.extend(part.parse::<usize>().ok()),
        }
    }
    cpus
}

/// The cpu each slice runs on and the sibling its blocking thread runs on, in the order slices
/// are added
///
/// The first is the core `first` is on, then the cores after it, wrapping, with the core of cpu
/// 0, where the host's interrupts and housekeeping land, last of all.
///
/// # Arguments
///
/// * `first` - The cpu of the first slice
#[must_use]
pub fn slice_order(first: usize) -> Vec<(usize, Option<usize>)> {
    let cores = physical_cores();
    let start = cores.iter().position(|core| core.contains(&first)).unwrap_or(0);
    let mut order: Vec<Vec<usize>> = cores[start..].iter().chain(cores[..start].iter()).cloned().collect();
    // the housekeeping core goes last
    if let Some(zero) = order.iter().position(|core| core.contains(&0)) {
        let core = order.remove(zero);
        order.push(core);
    }
    order
        .into_iter()
        .map(|core| (core[0], core.get(1).copied()))
        .collect()
}

/// The sibling of a cpu on its physical core, if it has one
///
/// # Arguments
///
/// * `cpu` - The cpu
#[must_use]
pub fn sibling(cpu: usize) -> Option<usize> {
    physical_cores()
        .into_iter()
        .find(|core| core.contains(&cpu))
        .and_then(|core| core.into_iter().find(|&other| other != cpu))
}

/// What the probes found, which later measurements lean on
#[derive(Debug, Clone)]
pub struct Probes {
    /// How a directory is synced so that a rename in it is durable
    pub dir_sync: DirSync,
    /// Whether the filesystem clones a range, or why it refused
    pub clone: Result<(), String>,
    /// The probes' table, for the run's output
    pub table: String,
    /// The probes' figures, for a record a report can judge
    pub figures: Vec<(&'static str, f64)>,
}

/// Run every probe on an executor, in a directory of their own under the scratch directory
///
/// # Arguments
///
/// * `dir` - The probes' directory
/// * `devices` - The devices to count on
/// * `label` - The label for the table
pub async fn probe(dir: PathBuf, devices: Devices, label: String) -> Probes {
    // a clean directory every time
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("the probe directory is made");
    let mut table = Table::new(&["probe", "result", "detail"]);
    let payloads = io::Payloads::new(1 << 20);

    // direct I/O: a megabyte written and not synced shows on the device at once
    let file = io::open(&dir.join("direct"), true).await;
    let before = devices.snap();
    io::write_body(&file, &payloads, 1 << 20, 0).await;
    let delta = before.delta(&devices.snap());
    table.row(vec![
        "direct I/O reaches the device unsynced".into(),
        (delta.written >= 1 << 20).to_string(),
        format!("{} KiB written for a 1024 KiB write", delta.written / 1024),
    ]);
    file.close().await.expect("closed");

    // flushes: a hundred small overwrites, each synced
    let file = io::open(&dir.join("flush"), true).await;
    io::write_body(&file, &payloads, ALIGN, 0).await;
    file.fdatasync().await.expect("synced");
    let before = devices.snap();
    let mut syncs = Samples::default();
    for _ in 0..100 {
        io::write_body(&file, &payloads, ALIGN, 0).await;
        let start = Instant::now();
        file.fdatasync().await.expect("synced");
        syncs.push(start.elapsed());
    }
    let delta = before.delta(&devices.snap());
    let overwrite = syncs.summary();
    table.row(vec![
        "flushes for 100 overwrites, each synced".into(),
        format!("disk {} · fs device {}", delta.flushes, delta.fs_flushes),
        format!(
            "{} KiB a sync · sync p50 {} µs",
            fmt(delta.kib_per(100)),
            fmt(syncs.summary().p50)
        ),
    ]);

    // a sync with nothing to sync
    let before = devices.snap();
    let mut empty = Samples::default();
    for _ in 0..100 {
        let start = Instant::now();
        file.fdatasync().await.expect("synced");
        empty.push(start.elapsed());
    }
    let delta = before.delta(&devices.snap());
    let clean = empty.summary();
    table.row(vec![
        "fdatasync of a clean file".into(),
        format!("p50 {} µs, p99 {} µs", fmt(empty.summary().p50), fmt(empty.summary().p99)),
        format!("{} flushes and {} KiB for 100", delta.flushes, delta.written / 1024),
    ]);
    file.close().await.expect("closed");

    // a rename made durable by the directory's fdatasync, against its fsync
    let renames = dir.join("renames");
    std::fs::create_dir_all(&renames).expect("made");
    let directory = Rc::new(Directory::open(&renames).await.expect("opened"));
    let mut forms = Vec::new();
    let mut rename_p50 = 0.0;
    for form in [DirSync::Fdatasync, DirSync::Fsync] {
        let mut written = 0;
        let mut flushes = 0;
        let mut took = Samples::default();
        for index in 0..20 {
            // a synced file, then its rename alone under the counters
            let staged = renames.join(format!("{form:?}-{index}.staged"));
            let file = io::open(&staged, true).await;
            io::write_body(&file, &payloads, ALIGN, 0).await;
            file.fdatasync().await.expect("synced");
            io::sync_dir(&directory, form).await;
            let before = devices.snap();
            let start = Instant::now();
            file.rename(renames.join(format!("{form:?}-{index}"))).await.expect("renamed");
            io::sync_dir(&directory, form).await;
            took.push(start.elapsed());
            let delta = before.delta(&devices.snap());
            written += delta.written;
            flushes += delta.flushes;
            file.close().await.expect("closed");
        }
        forms.push((form, written, flushes));
        if form == DirSync::Fdatasync {
            rename_p50 = took.summary().p50;
        }
        table.row(vec![
            format!("rename, then the directory's {form:?}"),
            format!("{} KiB and {} flushes a rename", written / 1024 / 20, fmt(flushes as f64 / 20.0)),
            format!("p50 {} µs", fmt(took.summary().p50)),
        ]);
    }
    // the fdatasync is enough if it commits as much as the fsync does
    let dir_sync = if forms[0].1 * 2 >= forms[1].1 && forms[1].1 > 0 {
        DirSync::Fdatasync
    } else if forms[1].1 > 0 {
        DirSync::Fsync
    } else {
        DirSync::Fdatasync
    };
    table.row(vec![
        "directory sync the measurements use".into(),
        format!("{dir_sync:?}"),
        "the fdatasync unless it wrote less than half of what the fsync did".into(),
    ]);

    // a clone of one block, between two files open for reading and writing
    let source = io::open(&dir.join("clone-src"), true).await;
    io::write_body(&source, &payloads, 2 * ALIGN, 0).await;
    source.fdatasync().await.expect("synced");
    let target = io::open(&dir.join("clone-dst"), true).await;
    io::write_body(&target, &payloads, 2 * ALIGN, 0).await;
    target.fdatasync().await.expect("synced");
    let clone = io::clone(&source, ALIGN, ALIGN, &target, 0).await.map(|_| ());
    let extents = sys::fiemap(target.as_raw_fd());
    table.row(vec![
        "FICLONERANGE of one block".into(),
        match &clone {
            Ok(()) => "supported".into(),
            Err(error) => format!("refused: {error}"),
        },
        format!("FIEMAP on the target: {extents:?}"),
    ]);
    let clone = clone.map_err(|error| error.to_string());
    source.close().await.expect("closed");
    target.close().await.expect("closed");

    // what a hop to the blocking thread costs
    let mut hops = Samples::default();
    for _ in 0..1000 {
        let start = Instant::now();
        glommio::executor().spawn_blocking(|| ()).await;
        hops.push(start.elapsed());
    }
    table.row(vec![
        "an empty spawn_blocking".into(),
        format!("p50 {} µs, p99 {} µs", fmt(hops.summary().p50), fmt(hops.summary().p99)),
        "the hop every rename, unlink, mkdir and clone pays".into(),
    ]);

    // alignment
    let file: DmaFile = io::open(&dir.join("align"), true).await;
    table.row(vec![
        "alignment".into(),
        format!("{ALIGN} B"),
        format!("glommio's file alignment {} B", file.alignment()),
    ]);
    file.close().await.expect("closed");
    let _ = std::fs::remove_dir_all(&dir);
    glommio::timer::sleep(Duration::from_millis(10)).await;
    Probes {
        dir_sync,
        clone,
        table: table.render("Probes", &label),
        figures: vec![
            ("overwrite_sync_p50", overwrite.p50),
            ("overwrite_sync_p99", overwrite.p99),
            ("clean_sync_p50", clean.p50),
            ("rename_dir_sync_p50", rename_p50),
            ("hop_p50", hops.summary().p50),
        ],
    }
}
