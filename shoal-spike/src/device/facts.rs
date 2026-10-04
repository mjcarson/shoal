//! What every table is labelled with, and the probes that say what the counters mean
//!
//! A figure from X6 is worth nothing without the host, cpu, governor, device, firmware,
//! filesystem and mount options it came from, so every table carries them. They are read from
//! the kernel, never typed in. The probes come first in every run: they check that direct I/O
//! is direct, find which device counts a flush, find whether a directory's `fdatasync` makes a
//! rename durable, and find whether the filesystem can clone. The measurements after them lean
//! on those answers.

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
    /// The filesystem's block size
    pub block: u64,
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
            model: attr("device/model"),
            firmware: attr("device/firmware_rev"),
            write_cache: attr("queue/write_cache"),
            logical_block: attr("queue/logical_block_size").parse().unwrap_or(512),
            block: vfs.f_bsize,
        }
    }

    /// The line every table carries
    #[must_use]
    pub fn label(&self) -> String {
        format!(
            "{} · {} · governor {} · kernel {} · {} {} (fw {}, cache {}, {} B blocks) · {} at {} on {} = {} ({}) · fs block {} B",
            self.host,
            self.cpu,
            self.governor,
            self.kernel,
            self.devices.disk,
            self.model,
            self.firmware,
            self.write_cache,
            self.logical_block,
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
    }
}
