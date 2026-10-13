//! What the device and the cpus did while a side ran
//!
//! The kernel counts every block device's requests in `/sys/class/block/<name>/stat`. A side
//! reads them before and after, for two devices: the filesystem's own (an LV's `dm-N`, or a
//! partition), which counts its bytes without the rest of the disk's traffic, and the whole
//! disk, which is where blk-mq counts a cache flush. Which of the two counts flushes is checked
//! by a probe, not assumed. The cpu side is the process's CPU time, every thread of it, which
//! includes the io_uring workers the kernel runs for a sync, an allocation or a cold open.

use std::time::Instant;

use super::sys;

/// The bytes in a sector as the block layer counts them, whatever the device's own size
const SECTOR: u64 = 512;

/// One device's counters at one moment
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Dev {
    /// Reads completed
    pub reads: u64,
    /// Sectors read
    pub read_sectors: u64,
    /// Writes completed
    pub writes: u64,
    /// Sectors written
    pub write_sectors: u64,
    /// Discards completed
    pub discards: u64,
    /// Sectors discarded
    pub discard_sectors: u64,
    /// Flushes completed
    pub flushes: u64,
    /// Milliseconds with a request in flight
    pub busy_ms: u64,
    /// Reads the block layer merged into another
    pub read_merges: u64,
    /// Writes the block layer merged into another
    pub write_merges: u64,
    /// Milliseconds requests spent queued and in flight, summed over every request
    pub queue_ms: u64,
}

/// Read a block device's counters
///
/// The fields are reads, reads merged, sectors read, time reading, writes, writes merged,
/// sectors written, time writing, in flight, time busy, weighted time, then four for discards
/// and two for flushes. A device that does not count one of them reads as zero.
///
/// # Arguments
///
/// * `name` - The device as `/sys/class/block` names it
#[must_use]
pub fn read_dev(name: &str) -> Dev {
    let text = std::fs::read_to_string(format!("/sys/class/block/{name}/stat")).unwrap_or_default();
    let fields: Vec<u64> = text
        .split_whitespace()
        .map(|field| field.parse().unwrap_or(0))
        .collect();
    let field = |index: usize| fields.get(index).copied().unwrap_or(0);
    Dev {
        reads: field(0),
        read_merges: field(1),
        read_sectors: field(2),
        writes: field(4),
        write_merges: field(5),
        write_sectors: field(6),
        busy_ms: field(9),
        queue_ms: field(10),
        discards: field(11),
        discard_sectors: field(13),
        flushes: field(15),
    }
}

/// The two devices a side is counted on
#[derive(Debug, Clone)]
pub struct Devices {
    /// The filesystem's own device
    pub fs: String,
    /// The whole disk under it
    pub disk: String,
}

/// The counters, the clock and the process's CPU at one moment
#[derive(Debug, Clone, Copy)]
pub struct Snap {
    /// The filesystem's device
    pub fs: Dev,
    /// The whole disk
    pub disk: Dev,
    /// When it was taken
    pub at: Instant,
    /// The process's CPU time so far, nanoseconds
    pub cpu_ns: u64,
}

impl Devices {
    /// Read both devices, the clock and the process's CPU now
    #[must_use]
    pub fn snap(&self) -> Snap {
        Snap {
            fs: read_dev(&self.fs),
            disk: read_dev(&self.disk),
            at: Instant::now(),
            cpu_ns: sys::process_cpu_ns(),
        }
    }
}

/// What happened between two snapshots
#[derive(Debug, Clone, Copy, Default)]
pub struct Delta {
    /// Seconds between them
    pub secs: f64,
    /// Bytes written to the filesystem's device
    pub written: u64,
    /// Bytes read from it
    pub read: u64,
    /// Bytes it discarded
    pub discarded: u64,
    /// Flushes the whole disk completed
    pub flushes: u64,
    /// Flushes the filesystem's device completed, which a device mapper may not count
    pub fs_flushes: u64,
    /// The process's CPU time, nanoseconds
    pub cpu_ns: u64,
    /// Milliseconds the whole disk had a request in flight
    pub busy_ms: u64,
    /// Requests the whole disk's block layer merged into another
    pub merges: u64,
}

impl Snap {
    /// What happened between this snapshot and a later one
    ///
    /// # Arguments
    ///
    /// * `later` - The later snapshot
    #[must_use]
    pub fn delta(&self, later: &Snap) -> Delta {
        Delta {
            secs: later.at.duration_since(self.at).as_secs_f64(),
            written: later.fs.write_sectors.saturating_sub(self.fs.write_sectors) * SECTOR,
            read: later.fs.read_sectors.saturating_sub(self.fs.read_sectors) * SECTOR,
            discarded: later.fs.discard_sectors.saturating_sub(self.fs.discard_sectors) * SECTOR,
            flushes: later.disk.flushes.saturating_sub(self.disk.flushes),
            fs_flushes: later.fs.flushes.saturating_sub(self.fs.flushes),
            cpu_ns: later.cpu_ns.saturating_sub(self.cpu_ns),
            busy_ms: later.disk.busy_ms.saturating_sub(self.disk.busy_ms),
            merges: (later.disk.read_merges + later.disk.write_merges)
                .saturating_sub(self.disk.read_merges + self.disk.write_merges),
        }
    }
}

impl Delta {
    /// Device KiB written for each operation
    ///
    /// # Arguments
    ///
    /// * `ops` - How many operations the delta covers
    #[must_use]
    pub fn kib_per(&self, ops: usize) -> f64 {
        self.written as f64 / 1024.0 / ops.max(1) as f64
    }

    /// Flushes of the whole disk for each operation
    ///
    /// # Arguments
    ///
    /// * `ops` - How many operations the delta covers
    #[must_use]
    pub fn flushes_per(&self, ops: usize) -> f64 {
        self.flushes as f64 / ops.max(1) as f64
    }

    /// The share of the time between the snapshots the whole disk had a request in flight
    #[must_use]
    pub fn busy(&self) -> f64 {
        if self.secs > 0.0 {
            self.busy_ms as f64 / 1e3 / self.secs
        } else {
            0.0
        }
    }

    /// The process's CPU microseconds for each operation
    ///
    /// # Arguments
    ///
    /// * `ops` - How many operations the delta covers
    #[must_use]
    pub fn cpu_us_per(&self, ops: usize) -> f64 {
        self.cpu_ns as f64 / 1e3 / ops.max(1) as f64
    }
}

/// Every cpu's busy and total jiffies, from `/proc/stat`
///
/// Busy is user, nice, system, irq and softirq; total adds idle, iowait and steal.
#[must_use]
pub fn cpu_jiffies() -> Vec<(u64, u64)> {
    let text = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let mut cpus = Vec::new();
    for line in text.lines() {
        // only the per-cpu lines, `cpuN ...`, in order
        let Some(rest) = line.strip_prefix("cpu") else { continue };
        if !rest.starts_with(|c: char| c.is_ascii_digit()) {
            continue;
        }
        let fields: Vec<u64> = rest
            .split_whitespace()
            .skip(1)
            .map(|field| field.parse().unwrap_or(0))
            .collect();
        let field = |index: usize| fields.get(index).copied().unwrap_or(0);
        let busy = field(0) + field(1) + field(2) + field(5) + field(6);
        let total = busy + field(3) + field(4) + field(7);
        cpus.push((busy, total));
    }
    cpus
}

/// How busy one cpu was between two reads of `/proc/stat`, from 0 to 1
///
/// # Arguments
///
/// * `before` - The earlier read
/// * `after` - The later read
/// * `cpu` - The cpu
#[must_use]
pub fn cpu_busy(before: &[(u64, u64)], after: &[(u64, u64)], cpu: usize) -> f64 {
    let (Some(start), Some(end)) = (before.get(cpu), after.get(cpu)) else {
        return 0.0;
    };
    let total = end.1.saturating_sub(start.1);
    if total == 0 {
        0.0
    } else {
        end.0.saturating_sub(start.0) as f64 / total as f64
    }
}
