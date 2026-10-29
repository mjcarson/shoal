//! What a node reports about its capacity: the size and free bytes of every storage root, and
//! what a test says instead
//!
//! The control thread measures every root its configuration names once per report tick and
//! sends the figures to the leader beside the bytes each of its groups holds
//! ([F82](../../../../docs/src/features/root-capacity.md)); the receiving shard measures the
//! roots a stream would write before it accepts a snapshot stream, so a stale report is caught
//! where the bytes would land ([F46](../../../../docs/src/features/capacity-rebalancing.md)).
//! The append reserve reads the WAL's root alone. A test overrides the free bytes process-wide
//! rather than filling a disk, or arms a full disk under one root (F70).

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::shared::capacity::{RootCapacity, RootUse};

/// The free bytes a test says the storage has, or zero for what the filesystem says
static OVERRIDE: AtomicU64 = AtomicU64::new(0);

/// Override the free bytes every reader sees, or lift the override with zero
///
/// # Arguments
///
/// * `bytes` - The free bytes to report, or zero for the filesystem's figure
pub fn set_override(bytes: u64) {
    OVERRIDE.store(bytes, Ordering::Relaxed);
}

/// The size and the free bytes of the filesystem a path is on, as `statvfs` says
///
/// None when the filesystem cannot be asked.
///
/// # Arguments
///
/// * `path` - A path on the filesystem, which has to exist to be asked
fn statvfs(path: &Path) -> Option<(u64, u64)> {
    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).ok()?;
    let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: the path is a valid C string and the struct is a valid out pointer
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &raw mut stats) };
    if rc != 0 {
        return None;
    }
    // the fields are unsigned on every target this runs on; the widening is explicit
    #[allow(clippy::useless_conversion, clippy::unnecessary_cast)]
    let frag = stats.f_frsize as u64;
    #[allow(clippy::useless_conversion, clippy::unnecessary_cast)]
    let blocks = stats.f_blocks as u64;
    #[allow(clippy::useless_conversion, clippy::unnecessary_cast)]
    let avail = stats.f_bavail as u64;
    // the size is every fragment; the free bytes those an unprivileged writer may use
    Some((frag.saturating_mul(blocks), frag.saturating_mul(avail)))
}

/// The free bytes at a path, as the filesystem or the override says
///
/// None when the filesystem cannot be asked, which a planner takes as "not reported".
///
/// # Arguments
///
/// * `path` - A path on the storage
#[must_use]
pub fn free_bytes(path: &Path) -> Option<u64> {
    // a test's word first
    let forced = OVERRIDE.load(Ordering::Relaxed);
    if forced > 0 {
        return Some(forced);
    }
    // then a full disk a test armed under the path, which fills as it is written (F70)
    if let Some(left) = crate::server::faults::free_bytes_under(path) {
        return Some(left);
    }
    // then statvfs on the path
    statvfs(path).map(|(_, avail)| avail)
}

/// The size of the filesystem a path is on and the free bytes at it, as every reader sees them
///
/// A device is measured as a root is: its size from `statvfs`, its free bytes through the override
/// and a full disk a test armed ([F85](../../../../docs/src/features/devices-and-pools.md)).
///
/// # Arguments
///
/// * `path` - A path on the filesystem
#[must_use]
pub fn size_and_free(path: &Path) -> Option<(u64, u64)> {
    let (size, _) = statvfs(path)?;
    Some((size, free_bytes(path).unwrap_or(0)))
}

/// The device a path's filesystem is on, as `major:minor`
///
/// # Arguments
///
/// * `path` - A path that exists
fn device(path: &Path) -> Option<String> {
    let dev = std::fs::metadata(path).ok()?.dev();
    Some(format!("{}:{}", libc::major(dev), libc::minor(dev)))
}

/// Measure every root, in the order given
///
/// A root that cannot be read is reported with nothing free, so the planner places nothing on
/// it and the receiver takes nothing onto it, as a node that could not read its one root did
/// before every root was reported. The override and a full disk a test armed change the free
/// bytes alone; the size is always the filesystem's.
///
/// # Arguments
///
/// * `roots` - Each root with what the node writes under it
#[must_use]
pub fn measure(roots: &[(PathBuf, Vec<RootUse>)]) -> Vec<RootCapacity> {
    roots
        .iter()
        .map(|(path, uses)| {
            // the filesystem's figures, then the free bytes as every other reader sees them
            let (size_bytes, _) = statvfs(path).unwrap_or((0, 0));
            RootCapacity {
                path: path.to_string_lossy().into_owned(),
                device: device(path).unwrap_or_default(),
                size_bytes,
                free_bytes: free_bytes(path).unwrap_or(0),
                uses: uses.clone(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The override is process-wide, so the tests that set it run one at a time
    static OVERRIDDEN: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// The filesystem answers for a path that exists, an override wins, and lifting it restores
    #[test]
    fn free_bytes_reads_the_filesystem_and_the_override() {
        let _held = OVERRIDDEN
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = std::env::temp_dir();
        let real = free_bytes(&dir).expect("the temp dir has a filesystem");
        assert!(real > 0);
        set_override(1234);
        assert_eq!(free_bytes(&dir), Some(1234));
        set_override(0);
        assert!(free_bytes(&dir).is_some());
        assert_eq!(free_bytes(Path::new("/does/not/exist/anywhere")), None);
    }

    /// Every root is measured on its own: its size and device from its filesystem, its free
    /// bytes as a full disk armed under it says, and one that cannot be read has nothing free
    #[test]
    fn every_root_is_measured_on_its_own() {
        let _held = OVERRIDDEN
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = tempfile::tempdir().expect("a temp dir");
        let latency = base.path().join("latency");
        let throughput = base.path().join("throughput");
        std::fs::create_dir_all(&latency).unwrap();
        std::fs::create_dir_all(&throughput).unwrap();
        let missing = base.path().join("missing");
        let roots = vec![
            (latency.clone(), vec![RootUse::Primary, RootUse::Latency]),
            (throughput.clone(), vec![RootUse::Throughput]),
            (missing.clone(), vec![RootUse::Throughput]),
        ];
        // two directories on one filesystem report its figures and its device
        let measured = measure(&roots);
        assert_eq!(measured.len(), 3);
        assert_eq!(measured[0].path, latency.to_string_lossy());
        assert_eq!(measured[0].uses, vec![RootUse::Primary, RootUse::Latency]);
        assert!(measured[0].size_bytes > 0 && measured[0].free_bytes > 0);
        assert!(measured[0].free_bytes <= measured[0].size_bytes);
        assert!(!measured[0].device.is_empty());
        assert_eq!(measured[0].device, measured[1].device);
        assert_eq!(measured[0].size_bytes, measured[1].size_bytes);
        // a root that cannot be read has nothing free and no device
        assert_eq!(measured[2].free_bytes, 0);
        assert_eq!(measured[2].size_bytes, 0);
        assert!(measured[2].device.is_empty());
        // a full disk armed under one root is that root's figure and no other's
        crate::server::faults::arm(
            &throughput,
            crate::server::faults::Fault::Full { budget: 4096 },
        );
        let measured = measure(&roots);
        crate::server::faults::clear(&throughput);
        assert_eq!(measured[1].free_bytes, 4096);
        assert!(measured[0].free_bytes > 4096);
        assert_eq!(measured[0].size_bytes, measured[1].size_bytes);
    }
}
