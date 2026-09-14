//! What a node reports about its capacity: free bytes on its storage, and what a test says instead
//!
//! The control thread reads the free bytes of the latency sensitive storage path once per
//! report tick and sends them to the leader beside the bytes each of its groups holds; the
//! receiving shard reads the same figure before it accepts a snapshot stream, so a stale
//! report is caught where the bytes would land
//! ([F46](../../../../docs/src/features/capacity-rebalancing.md)). A test overrides the
//! figure process-wide rather than filling a disk.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

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
    // then statvfs on the path, which has to exist to be asked
    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).ok()?;
    let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: the path is a valid C string and the struct is a valid out pointer
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &raw mut stats) };
    if rc != 0 {
        return None;
    }
    // the bytes an unprivileged writer may use: available fragments times the fragment size
    // the fields are unsigned on every target this runs on; the widening is explicit
    #[allow(clippy::useless_conversion, clippy::unnecessary_cast)]
    let frag = stats.f_frsize as u64;
    #[allow(clippy::useless_conversion, clippy::unnecessary_cast)]
    let avail = stats.f_bavail as u64;
    Some(frag.saturating_mul(avail))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The filesystem answers for a path that exists, an override wins, and lifting it restores
    #[test]
    fn free_bytes_reads_the_filesystem_and_the_override() {
        let dir = std::env::temp_dir();
        let real = free_bytes(&dir).expect("the temp dir has a filesystem");
        assert!(real > 0);
        set_override(1234);
        assert_eq!(free_bytes(&dir), Some(1234));
        set_override(0);
        assert!(free_bytes(&dir).is_some());
        assert_eq!(free_bytes(Path::new("/does/not/exist/anywhere")), None);
    }
}
