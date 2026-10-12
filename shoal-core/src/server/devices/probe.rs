//! What a device's directory is on: its filesystem, its disk and its size
//!
//! The device store depends on direct I/O reaching the device and on a sync of one file flushing
//! it, so a node reads what each device is on before it serves it
//! ([S4](../../../../docs/src/object-storage/pools-and-devices.md#what-a-node-checks-before-it-serves-a-pool)):
//!
//! - **the filesystem**, from `statfs`: tmpfs is refused, since glommio turns direct I/O off there
//!   in silence, and so is btrfs, where every overwrite is a new allocation and an apply would
//!   need space ([Q22](../../../../docs/src/object-storage/contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04));
//!   XFS is preferred and ext4 accepted;
//! - **the disk under it**, from sysfs through a device mapper's slaves and a partition's parent:
//!   whether the kernel calls it rotational, and what its write cache does.
//!
//! # Invariants
//!
//! **Detection warns and never overrules.** A class is what an operator wrote; this only says what
//! the kernel says, for a warning ([S4](../../../../docs/src/object-storage/pools-and-devices.md#pools-and-bindings-are-policy)).
//!
//! **A test may say what a directory is on, and says so loudly.** The repository's test
//! directories are on btrfs and `/tmp` is tmpfs, so a test that runs the device store names the
//! facts it stands in for through [`set_override`] or `SHOAL_TEST_DEVICE_FACTS`, and every claim
//! under one logs a warning that it did.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};

use serde::{Deserialize, Serialize};

/// `statfs`'s magic for XFS
const XFS_MAGIC: i64 = 0x5846_5342;
/// `statfs`'s magic for ext2, ext3 and ext4, which share it
const EXT4_MAGIC: i64 = 0xef53;
/// `statfs`'s magic for btrfs
const BTRFS_MAGIC: i64 = 0x9123_683e;
/// `statfs`'s magic for tmpfs, which devtmpfs shares
const TMPFS_MAGIC: i64 = 0x0102_1994;
/// `statfs`'s magic for overlayfs
const OVERLAY_MAGIC: i64 = 0x794c_7630;
/// `statfs`'s magic for ZFS
const ZFS_MAGIC: i64 = 0x2fc1_2fc1;
/// `statfs`'s magic for F2FS
const F2FS_MAGIC: i64 = 0xf2f5_2010;

/// The environment variable a test child names the facts it stands in for through, as JSON
pub const OVERRIDE_ENV: &str = "SHOAL_TEST_DEVICE_FACTS";

/// What a device's directory is on
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Facts {
    /// The filesystem, as the kernel names its kind: `xfs`, `ext4`, `btrfs`, `tmpfs`, or the
    /// magic number in hex for one this does not name
    pub filesystem: String,
    /// The filesystem's device, `major:minor`
    pub device: String,
    /// Whether the kernel calls the disk under it rotational
    pub rotational: bool,
    /// What the disk's write cache does, `write back` or `write through`, where sysfs says
    pub write_cache: Option<String>,
    /// The bytes the filesystem holds
    pub size_bytes: u64,
    /// The bytes free on it
    pub free_bytes: u64,
}

impl Facts {
    /// Whether the device store may not run on this filesystem at all, and why
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        match self.filesystem.as_str() {
            "tmpfs" => Some(
                "it is on tmpfs, where direct I/O is off in silence, which hides exactly what the \
                 device store depends on"
                    .to_string(),
            ),
            "btrfs" => Some(
                "it is on btrfs, where every overwrite is a new allocation: an apply would need \
                 space, which the device store promises it never does (X6)"
                    .to_string(),
            ),
            _ => None,
        }
    }

    /// Whether the filesystem is one the device store was measured on
    #[must_use]
    pub fn measured(&self) -> bool {
        matches!(self.filesystem.as_str(), "xfs" | "ext4")
    }
}

/// Facts a test stands in for, for every device or for named paths
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProbeOverride {
    /// The filesystem every device is reported on
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filesystem: Option<String>,
    /// Devices the kernel is reported to call rotational
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rotational: Vec<PathBuf>,
    /// Whether two devices may stand on one filesystem, as a test's directories do
    #[serde(default)]
    pub shared_filesystem: bool,
    /// What every device's write cache is reported to do
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_cache: Option<String>,
}

/// The override an in-process test set
static OVERRIDE: Mutex<Option<ProbeOverride>> = Mutex::new(None);

/// The override a child process was started with, read once
static ENV_OVERRIDE: OnceLock<Option<ProbeOverride>> = OnceLock::new();

/// Stand facts in for every device this process claims, until cleared with `None`
///
/// For tests: the repository's test directories are on btrfs, which the store refuses.
///
/// # Arguments
///
/// * `facts` - The facts to stand in, or none to read the kernel's again
pub fn set_override(facts: Option<ProbeOverride>) {
    *OVERRIDE.lock().unwrap_or_else(PoisonError::into_inner) = facts;
}

/// The override in force, an in-process one before one from the environment
#[must_use]
pub fn current_override() -> Option<ProbeOverride> {
    if let Some(set) = OVERRIDE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
    {
        return Some(set);
    }
    ENV_OVERRIDE
        .get_or_init(|| {
            std::env::var(OVERRIDE_ENV)
                .ok()
                .and_then(|text| serde_json::from_str(&text).ok())
        })
        .clone()
}

/// The name of a filesystem by its `statfs` magic
///
/// # Arguments
///
/// * `magic` - The `f_type` `statfs` gave
fn filesystem_name(magic: i64) -> String {
    match magic {
        XFS_MAGIC => "xfs".to_string(),
        EXT4_MAGIC => "ext4".to_string(),
        BTRFS_MAGIC => "btrfs".to_string(),
        TMPFS_MAGIC => "tmpfs".to_string(),
        OVERLAY_MAGIC => "overlay".to_string(),
        ZFS_MAGIC => "zfs".to_string(),
        F2FS_MAGIC => "f2fs".to_string(),
        other => format!("{other:#x}"),
    }
}

/// The whole disk under a block device, by its name in sysfs
///
/// A device mapper device is followed through its first slave, and a partition to its parent.
///
/// # Arguments
///
/// * `name` - The block device's name, such as `dm-0` or `nvme1n1p1`
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

/// One attribute of a disk's queue, trimmed, if sysfs has it
///
/// # Arguments
///
/// * `disk` - The disk's name
/// * `attribute` - The attribute under `queue/`
fn queue_attribute(disk: &str, attribute: &str) -> Option<String> {
    std::fs::read_to_string(format!("/sys/class/block/{disk}/queue/{attribute}"))
        .ok()
        .map(|text| text.trim().to_string())
}

/// What a directory is on, as the kernel says, with a test's override laid over it
///
/// # Arguments
///
/// * `path` - The device's directory
///
/// # Errors
///
/// When the directory cannot be stat'ed, which a missing or failed device gives.
pub fn probe(path: &Path) -> std::io::Result<Facts> {
    // the filesystem's device, from the directory itself
    let metadata = std::fs::metadata(path)?;
    let dev = metadata.dev();
    let (major, minor) = (libc::major(dev), libc::minor(dev));
    // its kind and its size, from statfs
    let cpath = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| std::io::Error::other("a device path holds a nul byte"))?;
    // SAFETY: a zeroed statfs is a valid out parameter, filled by the call
    let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: a nul terminated path and an out parameter of the right type
    if unsafe { libc::statfs(cpath.as_ptr(), &mut fs) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    #[allow(clippy::useless_conversion, clippy::unnecessary_cast)]
    let magic = i64::from(fs.f_type as i64);
    #[allow(clippy::cast_sign_loss, clippy::useless_conversion)]
    let block = u64::try_from(fs.f_bsize).unwrap_or(0);
    let size_bytes = (fs.f_blocks as u64).saturating_mul(block);
    let free_bytes = (fs.f_bavail as u64).saturating_mul(block);
    // the disk under it, through sysfs, where it has one: a btrfs device is anonymous
    let disk = std::fs::canonicalize(format!("/sys/dev/block/{major}:{minor}"))
        .ok()
        .and_then(|real| real.file_name().map(|name| name.to_string_lossy().into_owned()))
        .map(|name| disk_of(&name));
    let rotational = disk
        .as_deref()
        .and_then(|disk| queue_attribute(disk, "rotational"))
        .is_some_and(|value| value == "1");
    let write_cache = disk
        .as_deref()
        .and_then(|disk| queue_attribute(disk, "write_cache"));
    let mut facts = Facts {
        filesystem: filesystem_name(magic),
        device: format!("{major}:{minor}"),
        rotational,
        write_cache,
        size_bytes,
        free_bytes,
    };
    // a test's facts over the kernel's, and a warning that they were
    if let Some(over) = current_override() {
        if let Some(filesystem) = &over.filesystem {
            facts.filesystem.clone_from(filesystem);
        }
        if over.rotational.iter().any(|named| named == path) {
            facts.rotational = true;
        }
        if over.write_cache.is_some() {
            facts.write_cache.clone_from(&over.write_cache);
        }
        tracing::event!(
            tracing::Level::WARN,
            msg = "a test's device facts stand in for the kernel's",
            path = %path.display(),
            facts = ?facts,
        );
    }
    Ok(facts)
}

#[cfg(test)]
mod tests {
    use super::{filesystem_name, probe, Facts};

    /// The filesystems the store names are named, and the two it refuses are refused
    #[test]
    fn the_store_refuses_tmpfs_and_btrfs_by_name() {
        assert_eq!(filesystem_name(0x5846_5342), "xfs");
        assert_eq!(filesystem_name(0xef53), "ext4");
        let on = |filesystem: &str| Facts {
            filesystem: filesystem.to_string(),
            ..Facts::default()
        };
        assert!(on("tmpfs").refusal().is_some_and(|why| why.contains("tmpfs")));
        assert!(on("btrfs").refusal().is_some_and(|why| why.contains("btrfs")));
        assert!(on("xfs").refusal().is_none() && on("xfs").measured());
        assert!(on("ext4").refusal().is_none() && on("ext4").measured());
        assert!(on("zfs").refusal().is_none() && !on("zfs").measured());
    }

    /// A real directory is probed: some filesystem, a device and a size
    #[test]
    fn a_directory_is_probed() {
        let dir = tempfile::tempdir().expect("a directory");
        let facts = probe(dir.path()).expect("probed");
        assert!(!facts.filesystem.is_empty());
        assert!(facts.device.contains(':'));
        assert!(facts.size_bytes > 0 && facts.free_bytes <= facts.size_bytes);
    }
}
