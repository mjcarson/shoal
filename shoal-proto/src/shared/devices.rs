//! Devices and their slices: where a pool's stripe chunks are kept
//!
//! A **device** is the physical thing object bytes are stored on, today a disk with a filesystem
//! mounted at a path on one node; a **slice** is the part of it one executor owns, a directory
//! under that path ([S4](../../../docs/src/object-storage/pools-and-devices.md#a-device-has-slices)).
//! A device is the unit of failure and a slice the unit of work: two slices of one device fail
//! together, so they are never two failure domains.
//!
//! ```yaml
//! storage:
//!   devices:
//!     - {path: /mnt/nvme0/shoal-objects, class: ssd}
//!     - {path: /mnt/nvme1/shoal-objects, class: ssd, slices: 2}
//! ```
//!
//! The types here are what a node's configuration, its status report, the control group's pool
//! map and `shoaladm`'s inventory all hold, so none of them can spell a device another way.
//!
//! # Invariants
//!
//! **Devices are a node's own facts, never policy.** A node reads its `storage.devices` for
//! itself, reports what it claimed, and the control group commits what it reported; no node reads
//! another's file, and nothing here reaches the schema fingerprint
//! ([F85](../../../docs/src/features/devices-and-pools.md)).
//!
//! **A class is what an operator wrote.** Detection warns about a class that disagrees with the
//! kernel and never overrules it.
//!
//! **A device id and a slice id are minted once and never reused**, each in a marker at its
//! directory. A device's seat is not its id: it is the pool map's, and passes to a replacement.
//!
//! **Unknown keys are refused**, so a key from a build that knows more is never read as a device
//! without it.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::shared::pools::ByteSize;

/// The most slices a device can have: more cores than a node has would drive nothing more
pub const MAX_SLICES: u16 = 16;

/// The journal a slice keeps when its device names none: four segments of 64 MiB
pub const DEFAULT_JOURNAL_BYTES: u64 = 256 << 20;

/// The fewest bytes a slice's journal can have: four segments of 1 MiB
pub const MIN_JOURNAL_BYTES: u64 = 4 << 20;

/// The segments a slice's journal is cut into
pub const JOURNAL_SEGMENTS: u64 = 4;

/// The files written ahead a slice keeps for each chunk length it serves, unless its device says
pub const DEFAULT_FREE_FILES: u32 = 8;

/// The most files written ahead a slice keeps for one chunk length
pub const MAX_FREE_FILES: u32 = 1024;

/// The free bytes a device keeps when nothing names its reserve: 1 GiB, as a move's reserve is
pub const DEFAULT_DEVICE_RESERVE: u64 = 1 << 30;

/// A device's identity, minted the first time its directory is claimed
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct DeviceId(pub uuid::Uuid);

impl DeviceId {
    /// A new device's id, ordered by when it was minted
    #[must_use]
    pub fn mint() -> Self {
        DeviceId(uuid::Uuid::now_v7())
    }
}

impl std::fmt::Display for DeviceId {
    /// Write the id as its uuid
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A slice's identity, minted when the slice is made and kept in its marker
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct SliceId(pub uuid::Uuid);

impl SliceId {
    /// A new slice's id, ordered by when it was minted
    #[must_use]
    pub fn mint() -> Self {
        SliceId(uuid::Uuid::now_v7())
    }
}

impl std::fmt::Display for SliceId {
    /// Write the id as its uuid
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// How an apply's write in place is made durable
///
/// `shared` is one flush of the device covering every apply that completed before it began, X8's
/// supplement, which made small writes 1.70 times as fast on the 970 EVO; it leans on a sync of one
/// file flushing the device for direct writes into every file written ahead, which XFS and ext4 do
/// ([F85](../../../docs/src/features/devices-and-pools.md#the-shared-flush)). `each` syncs every
/// chunk an apply wrote, as S6's table first had it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApplySync {
    /// One flush of the device for every apply that completed before it began
    Shared,
    /// A sync of each chunk an apply wrote
    Each,
}

impl std::fmt::Display for ApplySync {
    /// Write the setting as a configuration spells it
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplySync::Shared => write!(f, "shared"),
            ApplySync::Each => write!(f, "each"),
        }
    }
}

/// A device as a node's configuration names it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceSpec {
    /// Where its filesystem is mounted; it must exist, and is never created
    pub path: PathBuf,
    /// The class its pools select it by: `ssd`, `hdd` or any other label
    pub class: String,
    /// How many slices it is cut into, each owned by one executor; one unless set
    #[serde(default = "one_slice", skip_serializing_if = "is_one_slice")]
    pub slices: u16,
    /// The share of a pool it takes, its size unless set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<ByteSize>,
    /// The free bytes it keeps, which nothing written ahead may take; 1 GiB, or a cluster's
    /// `migration.disk_reserve`, unless set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserve: Option<ByteSize>,
    /// How an apply is made durable; `shared` on XFS and ext4 unless set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply_sync: Option<ApplySync>,
    /// The bytes of each slice's journal, written ahead in four segments
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal: Option<ByteSize>,
    /// The files written ahead each slice keeps for each chunk length it serves
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_files: Option<u32>,
}

/// A device's slices when its configuration names none
fn one_slice() -> u16 {
    1
}

/// Whether a device's slices are the default, so its configuration need not say so
///
/// # Arguments
///
/// * `slices` - The slices it is cut into
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_one_slice(slices: &u16) -> bool {
    *slices == 1
}

impl DeviceSpec {
    /// A device of one slice at a path, carrying a class
    ///
    /// # Arguments
    ///
    /// * `path` - Where its filesystem is mounted
    /// * `class` - The class its pools select it by
    #[must_use]
    pub fn new<P: Into<PathBuf>>(path: P, class: &str) -> Self {
        DeviceSpec {
            path: path.into(),
            class: class.to_string(),
            slices: 1,
            weight: None,
            reserve: None,
            apply_sync: None,
            journal: None,
            free_files: None,
        }
    }

    /// Cut it into this many slices
    ///
    /// # Arguments
    ///
    /// * `slices` - How many slices
    #[must_use]
    pub fn slices(mut self, slices: u16) -> Self {
        self.slices = slices;
        self
    }

    /// Keep this many free bytes on it
    ///
    /// # Arguments
    ///
    /// * `bytes` - The reserve
    #[must_use]
    pub fn reserve(mut self, bytes: u64) -> Self {
        self.reserve = Some(ByteSize(bytes));
        self
    }

    /// Make its applies durable this way
    ///
    /// # Arguments
    ///
    /// * `sync` - How an apply is made durable
    #[must_use]
    pub fn apply_sync(mut self, sync: ApplySync) -> Self {
        self.apply_sync = Some(sync);
        self
    }

    /// Give each slice a journal of this many bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - The journal's bytes
    #[must_use]
    pub fn journal(mut self, bytes: u64) -> Self {
        self.journal = Some(ByteSize(bytes));
        self
    }

    /// Keep this many files written ahead for each chunk length
    ///
    /// # Arguments
    ///
    /// * `files` - How many files
    #[must_use]
    pub fn free_files(mut self, files: u32) -> Self {
        self.free_files = Some(files);
        self
    }

    /// The bytes of each slice's journal
    #[must_use]
    pub fn journal_bytes(&self) -> u64 {
        self.journal.map_or(DEFAULT_JOURNAL_BYTES, ByteSize::get)
    }

    /// The bytes of each of a slice's journal segments, a whole number of 4 KiB blocks
    #[must_use]
    pub fn segment_bytes(&self) -> u64 {
        // a quarter of the journal, rounded down to a block
        (self.journal_bytes() / JOURNAL_SEGMENTS) & !4095
    }

    /// The files written ahead a slice keeps for each chunk length
    #[must_use]
    pub fn free_files_kept(&self) -> u32 {
        self.free_files.unwrap_or(DEFAULT_FREE_FILES)
    }

    /// Whether this device's facts can be served, and why not
    ///
    /// # Errors
    ///
    /// The first fact no device can have, naming the device.
    pub fn check(&self) -> Result<(), String> {
        // a refusal names the device and what about it cannot be
        let name = self.path.display();
        if !self.path.is_absolute() {
            return Err(format!("device {name}: its path is not absolute"));
        }
        if self.class.trim().is_empty() {
            return Err(format!(
                "device {name}: its class is empty; name the class its pools select it by"
            ));
        }
        if self.slices == 0 || self.slices > MAX_SLICES {
            return Err(format!(
                "device {name}: it is cut into {} slices, and a device has 1 to {MAX_SLICES}",
                self.slices
            ));
        }
        if self.weight.is_some_and(|weight| weight.get() == 0) {
            return Err(format!("device {name}: its weight is zero, so no pool would draw it"));
        }
        if self.journal_bytes() < MIN_JOURNAL_BYTES {
            return Err(format!(
                "device {name}: its journal of {} is under the {} a slice's four segments need",
                ByteSize(self.journal_bytes()),
                ByteSize(MIN_JOURNAL_BYTES)
            ));
        }
        if self.free_files_kept() == 0 || self.free_files_kept() > MAX_FREE_FILES {
            return Err(format!(
                "device {name}: it keeps {} files written ahead, and a slice keeps 1 to {MAX_FREE_FILES}",
                self.free_files_kept()
            ));
        }
        Ok(())
    }
}

/// Whether a device is serving, as its node reports it
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum DeviceHealth {
    /// Its slices are claimed and answering
    #[default]
    Serving,
    /// It returned an error, or was missing or emptied at start; every slice on it with it
    Failed {
        /// What failed, for an operator
        reason: String,
    },
}

impl DeviceHealth {
    /// Whether it has failed
    #[must_use]
    pub fn is_failed(&self) -> bool {
        matches!(self, DeviceHealth::Failed { .. })
    }
}

/// One slice of a device, as its node reports it
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SliceReport {
    /// The slice's id
    pub id: SliceId,
    /// Its place on the device, `slice-<index>`
    pub index: u16,
    /// The stripe chunks it holds
    #[serde(default)]
    pub chunks: u64,
    /// The bytes staged on it and not yet applied or discarded
    #[serde(default)]
    pub staged_bytes: u64,
}

/// A device as its node reports it, once a tick
///
/// What changes every second - the free bytes, a slice's figures - is kept by the control leader in
/// memory and never committed; what the pool map is made of is [`DeviceFacts`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DeviceReport {
    /// The device's id
    pub id: DeviceId,
    /// Where it is mounted on its node
    pub path: String,
    /// The class its operator wrote
    pub class: String,
    /// Its slices, by index
    #[serde(default)]
    pub slices: Vec<SliceReport>,
    /// The bytes its filesystem holds
    pub size_bytes: u64,
    /// The bytes free on it
    pub free_bytes: u64,
    /// The free bytes it keeps
    #[serde(default)]
    pub reserve_bytes: u64,
    /// The filesystem's device, `major:minor`
    #[serde(default)]
    pub device: String,
    /// The filesystem it is on, as the kernel names it
    #[serde(default)]
    pub filesystem: String,
    /// Whether the kernel calls the disk under it rotational
    #[serde(default)]
    pub rotational: bool,
    /// What an operator is warned of about it: a class the kernel disagrees with, a filesystem
    /// the store was not measured on
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Whether it is serving
    #[serde(default)]
    pub health: DeviceHealth,
}

impl DeviceReport {
    /// Whether it is short of space: its free bytes at or under its reserve
    #[must_use]
    pub fn short_of_space(&self) -> bool {
        self.free_bytes <= self.reserve_bytes
    }
}

/// What the pool map is made of: a device's identity as its node claimed it
///
/// A node proposes these when it starts, and the control group commits them into the pool map
/// ([`crate::shared::poolmap::PoolMap::observe`]). The size is the one first claimed: a figure
/// that moved with every write would move every placement with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceFacts {
    /// The device's id
    pub id: DeviceId,
    /// The class its operator wrote
    pub class: String,
    /// Its slices' ids, by index
    pub slices: Vec<SliceId>,
    /// The bytes its filesystem held when it was claimed
    pub size_bytes: u64,
    /// The weight its operator gave it, if any
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<u64>,
    /// Whether its disk's sync flushes a volatile write cache, which decides whether a small write
    /// rides in its commit on a pool of such devices (X8, [F86](../../../docs/src/features/replicated-pools.md))
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub write_back: bool,
}

impl DeviceFacts {
    /// The share of a pool it takes: its operator's weight, or its size
    #[must_use]
    pub fn capacity_weight(&self) -> u64 {
        self.weight.unwrap_or(self.size_bytes).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::{ApplySync, DeviceSpec, DEFAULT_JOURNAL_BYTES};

    /// A device reads as the documented shape, with every default, and refuses an unknown key
    #[test]
    fn a_device_reads_as_documented() {
        let written = "path: /mnt/nvme0/shoal-objects\nclass: ssd\n";
        let device: DeviceSpec = serde_yaml::from_str(written).expect("a device reads");
        assert_eq!(device, DeviceSpec::new("/mnt/nvme0/shoal-objects", "ssd"));
        assert_eq!(device.journal_bytes(), DEFAULT_JOURNAL_BYTES);
        assert_eq!(device.segment_bytes(), DEFAULT_JOURNAL_BYTES / 4);
        device.check().expect("a device of one slice is served");
        // every field written
        let full = "path: /d\nclass: hdd\nslices: 2\nweight: 4TiB\nreserve: 2GiB\napply_sync: each\njournal: 64MiB\nfree_files: 4\n";
        let device: DeviceSpec = serde_yaml::from_str(full).expect("every field reads");
        assert_eq!(device.slices, 2);
        assert_eq!(device.apply_sync, Some(ApplySync::Each));
        assert_eq!(device.weight.map(|weight| weight.get()), Some(4 << 40));
        // a key this build does not know is refused, not ignored
        let unknown = "path: /d\nclass: ssd\njournals: 2\n";
        assert!(serde_yaml::from_str::<DeviceSpec>(unknown).is_err());
    }

    /// A device's facts that no device can have are refused by name
    #[test]
    fn a_device_that_cannot_be_is_refused() {
        let refused = [
            DeviceSpec::new("relative/path", "ssd"),
            DeviceSpec::new("/d", " "),
            DeviceSpec::new("/d", "ssd").slices(0),
            DeviceSpec::new("/d", "ssd").slices(17),
            DeviceSpec::new("/d", "ssd").journal(1 << 20),
            DeviceSpec::new("/d", "ssd").free_files(0),
        ];
        for device in refused {
            let refusal = device.check().expect_err("refused");
            assert!(refusal.starts_with("device "), "{refusal}");
        }
    }
}
