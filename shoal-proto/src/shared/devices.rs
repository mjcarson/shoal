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
//!     - {path: /mnt/hdd0/shoal, class: hdd, journal: /mnt/nvme0/shoal-journal}
//! ```
//!
//! A device the kernel calls rotational journals on an SSD of its node, named by `journal`, runs
//! with its disk's write cache off, on XFS, as one slice ([M19](../../../docs/src/features/rotational-devices.md)).
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

/// The journal a slice keeps when its device names no size: four segments of 64 MiB
pub const DEFAULT_JOURNAL_BYTES: u64 = 256 << 20;

/// The fewest bytes a slice's journal can have: four segments of 1 MiB
pub const MIN_JOURNAL_BYTES: u64 = 4 << 20;

/// The segments a slice's journal is cut into
pub const JOURNAL_SEGMENTS: u64 = 4;

/// The files removed chunks give back that a slice keeps of each length, unless its device says;
/// ~~the files written ahead a slice keeps for each chunk length it serves~~ until
/// [F96](../../../docs/src/features/chunk-file-classes.md) retired the refill
pub const DEFAULT_FREE_FILES: u32 = 8;

/// The most files a slice keeps of one length
pub const MAX_FREE_FILES: u32 = 1024;

/// The free bytes a device keeps when nothing names its reserve: 1 GiB, as a move's reserve is
pub const DEFAULT_DEVICE_RESERVE: u64 = 1 << 30;

/// The longest a rotational device's batch of applies takes unless it says: 100 ms
///
/// A foreground read beside applies waits for at most the batch in flight, and X7 found a read's
/// p99 rose with the batch: 0.2 to 0.8 s at 32 applies in flight. Decided with the user on
/// 2026-10-16 ([F91](../../../docs/src/features/rotational-devices.md)): at the disk's 5 to 20 ms an
/// apply in flight, five to twenty applies a batch.
pub const DEFAULT_APPLY_BATCH: Millis = Millis(100);

/// The shortest bound a device's batch of applies may have
pub const MIN_APPLY_BATCH: Millis = Millis(1);

/// The longest bound a device's batch of applies may have: ten seconds, past which a read beside
/// applies would wait longer than any client's deadline
pub const MAX_APPLY_BATCH: Millis = Millis(10_000);

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

/// A journal directory's identity, minted the first time it is claimed and kept in its marker
///
/// One journal directory is one SSD's filesystem, since a node refuses two on one, so devices that
/// journal under one id fail together: placement counts them as one `device` failure domain
/// ([S5](../../../docs/src/object-storage/placement.md#failure-domains)).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct JournalId(pub uuid::Uuid);

impl JournalId {
    /// A new journal's id, ordered by when it was minted
    #[must_use]
    pub fn mint() -> Self {
        JournalId(uuid::Uuid::now_v7())
    }
}

impl std::fmt::Display for JournalId {
    /// Write the id as its uuid
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What a device's configuration says of its disk's volatile write cache
///
/// A rotational device runs with its cache off: with it on, the lab's 14 TB disks stalled a read
/// behind a flush for a tenth of a second and its 6 TB disk acknowledged a sync before its platter
/// could hold the block ([X7](../../../docs/src/object-storage/device-store-hdd.md)). `on` says the
/// cache is on deliberately, which starts the device with a warning that names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WriteCache {
    /// The cache is on, deliberately; the device starts with a warning
    On,
    /// The cache is off, which the kernel must say for a rotational device to start
    Off,
}

/// A span of milliseconds written as `100ms` or `2s`
///
/// A bare number is refused, as a span of seconds refuses one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Millis(pub u64);

impl Millis {
    /// The span as a duration
    #[must_use]
    pub const fn duration(self) -> std::time::Duration {
        std::time::Duration::from_millis(self.0)
    }

    /// Parse a span of milliseconds from its written form
    ///
    /// # Arguments
    ///
    /// * `text` - A whole number followed by `ms` or `s`
    ///
    /// # Errors
    ///
    /// Text with no unit, another unit, no whole number, or a span past a `u64` of milliseconds.
    pub fn parse(text: &str) -> Result<Self, String> {
        // the unit is whatever follows the digits
        let text = text.trim();
        let split = text
            .find(|character: char| !character.is_ascii_digit())
            .ok_or_else(|| format!("{text:?} has no unit; write it as 100ms or 2s"))?;
        let (number, unit) = text.split_at(split);
        let number: u64 = number
            .parse()
            .map_err(|_| format!("{text:?} does not start with a whole number"))?;
        // one of two units, and nothing else
        let scale = match unit {
            "ms" => 1,
            "s" => 1000,
            other => return Err(format!("{text:?} has unit {other:?}; use ms or s")),
        };
        number
            .checked_mul(scale)
            .map(Millis)
            .ok_or_else(|| format!("{text:?} is longer than any span there is"))
    }
}

impl std::fmt::Display for Millis {
    /// Write the span in seconds when it is whole seconds, and in milliseconds otherwise
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 != 0 && self.0.is_multiple_of(1000) {
            write!(f, "{}s", self.0 / 1000)
        } else {
            write!(f, "{}ms", self.0)
        }
    }
}

impl Serialize for Millis {
    /// Write the span as its written form
    ///
    /// # Arguments
    ///
    /// * `serializer` - Where it goes
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Millis {
    /// Read the span from its written form
    ///
    /// # Arguments
    ///
    /// * `deserializer` - Where it comes from
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Millis::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// How an apply's write in place is made durable
///
/// `shared` is one flush of the device covering every apply that completed before it began, X8's
/// supplement, which made small writes 1.70 times as fast on the 970 EVO; it leans on a sync of one
/// file flushing the device for direct writes into every file ~~written ahead~~ whose every byte was
/// written and synced before anything named it (F96), which XFS and ext4 do
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
    /// The directory on an SSD of this node its slices journal in, which a rotational device
    /// must name and any other may; under its own directory unless set
    ///
    /// A stage on a disk took 42 to 251 ms beside applies, and a millisecond or less on the SSD
    /// beside it ([X7](../../../docs/src/object-storage/device-store-hdd.md)). The directory must
    /// exist, is never created, and is claimed with a marker of its own; devices that name one
    /// fail together ([F91](../../../docs/src/features/rotational-devices.md)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal: Option<PathBuf>,
    /// The bytes of each slice's journal, written ahead in four segments
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_size: Option<ByteSize>,
    /// What its disk's volatile write cache is: a rotational device starts only when the kernel
    /// says its cache writes through, unless this says `on`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_cache: Option<WriteCache>,
    /// The longest one batch of a rotational device's applies takes; 100 ms unless set, and read
    /// on no other device
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply_batch: Option<Millis>,
    /// The files removed chunks give back that each slice keeps of each length; ~~the files
    /// written ahead each slice keeps for each chunk length it serves~~ until F96
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_files: Option<u32>,
    /// The bytes a second recovery may read and write on it, rebuilds and moves together; 256 MiB
    /// a second on an `ssd` and 64 MiB on any other class unless set
    ///
    /// A ceiling above the pacing by the device's idle time, which is what holds a foreground's
    /// tail ([X12](../../../docs/src/object-storage/recovery-scrub-rates.md),
    /// [F88](../../../docs/src/features/recovery-and-moves.md)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_ceiling: Option<ByteSize>,
    /// How many checksum failures within `checksum_window` fail it: ten unless set, and zero
    /// for none ([F89](../../../docs/src/features/scrub-and-repair.md))
    ///
    /// A device whose chunks keep failing is failing; past this its node fails it as an
    /// operator's order would, and its placement groups move off. Ten in ten minutes is ZFS's
    /// `checksum_n` and `checksum_t`, decided with the user on 2026-10-15.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_failures: Option<u32>,
    /// The time `checksum_failures` are counted over; ten minutes unless set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_window: Option<crate::shared::scrub::Span>,
    /// How many checksum failures within `checksum_drain_window` drain it: three unless set, and
    /// zero for none ([F92](../../../docs/src/features/reclamation-and-device-lifecycle.md))
    ///
    /// A device that fails a few checksums over a day is wearing out before it fails: drained, its
    /// placement groups are copied off it while it still serves them, which a failure past
    /// `checksum_failures` would leave to a rebuild from the other holders. Decided with the user
    /// on 2026-10-16.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_drain: Option<u32>,
    /// The time `checksum_drain` failures are counted over; a day unless set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_drain_window: Option<crate::shared::scrub::Span>,
}

/// How many checksum failures within [`CHECKSUM_WINDOW`] fail a device that sets none
pub const CHECKSUM_FAILURES: u32 = 10;

/// The time a device's checksum failures are counted over when it sets none: ten minutes
pub const CHECKSUM_WINDOW: crate::shared::scrub::Span = crate::shared::scrub::Span(600);

/// How many checksum failures within [`CHECKSUM_DRAIN_WINDOW`] drain a device that sets none (F92)
pub const CHECKSUM_DRAIN: u32 = 3;

/// The time a device's checksum failures are counted over for a drain when it sets none: a day
pub const CHECKSUM_DRAIN_WINDOW: crate::shared::scrub::Span = crate::shared::scrub::Span(86_400);

/// The recovery ceiling of a device the kernel calls solid state that sets none: 256 MiB a second
pub const SSD_RECOVERY_CEILING: u64 = 256 << 20;

/// The recovery ceiling of a device the kernel calls rotational that sets none: 64 MiB a second
///
/// Above what pacing by the disk's idle time gives, 44 to 59 MiB/s on the lab's disks, since X12
/// found a lower ceiling cost the foreground what none did
/// ([X12](../../../docs/src/object-storage/recovery-scrub-rates.md)).
pub const HDD_RECOVERY_CEILING: u64 = 64 << 20;

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
            journal_size: None,
            write_cache: None,
            apply_batch: None,
            free_files: None,
            recovery_ceiling: None,
            checksum_failures: None,
            checksum_window: None,
            checksum_drain: None,
            checksum_drain_window: None,
        }
    }

    /// How many checksum failures within its window fail it, zero for none, and the window
    #[must_use]
    pub fn checksum_limit(&self) -> (u32, std::time::Duration) {
        (
            self.checksum_failures.unwrap_or(CHECKSUM_FAILURES),
            self.checksum_window.unwrap_or(CHECKSUM_WINDOW).duration(),
        )
    }

    /// How many checksum failures within its drain window drain it, zero for none, and the window
    /// (F92)
    #[must_use]
    pub fn checksum_drain(&self) -> (u32, std::time::Duration) {
        (
            self.checksum_drain.unwrap_or(CHECKSUM_DRAIN),
            self.checksum_drain_window.unwrap_or(CHECKSUM_DRAIN_WINDOW).duration(),
        )
    }

    /// Drain it past this many checksum failures within a window, zero for never (F92)
    ///
    /// # Arguments
    ///
    /// * `failures` - How many
    /// * `window` - Within how long
    #[must_use]
    pub fn with_checksum_drain(mut self, failures: u32, window: crate::shared::scrub::Span) -> Self {
        self.checksum_drain = Some(failures);
        self.checksum_drain_window = Some(window);
        self
    }

    /// Fail it past this many checksum failures within a window, zero for never
    ///
    /// # Arguments
    ///
    /// * `failures` - How many
    /// * `window` - Within how long
    #[must_use]
    pub fn with_checksum_limit(mut self, failures: u32, window: crate::shared::scrub::Span) -> Self {
        self.checksum_failures = Some(failures);
        self.checksum_window = Some(window);
        self
    }

    /// The bytes a second recovery may read and write on it
    ///
    /// Chosen by what the kernel says of its disk, not by its class, which is only a label
    /// ([F91](../../../docs/src/features/rotational-devices.md)).
    ///
    /// # Arguments
    ///
    /// * `rotational` - Whether the kernel calls its disk rotational
    #[must_use]
    pub fn recovery_rate(&self, rotational: bool) -> u64 {
        self.recovery_ceiling.map_or(
            if rotational {
                HDD_RECOVERY_CEILING
            } else {
                SSD_RECOVERY_CEILING
            },
            |ceiling| ceiling.0,
        )
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

    /// Let recovery move this many bytes a second on it
    ///
    /// # Arguments
    ///
    /// * `bytes` - The ceiling, a second
    #[must_use]
    pub fn with_recovery_ceiling(mut self, bytes: u64) -> Self {
        self.recovery_ceiling = Some(ByteSize(bytes));
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
    pub fn journal_size(mut self, bytes: u64) -> Self {
        self.journal_size = Some(ByteSize(bytes));
        self
    }

    /// Journal its slices in a directory on another device of this node
    ///
    /// # Arguments
    ///
    /// * `path` - The journal directory
    #[must_use]
    pub fn journal<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.journal = Some(path.into());
        self
    }

    /// Say what its disk's write cache is
    ///
    /// # Arguments
    ///
    /// * `cache` - The cache, on deliberately or off
    #[must_use]
    pub fn write_cache(mut self, cache: WriteCache) -> Self {
        self.write_cache = Some(cache);
        self
    }

    /// Bound one batch of its applies, on a rotational device, by this long
    ///
    /// # Arguments
    ///
    /// * `bound` - The longest a batch takes
    #[must_use]
    pub fn apply_batch(mut self, bound: Millis) -> Self {
        self.apply_batch = Some(bound);
        self
    }

    /// The longest one batch of its applies takes, on a rotational device
    #[must_use]
    pub fn apply_batch_bound(&self) -> std::time::Duration {
        self.apply_batch.unwrap_or(DEFAULT_APPLY_BATCH).duration()
    }

    /// Whether its configuration says its disk's write cache is on deliberately
    #[must_use]
    pub fn cache_on_deliberately(&self) -> bool {
        self.write_cache == Some(WriteCache::On)
    }

    /// Keep this many files of each length that removed chunks give back
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
        self.journal_size.map_or(DEFAULT_JOURNAL_BYTES, ByteSize::get)
    }

    /// The bytes of each of a slice's journal segments, a whole number of 4 KiB blocks
    #[must_use]
    pub fn segment_bytes(&self) -> u64 {
        // a quarter of the journal, rounded down to a block
        (self.journal_bytes() / JOURNAL_SEGMENTS) & !4095
    }

    /// The files removed chunks give back that a slice keeps of each length
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
                "device {name}: it keeps {} free files of a length, and a slice keeps 1 to {MAX_FREE_FILES}",
                self.free_files_kept()
            ));
        }
        // a journal elsewhere is a directory of its own, never the device's or one inside it
        if let Some(journal) = &self.journal {
            if !journal.is_absolute() {
                return Err(format!(
                    "device {name}: its journal {} is not an absolute path",
                    journal.display()
                ));
            }
            if journal.starts_with(&self.path) || self.path.starts_with(journal) {
                return Err(format!(
                    "device {name}: its journal {} is in or around the device; name a directory on an SSD",
                    journal.display()
                ));
            }
        }
        if let Some(bound) = self.apply_batch {
            if bound < MIN_APPLY_BATCH || bound > MAX_APPLY_BATCH {
                return Err(format!(
                    "device {name}: its batch of applies is bounded at {bound}, and a bound is {MIN_APPLY_BATCH} to {MAX_APPLY_BATCH}"
                ));
            }
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
    /// The bytes of the chunk files it holds, headers included: what a device's reclamation gives
    /// back ([F92](../../../docs/src/features/reclamation-and-device-lifecycle.md))
    #[serde(default)]
    pub chunk_bytes: u64,
    /// The bytes staged on it and not yet applied or discarded
    #[serde(default)]
    pub staged_bytes: u64,
    /// The units and headers on it that failed their checksums since its node started, each
    /// found by a read, an apply or a scrub ([F89](../../../docs/src/features/scrub-and-repair.md))
    #[serde(default)]
    pub checksum_failures: u64,
    /// The chunks set aside on it for failing a checksum, each kept as evidence until a
    /// replacement verifies
    #[serde(default)]
    pub quarantined: u64,
    /// The bytes deep scrubs have read and verified on it since its node started
    #[serde(default)]
    pub scrub_bytes: u64,
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
    /// What the kernel says its disk's write cache does: `write through` or `write back`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_cache: Option<String>,
    /// The directory its slices journal in, when it is not under the device
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal: Option<String>,
    /// What an operator is warned of about it: a class the kernel disagrees with, a filesystem
    /// the store was not measured on, a write cache on deliberately, a journal lost
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Whether it is serving
    #[serde(default)]
    pub health: DeviceHealth,
    /// Why its node asks for it to be drained, if it does: its checksum failures passed its drain
    /// threshold, and the control leader drains it once (F92)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_wanted: Option<String>,
}

impl DeviceReport {
    /// Whether it is short of space: its free bytes at or under its reserve
    #[must_use]
    pub fn short_of_space(&self) -> bool {
        self.free_bytes <= self.reserve_bytes
    }

    /// Whether every slice of it holds nothing, chunk or staged byte: what a drained device is
    /// removed once of (F92)
    #[must_use]
    pub fn holds_nothing(&self) -> bool {
        self.slices
            .iter()
            .all(|slice| slice.chunks == 0 && slice.staged_bytes == 0)
    }

    /// The checksum failures its slices found since its node started
    #[must_use]
    pub fn checksum_failures(&self) -> u64 {
        self.slices.iter().map(|slice| slice.checksum_failures).sum()
    }

    /// The chunks its slices hold in quarantine
    #[must_use]
    pub fn quarantined(&self) -> u64 {
        self.slices.iter().map(|slice| slice.quarantined).sum()
    }

    /// The bytes its deep scrubs read since its node started
    #[must_use]
    pub fn scrub_bytes(&self) -> u64 {
        self.slices.iter().map(|slice| slice.scrub_bytes).sum()
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
    /// Whether the kernel calls its disk rotational, which places nothing and is read to warn of a
    /// pool whose media disagrees ([F91](../../../docs/src/features/rotational-devices.md))
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rotational: bool,
    /// The journal it stages in, when that is on another device of its node: devices that share
    /// one fail together, and are one `device` failure domain
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal: Option<JournalId>,
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
    use super::{ApplySync, DeviceSpec, Millis, WriteCache, DEFAULT_APPLY_BATCH, DEFAULT_JOURNAL_BYTES};

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
        let full = "path: /d\nclass: hdd\nslices: 2\nweight: 4TiB\nreserve: 2GiB\napply_sync: each\njournal_size: 64MiB\nfree_files: 4\njournal: /ssd/journal\nwrite_cache: on\napply_batch: 250ms\n";
        let device: DeviceSpec = serde_yaml::from_str(full).expect("every field reads");
        assert_eq!(device.slices, 2);
        assert_eq!(device.apply_sync, Some(ApplySync::Each));
        assert_eq!(device.weight.map(|weight| weight.get()), Some(4 << 40));
        assert_eq!(device.journal_bytes(), 64 << 20);
        assert_eq!(device.journal.as_deref(), Some(std::path::Path::new("/ssd/journal")));
        assert_eq!(device.write_cache, Some(WriteCache::On));
        assert!(device.cache_on_deliberately());
        assert_eq!(device.apply_batch, Some(Millis(250)));
        assert_eq!(device.apply_batch_bound(), std::time::Duration::from_millis(250));
        assert_eq!(
            DeviceSpec::new("/d", "hdd").apply_batch_bound(),
            DEFAULT_APPLY_BATCH.duration()
        );
        // a bound reads with a unit and writes back the same, and a bare number is refused
        assert_eq!(Millis::parse("2s"), Ok(Millis(2000)));
        assert_eq!(Millis(2000).to_string(), "2s");
        assert_eq!(Millis(250).to_string(), "250ms");
        assert!(Millis::parse("250").is_err());
        assert!(Millis::parse("1m").is_err());
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
            DeviceSpec::new("/d", "ssd").journal_size(1 << 20),
            DeviceSpec::new("/d", "ssd").free_files(0),
            DeviceSpec::new("/d", "hdd").journal("relative/journal"),
            DeviceSpec::new("/d", "hdd").journal("/d/journal"),
            DeviceSpec::new("/d/inner", "hdd").journal("/d"),
            DeviceSpec::new("/d", "hdd").apply_batch(Millis(0)),
            DeviceSpec::new("/d", "hdd").apply_batch(Millis(60_000)),
        ];
        for device in refused {
            let refusal = device.check().expect_err("refused");
            assert!(refusal.starts_with("device "), "{refusal}");
        }
    }
}
