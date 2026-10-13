//! Claiming a node's devices and their slices, before anything is written to them
//!
//! A device and each of its slices are claimed the way a storage root is, with three outcomes and
//! no fourth ([S4](../../../../docs/src/object-storage/pools-and-devices.md#a-device-has-slices)):
//!
//! - **empty**: a new device, or a new slice, given a new id. A replaced disk mounted at the old
//!   path is an empty directory, and it comes up as a new device whose slices hold nothing, never as
//!   the old one with its chunks gone; the old id is reported failed so its chunks are rebuilt;
//! - **marked**: held to this node, this cluster and this device, and refused by name otherwise;
//! - **files with no marker**: somebody's data, refused by name.
//!
//! A table's root differs on exactly one point: an empty root the node's marker lists is refused,
//! since a table's root has no rebuild of its own, where a device's chunks do.
//!
//! # Invariants
//!
//! **A device's path is never created.** It is where a disk is mounted; an unmounted disk leaves
//! an empty directory on the root filesystem, or none, and either is a device that is missing or
//! new, never the old one ([F85](../../../../docs/src/features/devices-and-pools.md)).
//!
//! **The device's marker is written before any slice is made**, so a crash between the two leaves
//! a marked device with empty slices, which the next claim makes, and never slices under a device
//! with no marker, which it would refuse.
//!
//! **A slice's number never changes while it holds a chunk**: a slice's id is in every chunk's
//! path, and a live re-slicing is not worth its protocol.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write as _;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{event, Level};

use super::probe::{self, Facts};
use crate::server::errors::ShoalError;
use crate::server::ServerError;
use crate::shared::devices::{ApplySync, DeviceId, DeviceSpec, SliceId};
use crate::shared::identity::{ClusterId, NodeId};

/// The marker a device's directory carries
pub const DEVICE_MARKER: &str = "shoal-device.json";

/// The marker each slice's directory carries
pub const SLICE_MARKER: &str = "shoal-slice.json";

/// The lock file a device and each of its slices hold while the node runs
pub const LOCK_FILE: &str = "shoal.lock";

/// The format of a device's and a slice's marker
pub const MARKER_FORMAT: u32 = 1;

/// The entries a device's directory holds for itself, which do not make it used
const DEVICE_OWN: &[&str] = &[DEVICE_MARKER, "shoal-device.json.tmp", LOCK_FILE, "lost+found"];

/// The entries a slice's directory holds for itself, which do not make it used
const SLICE_OWN: &[&str] = &[SLICE_MARKER, "shoal-slice.json.tmp", LOCK_FILE];

/// How many of an unmarked directory's entries its refusal names
const ENTRIES_NAMED: usize = 5;

/// The marker at a device's directory
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceMarker {
    /// The marker's format
    pub format: u32,
    /// The device's id, minted when the directory was first claimed
    pub device: DeviceId,
    /// The node that claimed it; a device never moves between nodes
    pub node: NodeId,
    /// The cluster that node was in, if any
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterId>,
    /// The class its operator wrote when it was last claimed
    pub class: String,
    /// The slices it is cut into
    pub slices: u16,
}

/// The marker at a slice's directory
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceMarker {
    /// The marker's format
    pub format: u32,
    /// The slice's id, minted when it was made
    pub slice: SliceId,
    /// The device it is a slice of
    pub device: DeviceId,
    /// Its index on the device
    pub index: u16,
}

/// A lock held on a directory while the node runs, released by the kernel if it dies
#[derive(Debug)]
pub struct HeldLock {
    /// The open lock file, whose descriptor carries the lock
    _file: File,
}

/// One slice claimed
#[derive(Debug)]
pub struct ClaimedSlice {
    /// Its id
    pub id: SliceId,
    /// Its index on its device
    pub index: u16,
    /// Its directory
    pub path: PathBuf,
    /// Its lock
    _lock: HeldLock,
}

/// One device claimed, its slices with it
#[derive(Debug)]
pub struct ClaimedDevice {
    /// What the configuration named
    pub spec: DeviceSpec,
    /// Its id
    pub id: DeviceId,
    /// What it is on, as the probe found it at the claim
    pub facts: Facts,
    /// Its slices, by index
    pub slices: Vec<ClaimedSlice>,
    /// The free bytes it keeps
    pub reserve: u64,
    /// How its applies are made durable
    pub apply_sync: ApplySync,
    /// Whether it was claimed new
    pub fresh: bool,
    /// What an operator is warned of about it
    pub warnings: Vec<String>,
    /// Its lock
    _lock: HeldLock,
}

/// A device the node knows by an id and cannot serve: missing, answering nothing, or replaced
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedDevice {
    /// Where it was claimed
    pub path: PathBuf,
    /// The id it had
    pub id: DeviceId,
    /// The class its configuration names now, empty when it names the device no longer
    pub class: String,
    /// Why it cannot be served
    pub reason: String,
}

/// Everything a node claimed of its devices
#[derive(Debug, Default)]
pub struct Claimed {
    /// The devices it serves
    pub devices: Vec<ClaimedDevice>,
    /// The ones it knows and cannot serve
    pub failed: Vec<FailedDevice>,
}

/// What a directory holds, read before anything is written to it
enum DirState<M> {
    /// Nothing but what a claim writes for itself, or nothing at all
    Empty,
    /// A marker
    Marked(M),
    /// Files and no marker
    Unmarked {
        /// The first few entries in it, by name
        found: Vec<String>,
        /// How many more there are
        more: usize,
    },
}

/// A refusal naming a device's path
///
/// # Arguments
///
/// * `path` - The device's or the slice's path
/// * `why` - Why
fn refuse(path: &Path, why: impl Into<String>) -> ServerError {
    ServerError::Shoal(ShoalError::DeviceRefused {
        path: path.to_path_buf(),
        why: why.into(),
    })
}

/// Look at what a directory holds, without writing to it
///
/// # Arguments
///
/// * `dir` - The directory
/// * `marker` - Its marker's name
/// * `own` - The entries a claim writes for itself
///
/// # Errors
///
/// A marker that does not read, or a directory that cannot be listed.
fn survey<M: for<'de> Deserialize<'de>>(
    dir: &Path,
    marker: &str,
    own: &[&str],
) -> Result<DirState<M>, ServerError> {
    // a marker settles it, whatever else is beside it
    match std::fs::read(dir.join(marker)) {
        Ok(bytes) => {
            let found = serde_json::from_slice(&bytes).map_err(|error| {
                refuse(dir, format!("its marker {marker} does not read: {error}"))
            })?;
            return Ok(DirState::Marked(found));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(refuse(dir, format!("its marker cannot be read: {error}"))),
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(DirState::Empty),
        Err(error) => return Err(refuse(dir, format!("it cannot be listed: {error}"))),
    };
    let mut found = Vec::new();
    for entry in entries {
        let name = entry
            .map_err(|error| refuse(dir, format!("it cannot be listed: {error}")))?
            .file_name()
            .to_string_lossy()
            .into_owned();
        if !own.contains(&name.as_str()) {
            found.push(name);
        }
    }
    if found.is_empty() {
        return Ok(DirState::Empty);
    }
    found.sort_unstable();
    let more = found.len().saturating_sub(ENTRIES_NAMED);
    found.truncate(ENTRIES_NAMED);
    Ok(DirState::Unmarked { found, more })
}

/// Write a marker: staged, synced, renamed over, and the directory synced
///
/// # Arguments
///
/// * `dir` - The directory it marks
/// * `name` - The marker's name
/// * `marker` - What it says
///
/// # Errors
///
/// When the file or the directory cannot be written or synced.
fn write_marker<M: Serialize>(dir: &Path, name: &str, marker: &M) -> Result<(), ServerError> {
    let staged = dir.join(format!("{name}.tmp"));
    let mut file = File::create(&staged).map_err(|error| refuse(dir, error.to_string()))?;
    file.write_all(&serde_json::to_vec_pretty(marker)?)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&staged, dir.join(name))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Take a directory's lock, never creating the directory
///
/// # Arguments
///
/// * `dir` - The directory, which must exist
///
/// # Errors
///
/// When another process holds it, or the lock file cannot be opened.
fn lock(dir: &Path) -> Result<HeldLock, ServerError> {
    let path = dir.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|error| refuse(dir, format!("its lock file cannot be opened: {error}")))?;
    // SAFETY: `flock` on a descriptor this function just opened and still owns
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let error = std::io::Error::last_os_error();
        return Err(match error.raw_os_error() {
            Some(libc::EWOULDBLOCK) => refuse(dir, "another process holds it"),
            _ => ServerError::IO(error),
        });
    }
    Ok(HeldLock { _file: file })
}

/// Whether one path is the other or inside it, by components
///
/// # Arguments
///
/// * `inner` - The path that may be inside
/// * `outer` - The path that may hold it
fn within(inner: &Path, outer: &Path) -> bool {
    inner.starts_with(outer)
}

/// Whether a slice's directory holds a chunk, staged or applied
///
/// # Arguments
///
/// * `slice` - The slice's directory
fn holds_chunks(slice: &Path) -> bool {
    std::fs::read_dir(slice.join("chunks"))
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false)
}

/// What an operator is warned of about a device whose kernel and class disagree
///
/// # Arguments
///
/// * `spec` - The device as configured
/// * `facts` - What it is on
fn label_warnings(spec: &DeviceSpec, facts: &Facts) -> Vec<String> {
    let mut warnings = Vec::new();
    let class = spec.class.to_ascii_lowercase();
    let solid = class.starts_with("ssd") || class.starts_with("nvme");
    // a class is what an operator wrote: detection warns and never overrules
    if facts.rotational && solid {
        warnings.push(format!(
            "the kernel calls this device rotational and its class says {}; it keeps its class",
            spec.class
        ));
    }
    if !facts.rotational && class.starts_with("hdd") {
        warnings.push(format!(
            "the kernel calls this device non-rotational and its class says {}; it keeps its class",
            spec.class
        ));
    }
    if facts.rotational {
        warnings.push(
            "a rotational device's rules - its journal on an SSD, its write cache off, XFS alone - \
             are not enforced before M19"
                .to_string(),
        );
    }
    if !facts.measured() {
        warnings.push(format!(
            "it is on {}, which the device store was not measured on; XFS is preferred",
            facts.filesystem
        ));
    }
    warnings
}

/// Claim one slice of a device
///
/// # Arguments
///
/// * `device` - The device's id
/// * `dir` - The slice's directory
/// * `index` - Its index on the device
///
/// # Errors
///
/// A slice another device's, or with files and no marker, refused by name.
fn claim_slice(device: DeviceId, dir: &Path, index: u16) -> Result<ClaimedSlice, ServerError> {
    let state = survey::<SliceMarker>(dir, SLICE_MARKER, SLICE_OWN)?;
    let id = match state {
        DirState::Marked(marker) => {
            if marker.device != device || marker.index != index {
                return Err(refuse(
                    dir,
                    format!(
                        "it is slice {} of device {}, not slice {index} of device {device}",
                        marker.index, marker.device
                    ),
                ));
            }
            marker.slice
        }
        DirState::Unmarked { found, more } => {
            return Err(refuse(
                dir,
                format!(
                    "this slice's directory holds files and no marker, so it is somebody's data: \
                     {found:?}{}",
                    if more > 0 { format!(" and {more} more") } else { String::new() }
                ),
            ))
        }
        DirState::Empty => {
            // a new slice: its directory, then its marker
            std::fs::create_dir_all(dir).map_err(|error| refuse(dir, error.to_string()))?;
            let marker = SliceMarker {
                format: MARKER_FORMAT,
                slice: SliceId::mint(),
                device,
                index,
            };
            write_marker(dir, SLICE_MARKER, &marker)?;
            marker.slice
        }
    };
    let held = lock(dir)?;
    Ok(ClaimedSlice {
        id,
        index,
        path: dir.to_path_buf(),
        _lock: held,
    })
}

/// The directory of a device's slice
///
/// # Arguments
///
/// * `device` - The device's path
/// * `index` - The slice's index
#[must_use]
pub fn slice_dir(device: &Path, index: u16) -> PathBuf {
    device.join(format!("slice-{index}"))
}

/// Claim one device that exists and answers
///
/// # Arguments
///
/// * `spec` - The device as configured
/// * `facts` - What it is on
/// * `node` - This node
/// * `cluster` - The cluster it is in, if any
/// * `known` - The id this node had for the path, if any
/// * `default_reserve` - The reserve a device keeps when it names none
///
/// # Errors
///
/// Anything the claim refuses, by name.
fn claim_device(
    spec: &DeviceSpec,
    facts: Facts,
    node: NodeId,
    cluster: Option<ClusterId>,
    default_reserve: u64,
) -> Result<ClaimedDevice, ServerError> {
    let path = &spec.path;
    let state = survey::<DeviceMarker>(path, DEVICE_MARKER, DEVICE_OWN)?;
    let (id, fresh) = match state {
        DirState::Unmarked { found, more } => {
            return Err(refuse(
                path,
                format!(
                    "it holds files and no marker, so it is somebody's data; Shoal claims only an \
                     empty directory or one it marked: {found:?}{}",
                    if more > 0 { format!(" and {more} more") } else { String::new() }
                ),
            ))
        }
        DirState::Marked(mut marker) => {
            // held to this node and its cluster
            if marker.node != node {
                return Err(refuse(
                    path,
                    format!(
                        "it was claimed by node {}, and this is node {node}; a device does not move \
                         between nodes",
                        marker.node
                    ),
                ));
            }
            if let (Some(theirs), Some(ours)) = (marker.cluster, cluster) {
                if theirs != ours {
                    return Err(refuse(
                        path,
                        format!("it belongs to cluster {theirs}, and this node is in cluster {ours}"),
                    ));
                }
            }
            // a change of slices is refused while any of them holds a chunk
            if marker.slices != spec.slices {
                let holding: Vec<u16> = (0..marker.slices.max(spec.slices))
                    .filter(|index| holds_chunks(&slice_dir(path, *index)))
                    .collect();
                if !holding.is_empty() {
                    return Err(refuse(
                        path,
                        format!(
                            "it is cut into {} slices and its configuration names {}; slices \
                             {holding:?} hold chunks, so drain the device and add it again to \
                             change them",
                            marker.slices, spec.slices
                        ),
                    ));
                }
                // nothing held: the slices are made again
                for index in 0..marker.slices.max(spec.slices) {
                    let dir = slice_dir(path, index);
                    if dir.exists() {
                        std::fs::remove_dir_all(&dir).map_err(|error| refuse(&dir, error.to_string()))?;
                    }
                }
            }
            // the class is what the operator wrote now, and the cluster once it has one
            let changed = marker.class != spec.class
                || marker.slices != spec.slices
                || (marker.cluster.is_none() && cluster.is_some());
            if changed {
                marker.class.clone_from(&spec.class);
                marker.slices = spec.slices;
                marker.cluster = marker.cluster.or(cluster);
                write_marker(path, DEVICE_MARKER, &marker)?;
            }
            (marker.device, false)
        }
        DirState::Empty => {
            // a new device: its marker before any slice
            let marker = DeviceMarker {
                format: MARKER_FORMAT,
                device: DeviceId::mint(),
                node,
                cluster,
                class: spec.class.clone(),
                slices: spec.slices,
            };
            write_marker(path, DEVICE_MARKER, &marker)?;
            (marker.device, true)
        }
    };
    let held = lock(path)?;
    // every slice, each with its own three outcomes
    let mut slices = Vec::with_capacity(usize::from(spec.slices));
    for index in 0..spec.slices {
        slices.push(claim_slice(id, &slice_dir(path, index), index)?);
    }
    let warnings = label_warnings(spec, &facts);
    for warning in &warnings {
        event!(Level::WARN, device = %path.display(), id = %id, msg = %warning);
    }
    // a shared flush where a sync of one file is known to flush the device, one each elsewhere
    let apply_sync = spec.apply_sync.unwrap_or(if facts.measured() {
        ApplySync::Shared
    } else {
        ApplySync::Each
    });
    Ok(ClaimedDevice {
        spec: spec.clone(),
        id,
        facts,
        slices,
        reserve: spec.reserve.map_or(default_reserve, |reserve| reserve.get()),
        apply_sync,
        fresh,
        warnings,
        _lock: held,
    })
}

/// Claim every device a node's configuration names
///
/// A device that is missing, or that answers nothing, is reported failed under the id this node
/// had for its path, and the node starts; one this node never claimed is refused, since it has no
/// id to fail under. A device found empty where this node had claimed one is a new device, and
/// its predecessor's id is reported failed.
///
/// # Arguments
///
/// * `specs` - The devices the configuration names
/// * `roots` - Every storage root, which no device may be in or around
/// * `node` - This node
/// * `cluster` - The cluster it is in, if any
/// * `known` - The id this node had for each path
/// * `default_reserve` - The reserve a device keeps when it names none
///
/// # Errors
///
/// Anything refused, by name: a device that cannot be, one nested in a root or in another device,
/// two on one filesystem, a filesystem the store refuses, files with no marker, another node's or
/// cluster's device, or a change of slices while chunks are held.
pub fn claim(
    specs: &[DeviceSpec],
    roots: &[PathBuf],
    node: NodeId,
    cluster: Option<ClusterId>,
    known: &BTreeMap<PathBuf, DeviceId>,
    default_reserve: u64,
) -> Result<Claimed, ServerError> {
    // every device on its own, then against the roots and each other
    for (index, spec) in specs.iter().enumerate() {
        spec.check().map_err(|why| refuse(&spec.path, why))?;
        for root in roots {
            if within(&spec.path, root) || within(root, &spec.path) {
                return Err(refuse(
                    &spec.path,
                    format!("it is in or around the storage root {}", root.display()),
                ));
            }
        }
        for other in &specs[..index] {
            if within(&spec.path, &other.path) || within(&other.path, &spec.path) {
                return Err(refuse(
                    &spec.path,
                    format!("it is in or around the device {}", other.path.display()),
                ));
            }
        }
    }
    let shared_allowed = probe::current_override().is_some_and(|over| over.shared_filesystem);
    let mut claimed = Claimed::default();
    let mut seen: BTreeMap<String, PathBuf> = BTreeMap::new();
    for spec in specs {
        let path = &spec.path;
        let known_id = known.get(path).copied();
        // a path that is not there, or answers nothing, is a device that failed if it was known
        let answer = crate::server::faults::guard(path).and_then(|()| {
            if path.is_dir() {
                Ok(())
            } else if path.exists() {
                Err(std::io::Error::other("it is not a directory"))
            } else {
                Err(std::io::Error::new(std::io::ErrorKind::NotFound, "it is missing"))
            }
        });
        let facts = answer.and_then(|()| probe::probe(path));
        let facts = match (facts, known_id) {
            (Ok(facts), _) => facts,
            (Err(error), Some(id)) => {
                let reason = format!("its path {} cannot be served: {error}", path.display());
                event!(Level::ERROR, device = %path.display(), id = %id, msg = %reason);
                claimed.failed.push(FailedDevice {
                    path: path.clone(),
                    id,
                    class: spec.class.clone(),
                    reason,
                });
                continue;
            }
            (Err(error), None) => {
                return Err(refuse(
                    path,
                    format!(
                        "it cannot be claimed: {error}; a device's path is where its disk is \
                         mounted, and is never created"
                    ),
                ))
            }
        };
        // a filesystem the store cannot run on
        if let Some(why) = facts.refusal() {
            return Err(refuse(path, why));
        }
        // two devices on one filesystem are one disk counted as two failure domains
        if let Some(other) = seen.get(&facts.device) {
            if !shared_allowed {
                return Err(refuse(
                    path,
                    format!(
                        "it is on filesystem {}, as the device {} is; two devices on one disk would \
                         count as two failure domains",
                        facts.device,
                        other.display()
                    ),
                ));
            }
        }
        seen.insert(facts.device.clone(), path.clone());
        let device = claim_device(spec, facts, node, cluster, default_reserve)?;
        // a new device where this node had another is a replacement, and the old one is gone
        if let Some(id) = known_id.filter(|id| *id != device.id) {
            let reason = format!(
                "its path {} was found empty and claimed as the new device {}",
                path.display(),
                device.id
            );
            event!(Level::WARN, device = %path.display(), id = %id, msg = %reason);
            claimed.failed.push(FailedDevice {
                path: path.clone(),
                id,
                class: spec.class.clone(),
                reason,
            });
        }
        claimed.devices.push(device);
    }
    // a device the configuration no longer names but this node had claimed stays known, failed
    for (path, id) in known {
        if specs.iter().all(|spec| &spec.path != path) {
            claimed.failed.push(FailedDevice {
                path: path.clone(),
                id: *id,
                class: String::new(),
                reason: "its configuration no longer names it; it stays on the pool map until an \
                         operator removes it"
                    .to_string(),
            });
        }
    }
    Ok(claimed)
}

impl Claimed {
    /// Every device this node knows, claimed or failed, by the path it was claimed at
    ///
    /// A replaced device's path names the new device; the old id is reported failed and is
    /// no longer remembered, since nothing will ever claim it again.
    #[must_use]
    pub fn known(&self) -> BTreeMap<PathBuf, DeviceId> {
        let mut known: BTreeMap<PathBuf, DeviceId> = self
            .failed
            .iter()
            .map(|failed| (failed.path.clone(), failed.id))
            .collect();
        for device in &self.devices {
            known.insert(device.spec.path.clone(), device.id);
        }
        known
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{claim, DEVICE_MARKER};
    use crate::server::devices::probe::{set_override, ProbeOverride};
    use crate::server::errors::ShoalError;
    use crate::server::ServerError;
    use crate::shared::devices::DeviceSpec;
    use crate::shared::identity::NodeId;

    /// The probe's override is process-wide, so the tests that set it run one at a time
    static OVERRIDDEN: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Hold the override's lock and stand a measured filesystem in for the test directory's
    fn on_xfs() -> std::sync::MutexGuard<'static, ()> {
        let held = OVERRIDDEN
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        stand_in_xfs();
        held
    }

    /// Stand a measured filesystem in, two devices allowed on one
    fn stand_in_xfs() {
        set_override(Some(ProbeOverride {
            filesystem: Some("xfs".to_string()),
            shared_filesystem: true,
            ..ProbeOverride::default()
        }));
    }

    /// An empty directory is claimed new, and claimed again as itself
    #[test]
    fn an_empty_device_is_claimed_and_kept() {
        let _held = on_xfs();
        let dir = tempfile::tempdir().expect("a directory");
        let node = NodeId::mint();
        let spec = DeviceSpec::new(dir.path(), "ssd").slices(2);
        let first = claim(std::slice::from_ref(&spec), &[], node, None, &BTreeMap::new(), 1 << 30).expect("claimed");
        assert!(first.devices[0].fresh);
        assert_eq!(first.devices[0].slices.len(), 2);
        let id = first.devices[0].id;
        let slices: Vec<_> = first.devices[0].slices.iter().map(|slice| slice.id).collect();
        let known = first.known();
        drop(first);
        let again = claim(&[spec], &[], node, None, &known, 1 << 30).expect("claimed");
        assert!(!again.devices[0].fresh);
        assert_eq!(again.devices[0].id, id);
        let again_slices: Vec<_> = again.devices[0].slices.iter().map(|slice| slice.id).collect();
        assert_eq!(again_slices, slices);
        assert!(again.failed.is_empty());
    }

    /// A device another node claimed is refused by name, as is a held one
    #[test]
    fn another_nodes_device_is_refused() {
        let _held = on_xfs();
        let dir = tempfile::tempdir().expect("a directory");
        let spec = DeviceSpec::new(dir.path(), "ssd");
        let held = claim(std::slice::from_ref(&spec), &[], NodeId::mint(), None, &BTreeMap::new(), 0).expect("claimed");
        // a second claim while the first holds the lock
        let error = claim(std::slice::from_ref(&spec), &[], NodeId::mint(), None, &BTreeMap::new(), 0)
            .expect_err("another node's device");
        assert!(error.to_string().contains("was claimed by node"), "{error}");
        drop(held);
        assert!(dir.path().join(DEVICE_MARKER).exists());
    }

    /// A device's path is never created, and a known one that is missing is failed, not refused
    #[test]
    fn a_missing_device_fails_under_its_id() {
        let _held = on_xfs();
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("not-mounted");
        let spec = DeviceSpec::new(&path, "ssd");
        let error = claim(std::slice::from_ref(&spec), &[], NodeId::mint(), None, &BTreeMap::new(), 0)
            .expect_err("a never claimed path that is missing");
        assert!(matches!(error, ServerError::Shoal(ShoalError::DeviceRefused { .. })), "{error}");
        assert!(!path.exists(), "the claim created a device's path");
        let id = crate::shared::devices::DeviceId::mint();
        let known = BTreeMap::from([(path.clone(), id)]);
        let claimed = claim(&[spec], &[], NodeId::mint(), None, &known, 0).expect("the node starts");
        assert!(claimed.devices.is_empty());
        assert_eq!(claimed.failed[0].id, id);
    }

    /// A device in a root, or two on one filesystem, are refused
    #[test]
    fn nested_and_shared_devices_are_refused() {
        let _held = on_xfs();
        let dir = tempfile::tempdir().expect("a directory");
        let inner = dir.path().join("inner");
        std::fs::create_dir_all(&inner).expect("made");
        let error = claim(
            &[DeviceSpec::new(&inner, "ssd")],
            &[dir.path().to_path_buf()],
            NodeId::mint(),
            None,
            &BTreeMap::new(),
            0,
        )
        .expect_err("a device in a root");
        assert!(error.to_string().contains("storage root"), "{error}");
        // without the test's allowance, two directories on one filesystem are one disk
        set_override(Some(ProbeOverride {
            filesystem: Some("xfs".to_string()),
            ..ProbeOverride::default()
        }));
        let a = tempfile::tempdir().expect("a directory");
        let b = tempfile::tempdir().expect("a directory");
        let error = claim(
            &[DeviceSpec::new(a.path(), "ssd"), DeviceSpec::new(b.path(), "ssd")],
            &[],
            NodeId::mint(),
            None,
            &BTreeMap::new(),
            0,
        )
        .expect_err("two devices on one filesystem");
        assert!(error.to_string().contains("two failure domains"), "{error}");
        stand_in_xfs();
    }
}
