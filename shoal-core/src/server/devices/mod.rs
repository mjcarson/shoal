//! A node's devices: claimed at its start, reported once a tick, served by its object executors
//!
//! A node claims every device its configuration names before any executor starts, as it claims
//! its storage roots ([`claim`]); what it claimed is held here, shared by the control thread,
//! which reports each device's figures and commits its identity, and by the object executors,
//! which own the slices and mark a device failed when it returns an error
//! ([S4](../../../../docs/src/object-storage/pools-and-devices.md),
//! [F85](../../../../docs/src/features/devices-and-pools.md)).
//!
//! # Invariants
//!
//! **A failing device stops nothing else** (P17): an error marks it and every slice on it failed,
//! it is reported, and every later operation on it is refused by name; the node, its tables and
//! its other devices go on ([S13](../../../../docs/src/object-storage/isolation.md#a-failing-pool-device-does-not-stop-its-node)).
//!
//! **An executor number is never reported.** Which executor owns a slice is the node's own
//! business; a report names devices and slices.

pub mod claim;
pub mod probe;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::shared::devices::{
    ApplySync, DeviceFacts, DeviceHealth, DeviceId, DeviceReport, DeviceSpec, SliceId,
    SliceReport,
};

pub use claim::{Claimed, FailedDevice};
pub use probe::Facts;

/// One slice a node serves, and the figures its owning executor keeps
#[derive(Debug)]
pub struct SliceState {
    /// Its id
    pub id: SliceId,
    /// Its index on its device
    pub index: u16,
    /// Its directory
    pub path: PathBuf,
    /// The stripe chunks it holds
    pub chunks: AtomicU64,
    /// The bytes staged on it and not yet applied or discarded
    pub staged_bytes: AtomicU64,
    /// Every cpu an operation on it has run on: its owner's alone, by S13's rule
    pub served_on: Mutex<std::collections::BTreeSet<usize>>,
}

/// One device a node serves
#[derive(Debug)]
pub struct DeviceState {
    /// Its id
    pub id: DeviceId,
    /// What the configuration named
    pub spec: DeviceSpec,
    /// What it is on, as the probe found it at the claim
    pub facts: Facts,
    /// The free bytes it keeps
    pub reserve: u64,
    /// How its applies are made durable
    pub apply_sync: ApplySync,
    /// Its slices, by index
    pub slices: Vec<Arc<SliceState>>,
    /// What an operator is warned of about it
    pub warnings: Vec<String>,
    /// Why it failed, once it has
    failed: Mutex<Option<String>>,
}

impl DeviceState {
    /// A device and its slices as a node serves them, for a claim, a test or a harness
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `spec` - What the configuration named
    /// * `facts` - What it is on
    /// * `reserve` - The free bytes it keeps
    /// * `apply_sync` - How its applies are made durable
    /// * `slices` - Its slices' ids and directories, by index
    /// * `warnings` - What an operator is warned of about it
    #[must_use]
    pub fn new(
        id: DeviceId,
        spec: DeviceSpec,
        facts: Facts,
        reserve: u64,
        apply_sync: ApplySync,
        slices: Vec<(SliceId, PathBuf)>,
        warnings: Vec<String>,
    ) -> Self {
        DeviceState {
            id,
            spec,
            facts,
            reserve,
            apply_sync,
            slices: slices
                .into_iter()
                .enumerate()
                .map(|(index, (id, path))| {
                    Arc::new(SliceState {
                        id,
                        index: u16::try_from(index).unwrap_or(u16::MAX),
                        path,
                        chunks: AtomicU64::new(0),
                        staged_bytes: AtomicU64::new(0),
                        served_on: Mutex::new(std::collections::BTreeSet::new()),
                    })
                })
                .collect(),
            warnings,
            failed: Mutex::new(None),
        }
    }

    /// Where it is mounted
    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.spec.path
    }

    /// Mark it failed, and every slice on it with it; the first reason is kept
    ///
    /// # Arguments
    ///
    /// * `reason` - What failed
    pub fn fail(&self, reason: impl Into<String>) {
        let mut failed = self.failed.lock().unwrap_or_else(PoisonError::into_inner);
        if failed.is_none() {
            let reason = reason.into();
            tracing::event!(
                tracing::Level::ERROR,
                device = %self.spec.path.display(),
                id = %self.id,
                msg = "a pool device failed; its slices are failed with it and the node goes on",
                reason = %reason,
            );
            *failed = Some(reason);
        }
    }

    /// Why it failed, if it has
    #[must_use]
    pub fn failed(&self) -> Option<String> {
        self.failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Its figures now, for a report
    #[must_use]
    pub fn report(&self) -> DeviceReport {
        // a device is measured as a root is
        let (size_bytes, free_bytes) =
            crate::server::control::capacity::size_and_free(&self.spec.path).unwrap_or((0, 0));
        DeviceReport {
            id: self.id,
            path: self.spec.path.to_string_lossy().into_owned(),
            class: self.spec.class.clone(),
            slices: self
                .slices
                .iter()
                .map(|slice| SliceReport {
                    id: slice.id,
                    index: slice.index,
                    chunks: slice.chunks.load(Ordering::Relaxed),
                    staged_bytes: slice.staged_bytes.load(Ordering::Relaxed),
                })
                .collect(),
            size_bytes,
            free_bytes,
            reserve_bytes: self.reserve,
            device: self.facts.device.clone(),
            filesystem: self.facts.filesystem.clone(),
            rotational: self.facts.rotational,
            warnings: self.warnings.clone(),
            health: match self.failed() {
                Some(reason) => DeviceHealth::Failed { reason },
                None => DeviceHealth::Serving,
            },
        }
    }

    /// Its identity, for the pool map
    #[must_use]
    pub fn facts(&self) -> DeviceFacts {
        DeviceFacts {
            id: self.id,
            class: self.spec.class.clone(),
            slices: self.slices.iter().map(|slice| slice.id).collect(),
            size_bytes: self.facts.size_bytes,
            weight: self.spec.weight.map(|weight| weight.get()),
        }
    }
}

/// Every device a node claimed, the ones it serves and the ones it knows have failed
#[derive(Debug, Default)]
pub struct NodeDevices {
    /// The devices it serves
    pub devices: Vec<Arc<DeviceState>>,
    /// The devices it knows by an id and cannot serve
    pub missing: Vec<FailedDevice>,
    /// The claims, whose locks are held for as long as this is
    _claims: Option<Claimed>,
}

impl NodeDevices {
    /// Hold what a node claimed
    ///
    /// # Arguments
    ///
    /// * `claimed` - What the claim found
    #[must_use]
    pub fn new(claimed: Claimed) -> Self {
        let devices = claimed
            .devices
            .iter()
            .map(|device| {
                Arc::new(DeviceState::new(
                    device.id,
                    device.spec.clone(),
                    device.facts.clone(),
                    device.reserve,
                    device.apply_sync,
                    device
                        .slices
                        .iter()
                        .map(|slice| (slice.id, slice.path.clone()))
                        .collect(),
                    device.warnings.clone(),
                ))
            })
            .collect();
        NodeDevices {
            devices,
            missing: claimed.failed.clone(),
            _claims: Some(claimed),
        }
    }

    /// Whether the node has no device, served or failed
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.devices.is_empty() && self.missing.is_empty()
    }

    /// Every device's figures, the failed ones it cannot serve among them
    #[must_use]
    pub fn reports(&self) -> Vec<DeviceReport> {
        let mut reports: Vec<DeviceReport> = self.devices.iter().map(|device| device.report()).collect();
        for missing in &self.missing {
            reports.push(DeviceReport {
                id: missing.id,
                path: missing.path.to_string_lossy().into_owned(),
                class: missing.class.clone(),
                health: DeviceHealth::Failed {
                    reason: missing.reason.clone(),
                },
                ..DeviceReport::default()
            });
        }
        reports
    }

    /// The identity of every device it serves, for the pool map
    #[must_use]
    pub fn facts(&self) -> Vec<DeviceFacts> {
        self.devices.iter().map(|device| device.facts()).collect()
    }

    /// Every device it knows to have failed, and why
    #[must_use]
    pub fn failed(&self) -> BTreeMap<DeviceId, String> {
        let mut failed: BTreeMap<DeviceId, String> = self
            .devices
            .iter()
            .filter_map(|device| device.failed().map(|reason| (device.id, reason)))
            .collect();
        for missing in &self.missing {
            failed.insert(missing.id, missing.reason.clone());
        }
        failed
    }

    /// A slice it serves, and its device
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice's id
    #[must_use]
    pub fn slice(&self, slice: SliceId) -> Option<(Arc<DeviceState>, Arc<SliceState>)> {
        self.devices.iter().find_map(|device| {
            device
                .slices
                .iter()
                .find(|candidate| candidate.id == slice)
                .map(|found| (device.clone(), found.clone()))
        })
    }
}

/// The file a standalone node keeps its pool map in, at its primary root
///
/// A cluster's pool map is the control group's; a standalone node has none, so it keeps its own,
/// whose generations survive a restart as a cluster's do in its log.
pub const POOL_MAP_FILE: &str = "shoal-poolmap.json";

/// A standalone node's pool map, with what it claimed at this start recorded in it
///
/// Read from the primary root, its devices observed as a control group would observe them, and
/// written back, staged and renamed, only when something changed.
///
/// # Arguments
///
/// * `root` - The primary root
/// * `node` - This node
/// * `devices` - What it claimed
///
/// # Errors
///
/// A pool map that does not read, a device the map holds on another node, or a file that cannot
/// be written.
pub fn standalone_pool_map(
    root: &std::path::Path,
    node: crate::shared::identity::NodeId,
    devices: &NodeDevices,
) -> Result<crate::shared::poolmap::PoolMap, crate::server::ServerError> {
    use crate::server::errors::ShoalError;
    use crate::server::ServerError;
    use std::io::Write as _;
    let path = root.join(POOL_MAP_FILE);
    // the map as the last start left it, or none
    let mut map: crate::shared::poolmap::PoolMap = match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Default::default(),
        Err(error) => return Err(ServerError::IO(error)),
    };
    if devices.is_empty() && map.is_empty() {
        return Ok(map);
    }
    let before = map.clone();
    // a standalone node stands on its own machine
    let domains = BTreeMap::from([(
        crate::shared::domains::HOST.to_string(),
        crate::server::control::stats::hostname().unwrap_or_else(|| "localhost".to_string()),
    )]);
    map.observe(node, &domains, &devices.facts())
        .map_err(|error| ServerError::Shoal(ShoalError::InvalidConfig(error.to_string())))?;
    map.refresh_domains(node, &domains);
    map.set_failed(node, &devices.failed());
    if map != before {
        let staged = root.join(format!("{POOL_MAP_FILE}.tmp"));
        let mut file = std::fs::File::create(&staged)?;
        file.write_all(&serde_json::to_vec_pretty(&map)?)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&staged, &path)?;
        std::fs::File::open(root)?.sync_all()?;
    }
    Ok(map)
}
