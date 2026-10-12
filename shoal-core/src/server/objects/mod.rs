//! Object work on a node: its executors, its budget and the device store its slices are kept in
//!
//! Object work runs on executors of its own, each owning some of the node's device slices, and
//! never on a table shard's ([S13](../../../../docs/src/object-storage/isolation.md),
//! [F85](../../../../docs/src/features/devices-and-pools.md)). No client's byte reaches a device
//! before M15: the store is driven by tests and by the lab's harness through [`ObjectStore`].

pub mod budget;
pub mod cpus;
pub mod executor;
pub mod store;

use std::collections::BTreeSet;

pub use executor::{ObjectPlan, ObjectStore, Owner};

use crate::server::conf::{Conf, OBJECT_IO_MEMORY};
use crate::server::devices::NodeDevices;
use store::SliceOptions;

/// What a node's object executors are to be, from its configuration and what it claimed
///
/// Each slice keeps files written ahead for the chunk lengths of the pools its device's class
/// serves, as this node's file defines them; a length a stage asks for is served on demand.
///
/// # Arguments
///
/// * `conf` - The node's configuration
/// * `devices` - What it claimed
/// * `cpus` - Where its object executors run
#[must_use]
pub fn plan(conf: &Conf, devices: &NodeDevices, cpus: &[cpus::ObjectCpu]) -> ObjectPlan {
    let slices = devices
        .devices
        .iter()
        .flat_map(|device| {
            // the chunk lengths of every pool of this device's class
            let lengths: BTreeSet<u64> = conf
                .storage
                .pools
                .values()
                .filter(|pool| pool.class == device.spec.class)
                .map(crate::shared::pools::PoolSpec::chunk_bytes)
                .collect();
            device.slices.iter().map(move |slice| {
                (
                    device.clone(),
                    slice.clone(),
                    SliceOptions {
                        segment_bytes: device.spec.segment_bytes(),
                        free_files: device.spec.free_files_kept(),
                        chunk_lengths: lengths.iter().copied().collect(),
                        budget: None,
                    },
                )
            })
        })
        .collect();
    ObjectPlan {
        cpus: cpus.to_vec(),
        budget: conf.resources.object_budget() as u64,
        io_memory: OBJECT_IO_MEMORY,
        slices,
    }
}
