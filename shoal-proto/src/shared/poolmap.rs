//! The pool map: every device a pool can place on, and the history placement is computed from
//!
//! The control group commits it, and every node derives it from the committed commands as it
//! derives the tablet map; a standalone node keeps its own
//! ([S5](../../../docs/src/object-storage/placement.md#the-pool-map)). It says where stripe chunks
//! **should** be - each device with its seat, its slices, its weight and the failure domains it
//! stands in - and never what is current, which is a stripe row's label (P17).
//!
//! # Invariants
//!
//! **A generation is computable for as long as it is kept.** Everything placement reads of a
//! device - its class, its domains, its slices and its weight - is held as an [`Era`] with the
//! generation it took effect at and the one it ended at, and fitted weights and exceptions carry
//! theirs, so [`View`](super::pool_placement::View) at any generation sees the map as it was then.
//!
//! **The generation moves when where a placement group belongs could change**, and only then: a
//! device joining or leaving, a reweight, or a change of its class, its domains or its slices. A
//! device or member going down, or a device failing, moves nothing; its chunks are missed, and
//! rebuilt elsewhere only by a move ([S10](../../../docs/src/object-storage/recovery.md#moves)).
//!
//! **Every weight is an integer.** The map crosses the control group as JSON, and a float that
//! did not round-trip to the bit would make two nodes draw differently at a near tie.
//!
//! **A seat is not an id.** It is minted with the device, from its id, and passes only to a device
//! named as its replacement, which is then drawn for exactly its predecessor's groups.
//!
//! **Nothing for a stripe or an object is here** (P18): devices, slices, eras, fitted weights and
//! exceptions grow with devices and moves, never with data.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::shared::devices::{DeviceFacts, DeviceId, SliceId};
use crate::shared::identity::NodeId;
use crate::shared::rendezvous::mix64;

/// A slice as placement names it: its device and its place on the device
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SliceRef {
    /// The device it is on
    pub device: DeviceId,
    /// Its index on the device, `slice-<index>`
    pub index: u16,
}

impl std::fmt::Display for SliceRef {
    /// Write the slice as its device and index
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/slice-{}", self.device, self.index)
    }
}

/// What placement reads of a device, from one generation until another
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Era {
    /// The generation it took effect at
    pub from: u64,
    /// The generation it ended at, if it has; it holds for every generation before
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<u64>,
    /// The class its operator wrote
    pub class: String,
    /// Its member's failure domains, by level
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub domains: BTreeMap<String, String>,
    /// Its slices' ids, by index
    pub slices: Vec<SliceId>,
    /// The share of a pool it takes: its operator's weight, or its size when it was claimed
    pub weight: u64,
    /// Whether its disk's sync flushes a volatile write cache; it places nothing, so a change of it
    /// is recorded in the era in force and moves no generation ([F86](../../../docs/src/features/replicated-pools.md))
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub write_back: bool,
}

impl Era {
    /// Whether it holds at a generation
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation
    #[must_use]
    pub fn holds_at(&self, generation: u64) -> bool {
        self.from <= generation && self.until.is_none_or(|until| generation < until)
    }

    /// Whether another era would place exactly as this one does
    ///
    /// # Arguments
    ///
    /// * `other` - The other era
    #[must_use]
    pub fn places_as(&self, other: &Era) -> bool {
        self.class == other.class
            && self.domains == other.domains
            && self.slices == other.slices
            && self.weight == other.weight
    }
}

/// A device on the pool map
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRecord {
    /// Its id
    pub id: DeviceId,
    /// The node it is on; a device never moves between nodes
    pub node: NodeId,
    /// The key it is drawn by, which a replacement takes over
    pub seat: u64,
    /// Why its node reports it failed, if it does; placement still names it, and its chunks are
    /// missed until a move or a rebuild puts them elsewhere
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed: Option<String>,
    /// What placement reads of it, oldest first
    pub eras: Vec<Era>,
}

impl DeviceRecord {
    /// The era that holds at a generation, if the device was on the map then
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation
    #[must_use]
    pub fn era_at(&self, generation: u64) -> Option<&Era> {
        self.eras.iter().find(|era| era.holds_at(generation))
    }

    /// The era that holds now, if the device has not left the map
    #[must_use]
    pub fn current(&self) -> Option<&Era> {
        self.eras.last().filter(|era| era.until.is_none())
    }

    /// A slice of it by index, at a generation
    ///
    /// # Arguments
    ///
    /// * `index` - The slice's index
    /// * `generation` - The generation
    #[must_use]
    pub fn slice_at(&self, index: u16, generation: u64) -> Option<SliceId> {
        self.era_at(generation)
            .and_then(|era| era.slices.get(usize::from(index)).copied())
    }
}

/// Weights the planner fitted for one pool, from one generation until another
///
/// Drawing several devices of unequal weight includes the heavy ones less than in proportion, and
/// these correct it ([X2](../../../docs/src/object-storage/placement-simulation.md#exceptions-and-fitted-weights)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FittedEra {
    /// The generation they took effect at
    pub from: u64,
    /// The generation they ended at, if they have
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<u64>,
    /// The placement weight of each device the fit covers; a device it does not name draws by
    /// its own weight
    pub weights: BTreeMap<DeviceId, u64>,
}

impl FittedEra {
    /// Whether these weights hold at a generation
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation
    #[must_use]
    pub fn holds_at(&self, generation: u64) -> bool {
        self.from <= generation && self.until.is_none_or(|until| generation < until)
    }
}

/// A placement group, by its tablet and its place among the tablet's groups
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct PlacementGroup {
    /// The tablet whose keys it is a sub-range of
    pub tablet: u16,
    /// Which of the tablet's groups it is
    pub sub: u16,
}

/// A slice for one placement group where the rule names another: the planner's correction
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exception {
    /// The pool
    pub pool: String,
    /// The consumer whose placement group it is
    pub consumer: u64,
    /// The placement group
    pub pg: PlacementGroup,
    /// The slice the rule names
    pub rule: SliceRef,
    /// The slice used instead
    pub instead: SliceRef,
    /// The generation it took effect at
    pub from: u64,
    /// The generation it ended at, if it has
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<u64>,
}

impl Exception {
    /// Whether it holds at a generation
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation
    #[must_use]
    pub fn holds_at(&self, generation: u64) -> bool {
        self.from <= generation && self.until.is_none_or(|until| generation < until)
    }
}

/// Every device a pool can place on, and what placement has read of them
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PoolMap {
    /// The generation now: every change that could move a placement group moves it
    #[serde(default)]
    pub generation: u64,
    /// Every device ever on the map, by id
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub devices: BTreeMap<DeviceId, DeviceRecord>,
    /// Each pool's fitted weights, oldest first
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fitted: BTreeMap<String, Vec<FittedEra>>,
    /// Every exception, in the order they were made
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exceptions: Vec<Exception>,
    /// The generation each pool places its stripes at, fixed the first time it was ready
    ///
    /// Until M16 moves a placement group from one generation's slices to another's, a pool that
    /// holds stripes cannot follow the map: every write and read places at its pin, through the
    /// eras that keep the map's past, and a device that joined since is not placed on by it
    /// ([F86](../../../docs/src/features/replicated-pools.md)). The pin never moves the
    /// generation: it says where stripes are, not where they should be.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub pinned: BTreeMap<String, u64>,
}

/// The seat a new device is drawn by: a mix of its id, so it is fixed by the id alone
///
/// # Arguments
///
/// * `id` - The device's id
#[must_use]
pub fn seat_of(id: DeviceId) -> u64 {
    // both halves of the id, each mixed, so no two ids share a seat by accident
    let bits = id.0.as_u128();
    #[allow(clippy::cast_possible_truncation)]
    let (high, low) = ((bits >> 64) as u64, bits as u64);
    mix64(low ^ mix64(high))
}

/// Why the pool map refused a change
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolMapError {
    /// A device another node already holds
    WrongNode {
        /// The device
        device: DeviceId,
        /// The node the map holds it on
        holder: NodeId,
        /// The node that claimed it
        claimed: NodeId,
    },
    /// A device that is not on the map
    Unknown {
        /// The device
        device: DeviceId,
    },
    /// A seat that a device on the map still holds
    SeatHeld {
        /// The device holding it
        holder: DeviceId,
    },
    /// A pin at a generation the map has not reached
    UnknownGeneration {
        /// The pool
        pool: String,
        /// The generation it named
        generation: u64,
        /// The map's generation
        now: u64,
    },
}

impl std::fmt::Display for PoolMapError {
    /// Write the refusal for a person
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolMapError::WrongNode {
                device,
                holder,
                claimed,
            } => write!(
                f,
                "device {device} is on node {holder}, and node {claimed} claimed it; a device does \
                 not move between nodes"
            ),
            PoolMapError::Unknown { device } => write!(f, "device {device} is not on the pool map"),
            PoolMapError::SeatHeld { holder } => write!(
                f,
                "the seat is still held by device {holder}, which has not left the pool map"
            ),
            PoolMapError::UnknownGeneration {
                pool,
                generation,
                now,
            } => write!(
                f,
                "pool {pool} cannot be pinned at generation {generation}; the map is at {now}"
            ),
        }
    }
}

impl std::error::Error for PoolMapError {}

impl PoolMap {
    /// Whether no device has ever been on the map
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.devices.is_empty()
    }

    /// The devices of one node, by id
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    pub fn devices_of(&self, node: NodeId) -> impl Iterator<Item = &DeviceRecord> {
        self.devices.values().filter(move |device| device.node == node)
    }

    /// Record what a node claimed, moving the generation once if any of it places differently
    ///
    /// A device the map has not seen joins under a seat minted from its id. One whose class,
    /// slices or weight changed, or whose member's domains did, ends its era and begins another.
    /// A device the node did not name stays as it was: it may be missing or emptied, which its
    /// node reports as failed, and only an operator takes a device off the map (M20). Returns
    /// whether the generation moved.
    ///
    /// # Arguments
    ///
    /// * `node` - The node that claimed them
    /// * `domains` - Its member's failure domains, by level
    /// * `facts` - What it claimed
    ///
    /// # Errors
    ///
    /// A device another node holds; nothing is recorded.
    pub fn observe(
        &mut self,
        node: NodeId,
        domains: &BTreeMap<String, String>,
        facts: &[DeviceFacts],
    ) -> Result<bool, PoolMapError> {
        // a device never moves between nodes, so refuse before recording anything
        for fact in facts {
            if let Some(device) = self.devices.get(&fact.id) {
                if device.node != node {
                    return Err(PoolMapError::WrongNode {
                        device: fact.id,
                        holder: device.node,
                        claimed: node,
                    });
                }
            }
        }
        // the era each device would have now, against the one it has
        let next = self.generation + 1;
        let mut moved = false;
        for fact in facts {
            let era = Era {
                from: next,
                until: None,
                class: fact.class.clone(),
                domains: domains.clone(),
                slices: fact.slices.clone(),
                weight: fact.capacity_weight(),
                write_back: fact.write_back,
            };
            match self.devices.get_mut(&fact.id) {
                // a device placing exactly as it did changes nothing but what places nothing
                Some(device) if device.current().is_some_and(|current| current.places_as(&era)) => {
                    if let Some(current) = device.eras.last_mut().filter(|era| era.until.is_none()) {
                        current.write_back = fact.write_back;
                    }
                }
                // one that places otherwise, or that had left and is back, begins an era
                Some(device) => {
                    if let Some(current) = device.eras.last_mut().filter(|era| era.until.is_none()) {
                        current.until = Some(next);
                    }
                    device.eras.push(era);
                    moved = true;
                }
                // and a new one joins under its own seat
                None => {
                    self.devices.insert(
                        fact.id,
                        DeviceRecord {
                            id: fact.id,
                            node,
                            seat: seat_of(fact.id),
                            failed: None,
                            eras: vec![era],
                        },
                    );
                    moved = true;
                }
            }
        }
        if moved {
            self.generation = next;
        }
        Ok(moved)
    }

    /// Give every device of a node its member's domains, moving the generation once if any changed
    ///
    /// A member observed under another host, rack or zone moves where its devices' chunks belong.
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `domains` - Its member's failure domains, by level
    pub fn refresh_domains(&mut self, node: NodeId, domains: &BTreeMap<String, String>) -> bool {
        let next = self.generation + 1;
        let mut moved = false;
        for device in self.devices.values_mut().filter(|device| device.node == node) {
            let Some(current) = device.current() else {
                continue;
            };
            if &current.domains == domains {
                continue;
            }
            // end the era and begin one under the new domains
            let mut era = current.clone();
            era.from = next;
            era.domains = domains.clone();
            if let Some(last) = device.eras.last_mut() {
                last.until = Some(next);
            }
            device.eras.push(era);
            moved = true;
        }
        if moved {
            self.generation = next;
        }
        moved
    }

    /// Take every device of a node off the map, as its member leaves the cluster
    ///
    /// Returns whether the generation moved.
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    pub fn retire_node(&mut self, node: NodeId) -> bool {
        let next = self.generation + 1;
        let mut moved = false;
        for device in self.devices.values_mut().filter(|device| device.node == node) {
            if let Some(last) = device.eras.last_mut().filter(|era| era.until.is_none()) {
                last.until = Some(next);
                moved = true;
            }
        }
        if moved {
            self.generation = next;
        }
        moved
    }

    /// Record which of a node's devices it reports failed, and why; moves no generation
    ///
    /// Returns whether anything changed.
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `failed` - Each of its failed devices and why
    pub fn set_failed(&mut self, node: NodeId, failed: &BTreeMap<DeviceId, String>) -> bool {
        let mut changed = false;
        for device in self.devices.values_mut().filter(|device| device.node == node) {
            let now = failed.get(&device.id).cloned();
            if device.failed != now {
                device.failed = now;
                changed = true;
            }
        }
        changed
    }

    /// Put a new device in a departed device's seat, so it is drawn for exactly its groups
    ///
    /// The predecessor leaves the map and the replacement joins it under the predecessor's seat,
    /// in one generation ([X2](../../../docs/src/object-storage/placement-simulation.md#a-replaced-device)).
    /// The replacement is still a new device that holds nothing; naming one is M20's operation.
    ///
    /// # Arguments
    ///
    /// * `predecessor` - The device whose seat is taken over
    /// * `node` - The replacement's node
    /// * `domains` - Its member's failure domains
    /// * `replacement` - What the replacement's node claimed
    ///
    /// # Errors
    ///
    /// A predecessor not on the map, or a replacement already on it.
    pub fn replace_in_seat(
        &mut self,
        predecessor: DeviceId,
        node: NodeId,
        domains: &BTreeMap<String, String>,
        replacement: &DeviceFacts,
    ) -> Result<(), PoolMapError> {
        let next = self.generation + 1;
        let seat = self
            .devices
            .get(&predecessor)
            .map(|device| device.seat)
            .ok_or(PoolMapError::Unknown {
                device: predecessor,
            })?;
        if let Some(holder) = self.devices.get(&replacement.id) {
            return Err(PoolMapError::SeatHeld { holder: holder.id });
        }
        // the predecessor leaves, and its seat is the replacement's
        if let Some(last) = self
            .devices
            .get_mut(&predecessor)
            .and_then(|device| device.eras.last_mut())
            .filter(|era| era.until.is_none())
        {
            last.until = Some(next);
        }
        self.devices.insert(
            replacement.id,
            DeviceRecord {
                id: replacement.id,
                node,
                seat,
                failed: None,
                eras: vec![Era {
                    from: next,
                    until: None,
                    class: replacement.class.clone(),
                    domains: domains.clone(),
                    slices: replacement.slices.clone(),
                    weight: replacement.capacity_weight(),
                    write_back: replacement.write_back,
                }],
            },
        );
        self.generation = next;
        Ok(())
    }

    /// Set a pool's fitted weights, moving the generation if they differ from those in force
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `weights` - Each device's placement weight
    pub fn set_fitted(&mut self, pool: &str, weights: BTreeMap<DeviceId, u64>) -> bool {
        let next = self.generation + 1;
        let eras = self.fitted.entry(pool.to_string()).or_default();
        // the same weights as those in force change nothing
        if eras
            .last()
            .is_some_and(|era| era.until.is_none() && era.weights == weights)
        {
            return false;
        }
        if let Some(last) = eras.last_mut().filter(|era| era.until.is_none()) {
            last.until = Some(next);
        }
        eras.push(FittedEra {
            from: next,
            until: None,
            weights,
        });
        self.generation = next;
        true
    }

    /// Replace one consumer's exceptions in a pool, moving the generation if they differ
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `consumer` - The consumer
    /// * `wanted` - Each placement group's slice the rule names and the slice used instead
    pub fn set_exceptions(
        &mut self,
        pool: &str,
        consumer: u64,
        wanted: &[(PlacementGroup, SliceRef, SliceRef)],
    ) -> bool {
        let next = self.generation + 1;
        // the exceptions in force for this pool and consumer, as the same triples
        let mut held: Vec<(PlacementGroup, SliceRef, SliceRef)> = self
            .exceptions
            .iter()
            .filter(|exception| {
                exception.pool == pool && exception.consumer == consumer && exception.until.is_none()
            })
            .map(|exception| (exception.pg, exception.rule, exception.instead))
            .collect();
        let mut sorted = wanted.to_vec();
        held.sort();
        sorted.sort();
        if held == sorted {
            return false;
        }
        // end every one in force and begin the wanted ones
        for exception in self.exceptions.iter_mut().filter(|exception| {
            exception.pool == pool && exception.consumer == consumer && exception.until.is_none()
        }) {
            exception.until = Some(next);
        }
        for (pg, rule, instead) in sorted {
            self.exceptions.push(Exception {
                pool: pool.to_string(),
                consumer,
                pg,
                rule,
                instead,
                from: next,
                until: None,
            });
        }
        self.generation = next;
        true
    }

    /// Pin a pool's placement at a generation, if it is not pinned yet
    ///
    /// Returns whether the pin was taken; a pool already pinned keeps its pin.
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `generation` - The generation it places at from now on
    ///
    /// # Errors
    ///
    /// A generation past the map's, which no view can be computed at.
    pub fn pin(&mut self, pool: &str, generation: u64) -> Result<bool, PoolMapError> {
        // a pin names a generation the map has been at
        if generation > self.generation {
            return Err(PoolMapError::UnknownGeneration {
                pool: pool.to_string(),
                generation,
                now: self.generation,
            });
        }
        // a pool's pin is taken once and kept
        if self.pinned.contains_key(pool) {
            return Ok(false);
        }
        self.pinned.insert(pool.to_string(), generation);
        Ok(true)
    }

    /// The generation a pool places at, if it is pinned
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    #[must_use]
    pub fn pinned_at(&self, pool: &str) -> Option<u64> {
        self.pinned.get(pool).copied()
    }

    /// Each pool's fitted weights at a generation
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `generation` - The generation
    #[must_use]
    pub fn fitted_at(&self, pool: &str, generation: u64) -> Option<&BTreeMap<DeviceId, u64>> {
        self.fitted
            .get(pool)?
            .iter()
            .find(|era| era.holds_at(generation))
            .map(|era| &era.weights)
    }

    /// Whether a slice is on the map now, and which device and index it is
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice's id
    #[must_use]
    pub fn find_slice(&self, slice: SliceId) -> Option<SliceRef> {
        self.devices.values().find_map(|device| {
            let current = device.current()?;
            let index = current.slices.iter().position(|id| *id == slice)?;
            u16::try_from(index).ok().map(|index| SliceRef {
                device: device.id,
                index,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{seat_of, PoolMap, PoolMapError};
    use crate::shared::devices::{DeviceFacts, DeviceId, SliceId};
    use crate::shared::identity::NodeId;

    /// A device's facts, with ids made from small numbers
    fn facts(id: u128, class: &str, slices: u16, size: u64) -> DeviceFacts {
        DeviceFacts {
            id: DeviceId(uuid::Uuid::from_u128(id)),
            class: class.to_string(),
            slices: (0..slices)
                .map(|index| SliceId(uuid::Uuid::from_u128(id << 8 | u128::from(index))))
                .collect(),
            size_bytes: size,
            weight: None,
            write_back: false,
        }
    }

    /// A host's domains
    fn host(name: &str) -> BTreeMap<String, String> {
        BTreeMap::from([("host".to_string(), name.to_string())])
    }

    /// Observing moves the generation once a change, and never for nothing new
    #[test]
    fn the_generation_moves_once_a_change_and_never_for_nothing() {
        let mut map = PoolMap::default();
        let node = NodeId::from(1);
        // two devices join at one generation, under seats of their ids
        let first = [facts(1, "ssd", 1, 100), facts(2, "ssd", 2, 200)];
        assert!(map.observe(node, &host("a"), &first).expect("observed"));
        assert_eq!(map.generation, 1);
        let id = DeviceId(uuid::Uuid::from_u128(1));
        assert_eq!(map.devices[&id].seat, seat_of(id));
        // the same claim again moves nothing
        assert!(!map.observe(node, &host("a"), &first).expect("observed"));
        assert_eq!(map.generation, 1);
        // a failure moves nothing either
        assert!(map.set_failed(node, &BTreeMap::from([(id, "EIO".to_string())])));
        assert_eq!(map.generation, 1);
        // a new class does, and the old era still answers for the old generation
        let changed = [facts(1, "nvme", 1, 100), facts(2, "ssd", 2, 200)];
        assert!(map.observe(node, &host("a"), &changed).expect("observed"));
        assert_eq!(map.generation, 2);
        assert_eq!(map.devices[&id].era_at(1).map(|era| era.class.as_str()), Some("ssd"));
        assert_eq!(map.devices[&id].era_at(2).map(|era| era.class.as_str()), Some("nvme"));
        // as does a host change, for both devices at once
        assert!(map.refresh_domains(node, &host("b")));
        assert_eq!(map.generation, 3);
        // and a node leaving takes both off the map
        assert!(map.retire_node(node));
        assert_eq!(map.generation, 4);
        assert!(map.devices.values().all(|device| device.current().is_none()));
        assert!(map.devices.values().all(|device| device.era_at(3).is_some()));
    }

    /// A device another node holds is refused, and nothing is recorded
    #[test]
    fn a_device_never_moves_between_nodes() {
        let mut map = PoolMap::default();
        map.observe(NodeId::from(1), &host("a"), &[facts(1, "ssd", 1, 100)])
            .expect("observed");
        let carried = map.observe(
            NodeId::from(2),
            &host("b"),
            &[facts(2, "ssd", 1, 100), facts(1, "ssd", 1, 100)],
        );
        assert!(matches!(carried, Err(PoolMapError::WrongNode { .. })));
        assert_eq!(map.devices.len(), 1);
        assert_eq!(map.generation, 1);
    }

    /// A pool is pinned once, at a generation the map has reached, and its pin outlives a change
    #[test]
    fn a_pool_is_pinned_once_and_the_map_moves_on_without_it() {
        let mut map = PoolMap::default();
        map.observe(NodeId::from(1), &host("a"), &[facts(1, "ssd", 1, 100)])
            .expect("observed");
        // a generation the map has not reached cannot be pinned
        assert!(matches!(
            map.pin("fast", 2),
            Err(PoolMapError::UnknownGeneration { .. })
        ));
        // the first pin is taken without moving the generation, and a second is not
        assert!(map.pin("fast", 1).expect("pinned"));
        assert_eq!(map.generation, 1);
        assert!(!map.pin("fast", 1).expect("kept"));
        // a device joining moves the map and leaves the pin where it was
        map.observe(NodeId::from(2), &host("b"), &[facts(2, "ssd", 1, 100)])
            .expect("observed");
        assert_eq!(map.generation, 2);
        assert_eq!(map.pinned_at("fast"), Some(1));
        assert_eq!(map.pinned_at("slow"), None);
    }

    /// The map crosses the control group as JSON and reads back as it was
    #[test]
    fn the_pool_map_round_trips_as_json() {
        let mut map = PoolMap::default();
        map.observe(NodeId::from(1), &host("a"), &[facts(1, "ssd", 2, 100)])
            .expect("observed");
        map.set_fitted(
            "fast",
            BTreeMap::from([(DeviceId(uuid::Uuid::from_u128(1)), 95)]),
        );
        map.pin("fast", 1).expect("pinned");
        let json = serde_json::to_string(&map).expect("written");
        let read: PoolMap = serde_json::from_str(&json).expect("read");
        assert_eq!(read, map);
        // an empty map reads from nothing at all, as a state from before it does
        let empty: PoolMap = serde_json::from_str("{}").expect("read");
        assert_eq!(empty, PoolMap::default());
    }
}
