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

use crate::shared::devices::{DeviceFacts, DeviceId, JournalId, SliceId};
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
    /// Whether the kernel calls its disk rotational; it places nothing, and is read only to warn of
    /// a pool whose media disagrees ([F91](../../../docs/src/features/rotational-devices.md))
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rotational: bool,
    /// The journal on another device it stages in: devices that share one fail together, so under
    /// a `device` failure domain they are one domain, and a change of it moves where its chunks
    /// belong ([F91](../../../docs/src/features/rotational-devices.md))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal: Option<JournalId>,
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
            && self.journal == other.journal
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
    /// An operator's order to fail it, and why, until its node reports it failed (F88)
    ///
    /// Its node fails it as an I/O error would when it reads the order, and its report of the
    /// failure clears it, so a restart that finds the device whole brings it back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fail_order: Option<String>,
    /// Why it is drained, if it is: its era ended so its placement groups move off it, while it
    /// serves what they copy from it ([F92](../../../docs/src/features/reclamation-and-device-lifecycle.md))
    ///
    /// No later observation begins an era for a drained device; it leaves the map by a removal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain: Option<String>,
    /// Why it was retired, if it was: a failure on a node that is up ended its era for good (F92)
    ///
    /// A failure its node no longer reports never clears it, so a restart that finds the disk whole
    /// does not bring back a device whose chunks were rebuilt elsewhere (item 250).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired: Option<String>,
    /// Who removed it, if anyone did: its id is never accepted again, and its node refuses to
    /// serve it until it is wiped and added as a new device (F92)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone: Option<String>,
    /// The weight an operator committed for it, which wins over what its configuration and size
    /// say at every later observation (F92)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<u64>,
    /// Whether its node last reported every slice of it holding nothing, as the control leader
    /// committed it once its era ended; what a removal of a drained device is admitted by (F92)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub empty: bool,
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

    /// Whether its era ended for good: drained, retired or removed, so no observation begins
    /// another and nothing is placed on it again (F92)
    #[must_use]
    pub fn is_ended(&self) -> bool {
        self.drain.is_some() || self.retired.is_some() || self.tombstone.is_some()
    }

    /// Whether nothing on it is read or written now: it failed, or its node refuses to serve it,
    /// removed or retired. A removal tombstones a device and never fails it, so a check of `failed`
    /// alone asked a removed device for every rebuild and every release (item 333)
    #[must_use]
    pub fn serves_nothing(&self) -> bool {
        self.failed.is_some() || self.tombstone.is_some() || self.retired.is_some()
    }

    /// Why its node must not serve it, if it must not: it was removed, or retired for a failure
    /// (F92). A drained device serves until it is removed, since its groups copy from it
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        if let Some(by) = &self.tombstone {
            return Some(format!(
                "device {} was removed by {by}: wipe it and add it as a new device",
                self.id
            ));
        }
        self.retired.as_ref().map(|why| {
            format!(
                "device {} was retired ({why}): its chunks were placed elsewhere, so remove it and add it again as a new device",
                self.id
            )
        })
    }

    /// The slice at an index in the newest era that has one, whatever the generation: where a
    /// slice a placement group left is reached to release it (M20)
    ///
    /// # Arguments
    ///
    /// * `index` - The slice's index
    #[must_use]
    pub fn slice_any(&self, index: u16) -> Option<SliceId> {
        self.eras
            .iter()
            .rev()
            .find_map(|era| era.slices.get(usize::from(index)).copied())
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
    /// A placement group that never moved places at its pool's pin, through the eras that keep the
    /// map's past ([F86](../../../docs/src/features/replicated-pools.md)); since wire 10 a group
    /// follows the map by a move to its pool's target, and its tablet group records where it is
    /// ([F88](../../../docs/src/features/recovery-and-moves.md)). The pin never moves the
    /// generation: it says where stripes are, not where they should be.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub pinned: BTreeMap<String, u64>,
    /// The generation each pool's placement groups move to: the newest it was fitted and ready at
    ///
    /// A placement group whose holders at its own generation - its pool's pin, or the one its last
    /// move switched it to - differ from the target's is moved there; one whose holders are the
    /// same stays where it is with nothing committed. Moves forward only, and a device change is
    /// the one thing that moves it ([F88](../../../docs/src/features/recovery-and-moves.md)).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub target: BTreeMap<String, u64>,
    /// Every move admitted and not yet done: one a device arriving at once (F88)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub moves: Vec<MoveSlot>,
    /// How many scrubs of each kind an operator has asked of each pool
    /// ([F89](../../../docs/src/features/scrub-and-repair.md)); a tablet group whose cursor serves
    /// fewer begins a pass at once. Never moves the generation: it says nothing of where a chunk is
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub scrub_requests: BTreeMap<String, crate::shared::scrub::ScrubRequests>,
}

/// How many admitted moves may arrive on one device at once
///
/// Each moves what one placement group holds of the device, and the device's recovery ceiling
/// bounds the bytes they move together; sixteen keep a device busy across the round trips of a
/// move's admission, begin, copy, switch and release (F88). ~~An empty group's move is nothing
/// else~~: a group that holds nothing moves with no slot since item 250, since nothing arrives, so
/// a failed device's empty groups no longer queue behind these.
pub const MOVES_A_DEVICE: usize = 16;

/// A move of one placement group the control group admitted, until its driver says it is done
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveSlot {
    /// The pool
    pub pool: String,
    /// The consumer whose placement group it is
    pub consumer: u64,
    /// The placement group
    pub pg: PlacementGroup,
    /// The generation it moves to
    pub generation: u64,
    /// The devices its chunks arrive on
    pub devices: Vec<DeviceId>,
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
    /// A device that was removed, whose id is never accepted again (F92)
    Removed {
        /// The device
        device: DeviceId,
        /// Who removed it
        by: String,
    },
    /// A device whose era already ended, drained or retired, which cannot be drained or reweighted
    Ended {
        /// The device
        device: DeviceId,
        /// Why its era ended
        why: String,
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
            PoolMapError::Removed { device, by } => write!(
                f,
                "device {device} was removed by {by}, and its id is never accepted again"
            ),
            PoolMapError::Ended { device, why } => write!(f, "device {device}'s era already ended: {why}"),
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
                // an operator's committed weight wins over the configuration's (F92)
                weight: self
                    .devices
                    .get(&fact.id)
                    .and_then(|device| device.weight)
                    .unwrap_or_else(|| fact.capacity_weight()),
                write_back: fact.write_back,
                rotational: fact.rotational,
                journal: fact.journal,
            };
            match self.devices.get_mut(&fact.id) {
                // a device drained, retired or removed never begins an era again, whatever its
                // node claims: it leaves the map by a removal, and comes back only as a new device
                Some(device) if device.is_ended() && device.current().is_none() => {}
                // a device placing exactly as it did changes nothing but what places nothing
                Some(device) if device.current().is_some_and(|current| current.places_as(&era)) => {
                    if let Some(current) = device.eras.last_mut().filter(|era| era.until.is_none()) {
                        current.write_back = fact.write_back;
                        current.rotational = fact.rotational;
                    }
                }
                // one retired for a failure its node still reports stays off the map until the
                // failure clears: its node names it among what it claimed, failed or not (F88)
                Some(device) if device.failed.is_some() && device.current().is_none() => {}
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
                            fail_order: None,
                            drain: None,
                            retired: None,
                            tombstone: None,
                            weight: None,
                            empty: false,
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
        // a removed device is nothing its node's word changes (F92)
        for device in self
            .devices
            .values_mut()
            .filter(|device| device.node == node && device.tombstone.is_none())
        {
            let now = failed.get(&device.id).cloned();
            if device.failed != now {
                device.failed = now;
                changed = true;
            }
            // an operator's order is done once the node reports the device failed
            if device.failed.is_some() && device.fail_order.take().is_some() {
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
                fail_order: None,
                drain: None,
                retired: None,
                tombstone: None,
                weight: None,
                empty: false,
                eras: vec![Era {
                    from: next,
                    until: None,
                    class: replacement.class.clone(),
                    domains: domains.clone(),
                    slices: replacement.slices.clone(),
                    weight: replacement.capacity_weight(),
                    write_back: replacement.write_back,
                    rotational: replacement.rotational,
                    journal: replacement.journal,
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

    /// The generation a pool's placement groups move to, if one was committed (F88)
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    #[must_use]
    pub fn target_of(&self, pool: &str) -> Option<u64> {
        self.target.get(pool).copied()
    }

    /// Where a pool's placement groups belong now: its target, or its pin before it has one
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    #[must_use]
    pub fn belongs_at(&self, pool: &str) -> Option<u64> {
        self.target_of(pool).or_else(|| self.pinned_at(pool))
    }

    /// Move a pool's target to a generation, forward only; whether it moved
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `generation` - The generation its groups move to
    ///
    /// # Errors
    ///
    /// A generation past the map's, which no view can be computed at.
    pub fn set_target(&mut self, pool: &str, generation: u64) -> Result<bool, PoolMapError> {
        if generation > self.generation {
            return Err(PoolMapError::UnknownGeneration {
                pool: pool.to_string(),
                generation,
                now: self.generation,
            });
        }
        // forward only: a target at or past it stays
        if self.belongs_at(pool).is_some_and(|at| at >= generation) {
            return Ok(false);
        }
        self.target.insert(pool.to_string(), generation);
        Ok(true)
    }

    /// End a failed device's era for good, so placement stops naming it; whether its era ended
    ///
    /// A device that failed on a node that is up is retired so its placement groups move off at
    /// once rather than waiting for its node's grace, which never comes (F88). Since F92 it stays
    /// retired: a failure its node stops reporting never begins another era (item 250).
    ///
    /// # Arguments
    ///
    /// * `id` - The device
    /// * `why` - Why, which its node is refused by when it claims the device again
    pub fn retire_device(&mut self, id: DeviceId, why: &str) -> bool {
        let next = self.generation + 1;
        let Some(device) = self.devices.get_mut(&id) else {
            return false;
        };
        if device.retired.is_none() {
            device.retired = Some(why.to_string());
        }
        let Some(last) = device.eras.last_mut().filter(|era| era.until.is_none()) else {
            return false;
        };
        last.until = Some(next);
        self.generation = next;
        true
    }

    /// Drain a device: end its era so its placement groups move off it, while it serves what they
    /// copy from it; whether its era ended (F92)
    ///
    /// # Arguments
    ///
    /// * `id` - The device
    /// * `why` - Who drained it and why
    ///
    /// # Errors
    ///
    /// A device not on the map, removed, or retired: a retired device's groups move off it without
    /// a drain, by rebuilding what it held.
    pub fn drain_device(&mut self, id: DeviceId, why: &str) -> Result<bool, PoolMapError> {
        let next = self.generation + 1;
        let device = self.devices.get_mut(&id).ok_or(PoolMapError::Unknown { device: id })?;
        if let Some(by) = &device.tombstone {
            return Err(PoolMapError::Removed { device: id, by: by.clone() });
        }
        if let Some(why) = &device.retired {
            return Err(PoolMapError::Ended {
                device: id,
                why: format!("it was retired ({why}), and its groups move off it by rebuilding"),
            });
        }
        // a drain asked again is the drain begun
        if device.drain.is_some() {
            return Ok(false);
        }
        device.drain = Some(why.to_string());
        device.empty = false;
        let Some(last) = device.eras.last_mut().filter(|era| era.until.is_none()) else {
            return Ok(false);
        };
        last.until = Some(next);
        self.generation = next;
        Ok(true)
    }

    /// Remove a device from the map for good: its era ended if it had not, and its id tombstoned
    /// so no observation or report of it is accepted again; whether the generation moved (F92)
    ///
    /// What admits a removal - drained and empty, retired, or the operator's word that it is lost -
    /// is the control state's to judge; this records it.
    ///
    /// # Arguments
    ///
    /// * `id` - The device
    /// * `by` - Who removed it
    ///
    /// # Errors
    ///
    /// A device not on the map.
    pub fn remove_device(&mut self, id: DeviceId, by: &str) -> Result<bool, PoolMapError> {
        let next = self.generation + 1;
        let device = self.devices.get_mut(&id).ok_or(PoolMapError::Unknown { device: id })?;
        if device.tombstone.is_none() {
            device.tombstone = Some(by.to_string());
        }
        device.fail_order = None;
        let Some(last) = device.eras.last_mut().filter(|era| era.until.is_none()) else {
            return Ok(false);
        };
        last.until = Some(next);
        self.generation = next;
        Ok(true)
    }

    /// Tombstone every device of a node whose identity was tombstoned, so none of them is accepted
    /// again; whether anything changed (F92)
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `by` - The operation that removed it
    pub fn tombstone_node(&mut self, node: NodeId, by: &str) -> bool {
        let next = self.generation + 1;
        let mut changed = false;
        let mut moved = false;
        for device in self
            .devices
            .values_mut()
            .filter(|device| device.node == node && device.tombstone.is_none())
        {
            device.tombstone = Some(by.to_string());
            changed = true;
            if let Some(last) = device.eras.last_mut().filter(|era| era.until.is_none()) {
                last.until = Some(next);
                moved = true;
            }
        }
        if moved {
            self.generation = next;
        }
        changed
    }

    /// Give a device the weight an operator committed, beginning an era at it if it places
    /// otherwise; whether the generation moved (F92)
    ///
    /// # Arguments
    ///
    /// * `id` - The device
    /// * `weight` - Its share of a pool
    ///
    /// # Errors
    ///
    /// A device not on the map, removed, or whose era ended.
    pub fn reweight_device(&mut self, id: DeviceId, weight: u64) -> Result<bool, PoolMapError> {
        let next = self.generation + 1;
        let device = self.devices.get_mut(&id).ok_or(PoolMapError::Unknown { device: id })?;
        if let Some(by) = &device.tombstone {
            return Err(PoolMapError::Removed { device: id, by: by.clone() });
        }
        let Some(current) = device.current().cloned() else {
            return Err(PoolMapError::Ended {
                device: id,
                why: device
                    .drain
                    .clone()
                    .or_else(|| device.retired.clone())
                    .unwrap_or_else(|| "it is off the map".to_string()),
            });
        };
        device.weight = Some(weight);
        if current.weight == weight {
            return Ok(false);
        }
        // the era in force ends and one at the new weight begins
        let mut era = current;
        era.from = next;
        era.weight = weight;
        if let Some(last) = device.eras.last_mut() {
            last.until = Some(next);
        }
        device.eras.push(era);
        self.generation = next;
        Ok(true)
    }

    /// Record which of a node's devices whose era ended hold nothing, as its node reported them;
    /// moves no generation, and whether anything changed (F92)
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `empty` - Its devices whose every slice holds nothing
    pub fn set_empty(&mut self, node: NodeId, empty: &std::collections::BTreeSet<DeviceId>) -> bool {
        let mut changed = false;
        for device in self
            .devices
            .values_mut()
            .filter(|device| device.node == node && device.tombstone.is_none())
        {
            // only a device nothing is placed on can stay empty
            let now = device.current().is_none() && empty.contains(&device.id);
            if device.empty != now {
                device.empty = now;
                changed = true;
            }
        }
        changed
    }

    /// Count an operator's request for a scrub of a pool, and answer the pool's new count
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `kind` - The kind of scrub
    /// * `target` - What it narrows its pass to, or none for the whole pool (F93)
    pub fn request_scrub(
        &mut self,
        pool: &str,
        kind: crate::shared::scrub::ScrubKind,
        target: Option<crate::shared::scrub::ScrubTarget>,
    ) -> u64 {
        self.scrub_requests
            .entry(pool.to_string())
            .or_default()
            .ask_for(kind, target)
    }

    /// A pool's counts of scrub requests
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    #[must_use]
    pub fn scrub_requests_of(&self, pool: &str) -> crate::shared::scrub::ScrubRequests {
        self.scrub_requests.get(pool).copied().unwrap_or_default()
    }

    /// Record an operator's order to fail a device, which its node carries out; whether it is new
    ///
    /// # Arguments
    ///
    /// * `id` - The device
    /// * `reason` - Why
    pub fn order_failed(&mut self, id: DeviceId, reason: &str) -> bool {
        match self.devices.get_mut(&id) {
            Some(device) if device.failed.is_none() && device.fail_order.is_none() && device.tombstone.is_none() => {
                device.fail_order = Some(reason.to_string());
                true
            }
            _ => false,
        }
    }

    /// Admit a move, at most [`MOVES_A_DEVICE`] arriving on a device at once; whether it is new
    ///
    /// # Arguments
    ///
    /// * `slot` - The move
    ///
    /// # Errors
    ///
    /// The device another admitted move is arriving on.
    pub fn admit(&mut self, slot: MoveSlot) -> Result<bool, DeviceId> {
        // the same group's move again is the move admitted
        if let Some(admitted) = self.move_of(&slot.pool, slot.consumer, slot.pg) {
            return if admitted.generation == slot.generation {
                Ok(false)
            } else {
                Err(admitted.devices.first().copied().unwrap_or_default())
            };
        }
        // so many moves arriving on a device at once
        for device in &slot.devices {
            let arriving = self
                .moves
                .iter()
                .filter(|admitted| admitted.devices.contains(device))
                .count();
            if arriving >= MOVES_A_DEVICE {
                return Err(*device);
            }
        }
        self.moves.push(slot);
        Ok(true)
    }

    /// The move admitted for a placement group, if there is one
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `consumer` - The consumer
    /// * `pg` - The placement group
    #[must_use]
    pub fn move_of(&self, pool: &str, consumer: u64, pg: PlacementGroup) -> Option<&MoveSlot> {
        self.moves
            .iter()
            .find(|slot| slot.pool == pool && slot.consumer == consumer && slot.pg == pg)
    }

    /// Free a placement group's move; whether there was one
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    /// * `consumer` - The consumer
    /// * `pg` - The placement group
    pub fn move_done(&mut self, pool: &str, consumer: u64, pg: PlacementGroup) -> bool {
        let before = self.moves.len();
        self.moves
            .retain(|slot| !(slot.pool == pool && slot.consumer == consumer && slot.pg == pg));
        self.moves.len() != before
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
            rotational: false,
            journal: None,
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

    /// An operator's order is carried out by the device's node and cleared by its report; a failed
    /// device retires once, and stays retired when its failure clears: a restart that finds the
    /// disk whole never brings back a device whose chunks were placed elsewhere (F88, item 250's
    /// second half fixed by F92)
    #[test]
    fn a_failed_device_retires_once_and_stays_retired() {
        let mut map = PoolMap::default();
        let node = NodeId::from(1);
        let claim = [facts(1, "ssd", 1, 100), facts(2, "ssd", 1, 100)];
        map.observe(node, &host("a"), &claim).expect("observed");
        let id = DeviceId(uuid::Uuid::from_u128(1));
        // an order is recorded once, and the node's report of the failure clears it
        assert!(map.order_failed(id, "operator"));
        assert!(!map.order_failed(id, "operator"));
        assert!(map.set_failed(node, &BTreeMap::from([(id, "operator".to_string())])));
        assert!(map.devices[&id].fail_order.is_none());
        assert!(!map.order_failed(id, "again"), "a failed device takes no order");
        // retired once, moving the generation, and never twice
        assert!(map.retire_device(id, "failed by operator"));
        assert_eq!(map.generation, 2);
        assert!(map.devices[&id].current().is_none());
        assert!(!map.retire_device(id, "again"));
        // its node still names it, failed, and the map leaves it off
        assert!(!map.observe(node, &host("a"), &claim).expect("observed"));
        assert_eq!(map.generation, 2);
        // and the failure clearing changes nothing: it stays off the map, refused by name
        assert!(map.set_failed(node, &BTreeMap::new()));
        assert!(!map.observe(node, &host("a"), &claim).expect("observed"));
        assert_eq!(map.generation, 2);
        assert!(map.devices[&id].current().is_none());
        let refusal = map.devices[&id].refusal().expect("a retired device is refused");
        assert!(refusal.contains("retired"), "{refusal}");
    }

    /// A drained device's era ends once, it serves still, and no observation brings it back; it is
    /// removed, tombstoned, and every door refuses it from then on: an observation, a report of
    /// its failure, an order, a drain and a reweight (F92)
    #[test]
    fn a_drained_device_is_removed_and_its_id_is_never_accepted_again() {
        let mut map = PoolMap::default();
        let node = NodeId::from(1);
        let claim = [facts(1, "ssd", 1, 100), facts(2, "ssd", 1, 100)];
        map.observe(node, &host("a"), &claim).expect("observed");
        let id = DeviceId(uuid::Uuid::from_u128(1));
        // drained once, moving the generation, and asked again changes nothing
        assert!(map.drain_device(id, "drained by admin").expect("drained"));
        assert_eq!(map.generation, 2);
        assert!(!map.drain_device(id, "again").expect("drained"));
        assert_eq!(map.generation, 2);
        assert!(map.devices[&id].current().is_none());
        assert!(map.devices[&id].refusal().is_none(), "a drained device serves its copies");
        // its node's claim begins no era for it
        assert!(!map.observe(node, &host("a"), &claim).expect("observed"));
        // empty is recorded only for a device nothing is placed on
        let both = std::collections::BTreeSet::from([id, DeviceId(uuid::Uuid::from_u128(2))]);
        assert!(map.set_empty(node, &both));
        assert!(map.devices[&id].empty);
        assert!(!map.devices[&DeviceId(uuid::Uuid::from_u128(2))].empty, "a placed device is never empty");
        // removed: tombstoned, and every door shut
        assert!(!map.remove_device(id, "op 1 by admin").expect("removed"), "its era had ended");
        assert!(map.devices[&id].tombstone.is_some());
        assert!(!map.observe(node, &host("a"), &claim).expect("observed"));
        assert!(!map.set_failed(node, &BTreeMap::from([(id, "gone".to_string())])));
        assert!(!map.order_failed(id, "operator"));
        assert!(matches!(map.drain_device(id, "again"), Err(PoolMapError::Removed { .. })));
        assert!(matches!(map.reweight_device(id, 5), Err(PoolMapError::Removed { .. })));
        let refusal = map.devices[&id].refusal().expect("a removed device is refused");
        assert!(refusal.contains("wipe it"), "{refusal}");
        // a retired device cannot be drained: its groups move off it by rebuilding
        let other = DeviceId(uuid::Uuid::from_u128(2));
        assert!(map.retire_device(other, "failed"));
        assert!(matches!(map.drain_device(other, "late"), Err(PoolMapError::Ended { .. })));
    }

    /// A device serves nothing once it failed, was retired, or was removed, which never fails it; a
    /// drained one serves the copies its groups take (item 333)
    #[test]
    fn a_removed_device_serves_nothing_though_it_never_failed() {
        let mut map = PoolMap::default();
        let node = NodeId::from(1);
        let claim = [facts(1, "ssd", 1, 100), facts(2, "ssd", 1, 100), facts(3, "ssd", 1, 100), facts(4, "ssd", 1, 100)];
        map.observe(node, &host("a"), &claim).expect("observed");
        let id = |n: u128| DeviceId(uuid::Uuid::from_u128(n));
        // a placed device serves, and a drained one still serves its copies
        assert!(!map.devices[&id(1)].serves_nothing());
        map.drain_device(id(1), "drained by admin").expect("drained");
        assert!(!map.devices[&id(1)].serves_nothing(), "a drained device serves its copies");
        // a removal tombstones a device and never fails it, and it serves nothing
        map.remove_device(id(2), "op 2 by admin, lost").expect("removed");
        assert!(map.devices[&id(2)].failed.is_none());
        assert!(map.devices[&id(2)].serves_nothing());
        // nor does a failed device, or a retired one
        assert!(map.set_failed(node, &BTreeMap::from([(id(3), "EIO".to_string())])));
        assert!(map.devices[&id(3)].serves_nothing());
        assert!(map.retire_device(id(4), "failed"));
        assert!(map.devices[&id(4)].serves_nothing());
    }

    /// A reweight begins an era at the operator's weight, which every later observation keeps
    /// over the configuration's, and the same weight again moves nothing (F92)
    #[test]
    fn a_reweight_begins_an_era_the_configuration_never_undoes() {
        let mut map = PoolMap::default();
        let node = NodeId::from(1);
        let claim = [facts(1, "ssd", 1, 100)];
        map.observe(node, &host("a"), &claim).expect("observed");
        let id = DeviceId(uuid::Uuid::from_u128(1));
        let before = map.devices[&id].current().expect("placed").weight;
        assert!(map.reweight_device(id, before * 3).expect("reweighted"));
        assert_eq!(map.generation, 2);
        assert_eq!(map.devices[&id].current().expect("placed").weight, before * 3);
        assert!(!map.reweight_device(id, before * 3).expect("reweighted"));
        // its node's claim, which says its old weight, changes nothing
        assert!(!map.observe(node, &host("a"), &claim).expect("observed"));
        assert_eq!(map.devices[&id].current().expect("placed").weight, before * 3);
        // and the old era still says what placed before
        assert_eq!(map.devices[&id].era_at(1).expect("an era").weight, before);
    }

    /// A member's tombstone tombstones its devices, once (F92)
    #[test]
    fn a_tombstoned_member_tombstones_its_devices() {
        let mut map = PoolMap::default();
        map.observe(NodeId::from(1), &host("a"), &[facts(1, "ssd", 1, 100)]).expect("observed");
        map.observe(NodeId::from(2), &host("b"), &[facts(2, "ssd", 1, 100)]).expect("observed");
        assert!(map.tombstone_node(NodeId::from(1), "op 9"));
        assert!(!map.tombstone_node(NodeId::from(1), "op 9"));
        assert!(map.devices[&DeviceId(uuid::Uuid::from_u128(1))].tombstone.is_some());
        assert!(map.devices[&DeviceId(uuid::Uuid::from_u128(2))].tombstone.is_none());
    }

    /// A pool's target moves forward only and never past the map; a move is admitted once a
    /// group, at most sixteen arriving on a device at once, and its slot freed when it is done
    #[test]
    fn a_target_moves_forward_and_moves_are_admitted_sixteen_a_device() {
        use super::{MoveSlot, PlacementGroup, MOVES_A_DEVICE};
        let mut map = PoolMap::default();
        map.observe(NodeId::from(1), &host("a"), &[facts(1, "ssd", 1, 100)]).expect("observed");
        map.observe(NodeId::from(2), &host("b"), &[facts(2, "ssd", 1, 100)]).expect("observed");
        map.pinned.insert("fast".to_string(), 1);
        // before a target, a pool belongs at its pin
        assert_eq!(map.belongs_at("fast"), Some(1));
        assert!(!map.set_target("fast", 1).expect("set"), "at its pin is no move");
        assert!(map.set_target("fast", 2).expect("set"));
        assert!(!map.set_target("fast", 1).expect("set"), "a target never goes back");
        assert_eq!(map.belongs_at("fast"), Some(2));
        assert!(matches!(map.set_target("fast", 9), Err(PoolMapError::UnknownGeneration { .. })));
        // sixteen moves arrive on a device at once, and the seventeenth names it
        let device = DeviceId(uuid::Uuid::from_u128(2));
        let slot = |sub: u16, generation: u64| MoveSlot {
            pool: "fast".to_string(),
            consumer: 7,
            pg: PlacementGroup { tablet: 0, sub },
            generation,
            devices: vec![device],
        };
        for sub in 0..MOVES_A_DEVICE {
            assert_eq!(map.admit(slot(u16::try_from(sub).unwrap(), 2)), Ok(true));
        }
        assert_eq!(map.admit(slot(100, 2)), Err(device));
        // the same group's move again is the one admitted, and another generation's is refused
        assert_eq!(map.admit(slot(0, 2)), Ok(false));
        assert_eq!(map.admit(slot(0, 3)), Err(device));
        // a move done frees its slot for the next
        assert!(map.move_done("fast", 7, PlacementGroup { tablet: 0, sub: 0 }));
        assert!(!map.move_done("fast", 7, PlacementGroup { tablet: 0, sub: 0 }));
        assert_eq!(map.admit(slot(100, 2)), Ok(true));
        assert!(map.move_of("fast", 7, PlacementGroup { tablet: 0, sub: 100 }).is_some());
    }
}
