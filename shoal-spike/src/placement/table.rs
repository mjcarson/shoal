//! Placement held as a table: the planner's candidate, and the exceptions a rule is given
//!
//! S5's third candidate is a table the planner assigns and the control group commits: any
//! balance wanted, nothing statistical, and a record for each placement group on the map. It is
//! simulated here as a planner would build it. Each chunk is placed on the device least full for
//! its weight among those the domain rule allows. A change is applied by moving no more than it
//! has to: the chunks of a device that left are placed again, a device that came or grew is
//! filled from the fullest, and a device that shrank is emptied into the emptiest. Its fill is
//! the best a shape allows, and its moves are the least a change could make, which is what the
//! other candidates are read against.
//!
//! The same machinery gives a rule its **exceptions**: starting from the rendezvous answer, a
//! chunk is moved off the fullest device onto the emptiest one the domain rule allows, one at a
//! time, as Ceph's balancer writes `pg_upmap_items`. Every chunk that ends up away from where the
//! rule puts it is one exception on the map. How many it takes to bring the fullest device
//! within a margin of the mean is what says whether exceptions are the remedy or the rule.

use std::collections::{HashMap, HashSet};

use super::candidates::View;

/// A slot not holding a chunk
const EMPTY: u32 = u32::MAX;

/// Every chunk of every placement group of one pool, against one view of its map
pub struct Assignment<'v> {
    /// The view the slots index into
    pub view: &'v View,
    /// Positions a placement group has
    pub width: usize,
    /// For each `pg * width + pos`, the index in the view of the slice holding it
    pub slots: Vec<u32>,
    /// Chunks on each of the view's devices
    pub device_load: Vec<u64>,
    /// Chunks on each of the view's slices
    pub slice_load: Vec<u64>,
    /// The slots each device has held, checked against `slots` when read
    chunks: Vec<Vec<u32>>,
}

impl<'v> Assignment<'v> {
    /// An assignment of nothing yet
    ///
    /// # Arguments
    ///
    /// * `view` - The view
    /// * `pgs` - How many placement groups
    #[must_use]
    pub fn empty(view: &'v View, pgs: usize) -> Self {
        Assignment {
            view,
            width: view.width,
            slots: vec![EMPTY; pgs * view.width],
            device_load: vec![0; view.devices.len()],
            slice_load: vec![0; view.slices.len()],
            chunks: vec![Vec::new(); view.devices.len()],
        }
    }

    /// An assignment of slice ids, from a candidate's answers or another map's table
    ///
    /// Returns the assignment and the slots whose slice is not in this view, left empty.
    ///
    /// # Arguments
    ///
    /// * `view` - The view to hold them against
    /// * `ids` - A slice id for each slot
    #[must_use]
    pub fn from_ids(view: &'v View, ids: &[u32]) -> (Self, Vec<u32>) {
        let index = view.index();
        let mut assignment = Assignment::empty(view, ids.len() / view.width);
        let mut orphans = Vec::new();
        for (slot, id) in ids.iter().enumerate() {
            match index.get(id) {
                Some(slice) => assignment.put(slot as u32, *slice),
                None => orphans.push(slot as u32),
            }
        }
        (assignment, orphans)
    }

    /// The slice ids of every slot, to compare with another map's
    #[must_use]
    pub fn ids(&self) -> Vec<u32> {
        self.slots
            .iter()
            .map(|slice| self.view.slices[*slice as usize].id)
            .collect()
    }

    /// Put a slot on a slice
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot
    /// * `slice` - The slice's index in the view
    fn put(&mut self, slot: u32, slice: u32) {
        // count it on the slice and its device, and list it with the device
        let device = self.view.slices[slice as usize].device as usize;
        self.slots[slot as usize] = slice;
        self.slice_load[slice as usize] += 1;
        self.device_load[device] += 1;
        self.chunks[device].push(slot);
    }

    /// Take a slot off its slice
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot
    fn take(&mut self, slot: u32) {
        // its device's list is left alone and read lazily
        let slice = self.slots[slot as usize];
        let device = self.view.slices[slice as usize].device as usize;
        self.slice_load[slice as usize] -= 1;
        self.device_load[device] -= 1;
        self.slots[slot as usize] = EMPTY;
    }

    /// Whether a slot may move to a device: no other position of its group is in that domain
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot
    /// * `device` - The device's index in the view
    fn can_take(&self, slot: u32, device: usize) -> bool {
        let domain = self.view.devices[device].domain;
        let pg = slot as usize / self.width;
        let pos = slot as usize % self.width;
        // every other position, filled or not
        (0..self.width).all(|other| {
            if other == pos {
                return true;
            }
            let slice = self.slots[pg * self.width + other];
            slice == EMPTY || self.view.slices[slice as usize].domain != domain
        })
    }

    /// Put a slot on a device's least loaded slice
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot
    /// * `device` - The device's index in the view
    fn put_on_device(&mut self, slot: u32, device: usize) {
        // the slices of one device share its space, so the least loaded spreads the work
        let range = self.view.devices[device].slices.clone();
        let slice = range
            .min_by_key(|slice| self.slice_load[*slice])
            .expect("every device has a slice");
        self.put(slot, slice as u32);
    }

    /// A device's load over its weight
    ///
    /// # Arguments
    ///
    /// * `device` - The device's index in the view
    #[must_use]
    pub fn util(&self, device: usize) -> f64 {
        self.device_load[device] as f64 / self.view.devices[device].weight
    }

    /// The pool's load over its weight
    #[must_use]
    pub fn mean(&self) -> f64 {
        let total: u64 = self.device_load.iter().sum();
        let weight: f64 = self.view.devices.iter().map(|device| device.weight).sum();
        total as f64 / weight
    }

    /// The fullest device over the mean
    #[must_use]
    pub fn worst(&self) -> f64 {
        let mean = self.mean();
        (0..self.view.devices.len())
            .map(|device| self.util(device))
            .fold(0.0, f64::max)
            / mean
    }

    /// Place a slot greedily: on the allowed device least full for its weight after taking it
    ///
    /// Returns false if no device is allowed, which only a pool short of domains gives.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot
    fn place_greedy(&mut self, slot: u32) -> bool {
        let mut best = None;
        let mut best_util = f64::INFINITY;
        for device in 0..self.view.devices.len() {
            // the fill it would reach, ties to the lower index
            let util = (self.device_load[device] + 1) as f64 * self.view.devices[device].inv_w;
            if util < best_util && self.can_take(slot, device) {
                best_util = util;
                best = Some(device);
            }
        }
        match best {
            Some(device) => {
                self.put_on_device(slot, device);
                true
            }
            None => false,
        }
    }

    /// Build the planner's table for every placement group
    ///
    /// # Arguments
    ///
    /// * `view` - The view
    /// * `pgs` - How many placement groups
    #[must_use]
    pub fn table(view: &'v View, pgs: usize) -> Self {
        let mut assignment = Assignment::empty(view, pgs);
        // every slot in turn, each on the device least full after it
        for slot in 0..(pgs * view.width) as u32 {
            assert!(assignment.place_greedy(slot), "a feasible pool places every chunk");
        }
        assignment
    }

    /// A slot on one device that may move to another, if there is one
    ///
    /// # Arguments
    ///
    /// * `from` - The device to take from
    /// * `to` - The device to move to
    fn movable(&mut self, from: usize, to: usize) -> Option<u32> {
        // drop the stale entries of the device's list as they are met
        let mut index = 0;
        while index < self.chunks[from].len() {
            let slot = self.chunks[from][index];
            let slice = self.slots[slot as usize];
            if slice == EMPTY || self.view.slices[slice as usize].device as usize != from {
                self.chunks[from].swap_remove(index);
                continue;
            }
            if self.can_take(slot, to) {
                return Some(slot);
            }
            index += 1;
        }
        None
    }

    /// Move one chunk from a device to another
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot
    /// * `to` - The device's index in the view
    fn move_to(&mut self, slot: u32, to: usize) {
        self.take(slot);
        self.put_on_device(slot, to);
    }

    /// Fill a device from the fullest devices until it holds its share; returns the moves
    ///
    /// # Arguments
    ///
    /// * `to` - The device's index in the view
    fn pull_into(&mut self, to: usize) -> u64 {
        let mut moves = 0;
        let target = (self.mean() * self.view.devices[to].weight).floor() as u64;
        while self.device_load[to] < target {
            // donors fullest first, the first with a chunk allowed on this device
            let mut donors: Vec<usize> = (0..self.view.devices.len())
                .filter(|d| *d != to && self.util(*d) > self.util(to))
                .collect();
            donors.sort_by(|a, b| self.util(*b).total_cmp(&self.util(*a)));
            let found = donors.into_iter().find_map(|donor| self.movable(donor, to));
            match found {
                Some(slot) => {
                    self.move_to(slot, to);
                    moves += 1;
                }
                None => break,
            }
        }
        moves
    }

    /// Empty a device into the emptiest until it holds no more than its share; returns the moves
    ///
    /// # Arguments
    ///
    /// * `from` - The device's index in the view
    fn push_from(&mut self, from: usize) -> u64 {
        let mut moves = 0;
        let target = (self.mean() * self.view.devices[from].weight).ceil() as u64;
        while self.device_load[from] > target {
            // receivers emptiest first, the first that may take a chunk of this device
            let mut receivers: Vec<usize> = (0..self.view.devices.len())
                .filter(|d| *d != from && self.util(*d) < self.util(from))
                .collect();
            receivers.sort_by(|a, b| self.util(*a).total_cmp(&self.util(*b)));
            let found = receivers
                .into_iter()
                .find_map(|receiver| self.movable(from, receiver).map(|slot| (slot, receiver)));
            match found {
                Some((slot, receiver)) => {
                    self.move_to(slot, receiver);
                    moves += 1;
                }
                None => break,
            }
        }
        moves
    }

    /// Carry the planner's table onto a changed map, moving as little as the change needs
    ///
    /// Returns the table on the new view and the chunks moved.
    ///
    /// # Arguments
    ///
    /// * `old` - The table on the old view
    /// * `new` - The new view
    #[must_use]
    pub fn carry(old: &Assignment<'_>, new: &'v View) -> (Self, u64) {
        let (mut table, orphans) = Assignment::from_ids(new, &old.ids());
        let mut moves = 0;
        // the chunks of every device that left, each on the device least full after it
        for slot in orphans {
            assert!(table.place_greedy(slot), "a feasible pool places every chunk");
            moves += 1;
        }
        // a device that came, or whose weight grew, is filled to its share
        let old_weights: HashMap<u32, f64> = old
            .view
            .devices
            .iter()
            .map(|device| (device.uid, device.weight))
            .collect();
        for device in 0..new.devices.len() {
            let uid = new.devices[device].uid;
            match old_weights.get(&uid) {
                None => moves += table.pull_into(device),
                Some(weight) if *weight < new.devices[device].weight => {
                    moves += table.pull_into(device);
                }
                Some(weight) if *weight > new.devices[device].weight => {
                    moves += table.push_from(device);
                }
                Some(_) => {}
            }
        }
        (table, moves)
    }

    /// Give a rule's answer exceptions until the fullest device is within each margin
    ///
    /// For each margin, ascending in strictness, the result is the exceptions on the map once
    /// it was reached and the placement groups they touch, or `None` if the balancer stopped
    /// first because no chunk on the fullest device could move anywhere emptier.
    ///
    /// # Arguments
    ///
    /// * `margins` - The margins over the mean, loosest first, such as 0.05
    /// * `cap` - The most moves to try before giving up
    #[must_use]
    pub fn exceptions(&mut self, margins: &[f64], cap: u64) -> Vec<Option<(u64, u64)>> {
        let rule = self.slots.clone();
        let mut results = Vec::with_capacity(margins.len());
        let mut moves = 0;
        for margin in margins {
            let reached = loop {
                // the fullest device, done when it is within the margin
                let mean = self.mean();
                let fullest = (0..self.view.devices.len())
                    .max_by(|a, b| self.util(*a).total_cmp(&self.util(*b)))
                    .expect("a pool has a device");
                if self.util(fullest) <= mean * (1.0 + margin) {
                    break true;
                }
                if moves >= cap {
                    break false;
                }
                // the emptiest device that may take one of its chunks
                // a move that would leave the receiver fuller than the donor is no help
                let mut receivers: Vec<usize> = (0..self.view.devices.len())
                    .filter(|d| {
                        *d != fullest
                            && (self.device_load[*d] + 1) as f64 * self.view.devices[*d].inv_w
                                < self.util(fullest)
                    })
                    .collect();
                receivers.sort_by(|a, b| self.util(*a).total_cmp(&self.util(*b)));
                let found = receivers
                    .into_iter()
                    .find_map(|receiver| self.movable(fullest, receiver).map(|slot| (slot, receiver)));
                match found {
                    Some((slot, receiver)) => {
                        self.move_to(slot, receiver);
                        moves += 1;
                    }
                    None => break false,
                }
            };
            if !reached {
                // a margin that was not reached leaves every stricter one unreached
                results.resize(margins.len(), None);
                return results;
            }
            // the exceptions are the slots away from the rule, and the groups they are in
            let mut pgs = HashSet::new();
            let mut entries = 0;
            for (slot, slice) in self.slots.iter().enumerate() {
                if *slice != rule[slot] {
                    entries += 1;
                    pgs.insert(slot / self.width);
                }
            }
            results.push(Some((entries, pgs.len() as u64)));
        }
        results
    }
}
