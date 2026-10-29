//! Where a stripe's chunks are, and whether a pool takes a write
//!
//! A pool places its stripes at its pin: the generation the control group committed the first
//! time the pool was ready, where a placement group sits until it moves from one generation's
//! slices to another's ([F86](../../../../../docs/src/features/replicated-pools.md)).
//! Every coordinator computes the same answer from the same map, through the eras that keep the
//! map's past, so a write and every later read of its stripe name the same holders.
//!
//! Since M16 a placement group can move ([F88](../../../../../docs/src/features/recovery-and-moves.md)):
//! its tablet group holds the generation its chunks sit under, each position's slice there, and
//! the move in flight. A [`StripeSet`] is a stripe's chunks under that state: each position's
//! holder, and while the group moves, each moving position's arrival beside it.
//!
//! # Invariants
//!
//! **A stripe is placed by its placement group's state and nowhere else**: at its pool's pin and
//! in the rule's order while the group never moved. A device that joined since is not placed on;
//! one that failed or whose node is down still holds its position, and a write counts it missed.
//!
//! **A moving position is held only when its old slice and its arrival both hold it.** Counting
//! the old slice alone lets the switch throw away the only copy the count stood on (X1's
//! `both_counts_the_old_holder_alone`).

use std::collections::BTreeMap;
use std::sync::Arc;

use std::cell::RefCell;

use crate::server::map::TabletMap;
use crate::server::objects::Holder;
use crate::shared::objects::stripe_key;
use crate::shared::placement_state::{PgKey, PgView};
use crate::shared::pool_placement::{holders, pg_of, readiness_at, PoolReadiness, View};
use crate::shared::poolmap::{PlacementGroup, SliceRef};
use crate::shared::pools::{PoolSpec, Redundancy};
use crate::shared::protocol::object::ObjectOutcome;
use crate::shared::protocol::ERASURE_WIRE_VERSION;
use crate::shared::stripes::ChunkId;

/// A pool's stripes as one operation places them
#[derive(Debug, Clone)]
pub(crate) struct Placement {
    /// The pool's name
    pub pool_name: String,
    /// The pool
    pub pool: PoolSpec,
    /// The consumer the bucket is bound as
    pub consumer: u64,
    /// The generation the pool places at
    pub generation: u64,
    /// The pool's slices at that generation
    view: Arc<View>,
    /// The pool's slices at every other generation a placement group was found at
    views: RefCell<BTreeMap<u64, Arc<View>>>,
    /// The map placed by
    map: Arc<TabletMap>,
}

/// A stripe's chunks as its placement group's state names them (F88)
#[derive(Debug, Clone)]
pub(crate) struct StripeSet {
    /// The stripe's placement group
    pub key: PgKey,
    /// The epoch of the state the set was made from, which every commit it stages claims
    pub epoch: u64,
    /// How many positions the stripe has
    pub width: usize,
    /// Every chunk a stage, a fold or a put's piece goes to: each position's holder at the
    /// group's generation, in position order, then each moving position's arrival
    pub targets: Vec<(ChunkId, Holder)>,
}

impl StripeSet {
    /// Each position's chunk and holder at the group's generation, which a read asks
    #[must_use]
    pub fn current(&self) -> &[(ChunkId, Holder)] {
        &self.targets[..self.width.min(self.targets.len())]
    }

    /// Which positions hold something, given which targets do
    ///
    /// A position is held only when every target of it holds: its holder and, while the group
    /// moves, its arrival.
    ///
    /// # Arguments
    ///
    /// * `held` - Whether each target holds it, in the order of [`StripeSet::targets`]
    #[must_use]
    pub fn positions(&self, held: &[bool]) -> Vec<bool> {
        let mut positions = vec![true; self.width];
        for (index, (chunk, _)) in self.targets.iter().enumerate() {
            if !held.get(index).copied().unwrap_or(false) {
                if let Some(slot) = positions.get_mut(usize::from(chunk.position)) {
                    *slot = false;
                }
            }
        }
        positions
    }
}

impl Placement {
    /// A pool's placement, refused by name when the pool cannot take what is asked of it
    ///
    /// A read needs only the pin; a write needs the pool ready at it, and an erasure coded pool a
    /// cluster that speaks its parity's frames (F90).
    ///
    /// # Arguments
    ///
    /// * `map` - The map
    /// * `pool_name` - The pool's name
    /// * `pool` - The pool
    /// * `consumer` - The consumer the bucket is bound as
    /// * `write` - Whether it is for a write
    ///
    /// # Errors
    ///
    /// The outcome to refuse with and why.
    pub fn new(
        map: Arc<TabletMap>,
        pool_name: &str,
        pool: &PoolSpec,
        consumer: u64,
        write: bool,
    ) -> Result<Self, (ObjectOutcome, String)> {
        // the generation the pool places at, which only a ready pool was given
        let Some(pinned) = map.pool_map.pinned_at(pool_name) else {
            let ready = readiness(&map, pool_name, pool, map.pool_map.generation);
            let why = if ready.short.is_empty() {
                "it has not been placed yet: its first readiness is not committed".to_string()
            } else {
                ready.short.join("; ")
            };
            return Err((ObjectOutcome::NotReady, format!("pool {pool_name} takes no writes: {why}")));
        };
        // and a consumer bound after the pin places a group that never moved where its pool
        // belonged when it was bound, never at a pin whose devices may since be retired (F93)
        let since = map
            .objects
            .bindings
            .values()
            .find(|binding| binding.consumer == consumer)
            .map_or(0, |binding| binding.since);
        let generation = pinned.max(since);
        if write {
            // a write needs the pool's holders where its groups belong now: its target once it
            // has one, which a group still at an older generation is moving to (F88)
            let judged = map.pool_map.belongs_at(pool_name).unwrap_or(generation);
            let ready = readiness(&map, pool_name, pool, judged);
            if !ready.ready() {
                return Err((
                    ObjectOutcome::NotReady,
                    format!("pool {pool_name} takes no writes: {}", ready.short.join("; ")),
                ));
            }
            // an erasure coded pool is written once every member speaks its parity's frames: a
            // cluster that has not activated them is not ready for one, by name (F90)
            if !matches!(pool.redundancy, Redundancy::Replicas(_))
                && !map.members.is_empty()
                && map.activated_wire < ERASURE_WIRE_VERSION
            {
                return Err((
                    ObjectOutcome::NotReady,
                    format!(
                        "pool {pool_name} is erasure coded, which this cluster writes once wire version \
                         {ERASURE_WIRE_VERSION} is activated, and it has activated {}",
                        map.activated_wire
                    ),
                ));
            }
        }
        let view = Arc::new(View::new(&map.pool_map, pool_name, pool, generation));
        Ok(Placement {
            pool_name: pool_name.to_string(),
            pool: pool.clone(),
            consumer,
            generation,
            view,
            views: RefCell::new(BTreeMap::new()),
            map,
        })
    }

    /// The placement group a stripe falls in
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `stripe` - The stripe
    #[must_use]
    pub fn pg_of(&self, object: u128, stripe: u64) -> PgKey {
        let key = stripe_key(self.consumer, object, stripe);
        (self.consumer, pg_of(key, self.pool.groups_per_tablet()))
    }

    /// The pool's view at a generation, its pin's or one made once and kept
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation
    fn view_at(&self, generation: u64) -> Arc<View> {
        if generation == self.generation {
            return self.view.clone();
        }
        self.views
            .borrow_mut()
            .entry(generation)
            .or_insert_with(|| {
                Arc::new(View::new(&self.map.pool_map, &self.pool_name, &self.pool, generation))
            })
            .clone()
    }

    /// The chunks and holders of some slices at a generation, by position
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `object` - The object
    /// * `stripe` - The stripe
    /// * `slices` - Each position's slice
    /// * `generation` - The generation the slices are named at
    fn chunks_of(
        &self,
        pg: PlacementGroup,
        object: u128,
        stripe: u64,
        slices: &[SliceRef],
        generation: u64,
    ) -> Result<Vec<(ChunkId, Holder)>, String> {
        slices
            .iter()
            .enumerate()
            .map(|(position, slice)| {
                let device = self
                    .map
                    .pool_map
                    .devices
                    .get(&slice.device)
                    .ok_or_else(|| format!("device {} is not on the pool map", slice.device))?;
                let id = device
                    .slice_at(slice.index, generation)
                    .ok_or_else(|| format!("device {} has no slice {} at generation {generation}", slice.device, slice.index))?;
                Ok((
                    ChunkId {
                        consumer: self.consumer,
                        pg,
                        object,
                        stripe,
                        position: u8::try_from(position).unwrap_or(u8::MAX),
                    },
                    Holder {
                        node: device.node,
                        slice: id,
                    },
                ))
            })
            .collect()
    }

    /// The rule's answer for a placement group at a generation, in the rule's order (F88)
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `generation` - The generation
    ///
    /// # Errors
    ///
    /// A placement group the generation's view cannot place.
    pub fn answer_at(&self, pg: PlacementGroup, generation: u64) -> Result<Vec<SliceRef>, String> {
        self.view_at(generation)
            .place(self.consumer, pg)
            .map_err(|infeasible| format!("pool {} cannot place {pg:?} at generation {generation}: {infeasible:?}", self.pool_name))
    }

    /// Each position's slice of a placement group under its state, at the group's generation
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `state` - Its state
    ///
    /// # Errors
    ///
    /// A placement group the generation's view cannot place.
    pub fn slices_of(&self, pg: PlacementGroup, state: &PgView) -> Result<Vec<SliceRef>, String> {
        let generation = state.generation.unwrap_or(self.generation);
        let answer = self.answer_at(pg, generation)?;
        Ok(holders(&answer, state.positions.as_deref()))
    }

    /// The holder a slice a placement group left is reached at, if its device is still on the map
    /// (M20)
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    #[must_use]
    pub fn holder_of(&self, slice: SliceRef) -> Option<Holder> {
        let device = self.map.pool_map.devices.get(&slice.device)?;
        Some(Holder {
            node: device.node,
            slice: device.slice_any(slice.index)?,
        })
    }

    /// Whether a placement group places on a slice under its state: a position's holder at its
    /// generation, or a moving position's arrival (M20)
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `state` - Its state
    /// * `slice` - The slice
    ///
    /// # Errors
    ///
    /// A placement group a generation's view cannot place.
    pub fn places_on(&self, pg: PlacementGroup, state: &PgView, slice: SliceRef) -> Result<bool, String> {
        if self.slices_of(pg, state)?.contains(&slice) {
            return Ok(true);
        }
        let (_, arrived) = self.moving_slices(pg, state)?;
        Ok(arrived.contains(&slice))
    }

    /// The slices a moving placement group leaves: its old holders that hold no position once it
    /// switches, and its arrivals, which a move given up leaves holding whatever it copied (M20)
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `state` - Its state, with the move in it
    ///
    /// # Errors
    ///
    /// A placement group a generation's view cannot place.
    pub fn moving_slices(&self, pg: PlacementGroup, state: &PgView) -> Result<(Vec<SliceRef>, Vec<SliceRef>), String> {
        let old = self.slices_of(pg, state)?;
        let Some(moving) = &state.moving else {
            return Ok((Vec::new(), Vec::new()));
        };
        let next = self.answer_at(pg, moving.generation)?;
        let new = crate::shared::pool_placement::holders(&next, Some(&moving.positions));
        // a slice in both holds a position either way
        let left = old.iter().filter(|slice| !new.contains(slice)).copied().collect();
        let arrived = new.iter().filter(|slice| !old.contains(slice)).copied().collect();
        Ok((left, arrived))
    }

    /// Whether the pool's placement groups may sit anywhere but its pin: its target moved past it,
    /// so a group this shard never read may have moved, and is read rather than taken at its base
    #[must_use]
    pub fn may_have_moved(&self) -> bool {
        self.map
            .pool_map
            .target_of(&self.pool_name)
            .is_some_and(|target| target > self.generation)
    }

    /// The generation a placement group's chunks sit under, its pool's pin until it moved
    ///
    /// # Arguments
    ///
    /// * `state` - Its state
    #[must_use]
    pub fn generation_of(&self, state: &PgView) -> u64 {
        state.generation.unwrap_or(self.generation)
    }

    /// A stripe's chunks under its placement group's state: each position's holder at the group's
    /// generation, and while it moves, each moving position's arrival
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `stripe` - The stripe
    /// * `state` - The placement group's state
    ///
    /// # Errors
    ///
    /// A stripe a generation's view cannot place, or a slice the map does not name.
    pub fn stripe_set(&self, object: u128, stripe: u64, state: &PgView) -> Result<StripeSet, String> {
        self.stripe_set_in(self.pg_of(object, stripe).1, object, stripe, state)
    }

    /// A stripe's chunks in a placement group named, under the group's state
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group the stripe falls in
    /// * `object` - The object
    /// * `stripe` - The stripe
    /// * `state` - The placement group's state
    ///
    /// # Errors
    ///
    /// A stripe a generation's view cannot place, or a slice the map does not name.
    pub fn stripe_set_in(
        &self,
        pg: PlacementGroup,
        object: u128,
        stripe: u64,
        state: &PgView,
    ) -> Result<StripeSet, String> {
        let key = (self.consumer, pg);
        // each position's slice at the group's generation, through the positions a move recorded
        let generation = state.generation.unwrap_or(self.generation);
        let answer = self
            .view_at(generation)
            .place(self.consumer, pg)
            .map_err(|infeasible| format!("pool {} cannot place stripe {stripe} at generation {generation}: {infeasible:?}", self.pool_name))?;
        let slices = holders(&answer, state.positions.as_deref());
        let mut targets = self.chunks_of(pg, object, stripe, &slices, generation)?;
        let width = targets.len();
        // and each moving position's arrival, the slice it has at the generation it moves to
        if let Some(moving) = &state.moving {
            let next = self
                .view_at(moving.generation)
                .place(self.consumer, pg)
                .map_err(|infeasible| format!("pool {} cannot place stripe {stripe} at generation {}: {infeasible:?}", self.pool_name, moving.generation))?;
            let arriving = holders(&next, Some(&moving.positions));
            let arrivals = self.chunks_of(pg, object, stripe, &arriving, moving.generation)?;
            for (position, arrival) in arrivals.into_iter().enumerate() {
                // a position whose slice stays is no arrival
                if targets.get(position).is_some_and(|(_, holder)| *holder != arrival.1) {
                    targets.push(arrival);
                }
            }
        }
        Ok(StripeSet {
            key,
            epoch: state.epoch,
            width,
            targets,
        })
    }

    /// How many of a stripe's chunks an acknowledged write needs current: `k + f`
    #[must_use]
    pub fn needed(&self) -> usize {
        let (data, _) = self.pool.redundancy.chunks();
        usize::from(data) + usize::from(self.pool.losses())
    }

    /// The size below which a write in place rides inside its commit, for this pool here
    ///
    /// The pool's rule for its devices' kind, and none at all where the rows' own groups would
    /// not survive the pool's `f` losses, since until the clear a small write's bytes are durable
    /// only where its commit is ([S7](../../../../../docs/src/object-storage/write-path.md#small-writes)).
    #[must_use]
    pub fn small_write_threshold(&self) -> u64 {
        // every device the pool places on flushes a volatile cache, or none rides its commit
        let volatile = self.view.devices.iter().all(|device| {
            self.map
                .pool_map
                .devices
                .get(&device.id)
                .and_then(|record| record.era_at(self.generation))
                .is_some_and(|era| era.write_back)
        });
        // a cluster's rows survive what a majority of their copies survives
        let survives = if self.map.members.is_empty() {
            true
        } else {
            (self.map.desired_rf.saturating_sub(1) / 2) as u8 >= self.pool.losses()
        };
        if survives {
            self.pool.small_write_threshold(volatile)
        } else {
            0
        }
    }
}

/// A pool's readiness at a generation, from the map alone: nobody's space is judged here
///
/// # Arguments
///
/// * `map` - The map
/// * `pool_name` - The pool's name
/// * `pool` - The pool
/// * `generation` - The generation
#[must_use]
pub(crate) fn readiness(map: &TabletMap, pool_name: &str, pool: &PoolSpec, generation: u64) -> PoolReadiness {
    // a standalone node's map names no members, and its own devices are up
    let standalone = map.members.is_empty();
    readiness_at(
        &map.pool_map,
        pool_name,
        pool,
        &BTreeMap::new(),
        |node| standalone || map.is_up(node),
        generation,
    )
}
