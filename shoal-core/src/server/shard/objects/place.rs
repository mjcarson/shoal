//! Where a stripe's chunks are, and whether a pool takes a write
//!
//! A pool places its stripes at its pin: the generation the control group committed the first
//! time the pool was ready, which nothing moves until M16 moves a placement group from one
//! generation's slices to another's ([F86](../../../../../docs/src/features/replicated-pools.md)).
//! Every coordinator computes the same answer from the same map, through the eras that keep the
//! map's past, so a write and every later read of its stripe name the same holders.
//!
//! # Invariants
//!
//! **A stripe is placed at its pool's pin and nowhere else.** A device that joined since is not
//! placed on; one that failed or whose node is down still holds its position, and a write counts
//! it missed.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::server::map::TabletMap;
use crate::server::objects::Holder;
use crate::shared::objects::stripe_key;
use crate::shared::pool_placement::{pg_of, readiness_at, PoolReadiness, View};
use crate::shared::pools::{PoolSpec, Redundancy};
use crate::shared::protocol::object::ObjectOutcome;
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
    /// The map placed by
    map: Arc<TabletMap>,
}

impl Placement {
    /// A pool's placement, refused by name when the pool cannot take what is asked of it
    ///
    /// A read needs only the pin; a write needs the pool ready at it and of a kind M15 writes.
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
        let Some(generation) = map.pool_map.pinned_at(pool_name) else {
            let ready = readiness(&map, pool_name, pool, map.pool_map.generation);
            let why = if ready.short.is_empty() {
                "it has not been placed yet: its first readiness is not committed".to_string()
            } else {
                ready.short.join("; ")
            };
            return Err((ObjectOutcome::NotReady, format!("pool {pool_name} takes no writes: {why}")));
        };
        if write {
            // a write needs the pool's holders at its pin
            let ready = readiness(&map, pool_name, pool, generation);
            if !ready.ready() {
                return Err((
                    ObjectOutcome::NotReady,
                    format!("pool {pool_name} takes no writes: {}", ready.short.join("; ")),
                ));
            }
            // and a kind of pool a write reaches
            if !matches!(pool.redundancy, Redundancy::Replicas(_)) {
                return Err((
                    ObjectOutcome::Unsupported,
                    format!(
                        "pool {pool_name} is erasure coded, and no write reaches an erasure coded pool until M18"
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
            map,
        })
    }

    /// Each position of a stripe: its chunk, and the holder it is on
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `stripe` - The stripe
    ///
    /// # Errors
    ///
    /// A stripe the pool's view cannot place, or a slice its map does not name.
    pub fn stripe(&self, object: u128, stripe: u64) -> Result<Vec<(ChunkId, Holder)>, String> {
        // the stripe's placement group, a sub-range of the tablet whose group orders its row
        let key = stripe_key(self.consumer, object, stripe);
        let pg = pg_of(key, self.pool.groups_per_tablet());
        let slices = self
            .view
            .place(self.consumer, pg)
            .map_err(|infeasible| format!("pool {} cannot place stripe {stripe}: {infeasible:?}", self.pool_name))?;
        // each position's slice, as it was at the pin
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
                    .slice_at(slice.index, self.generation)
                    .ok_or_else(|| format!("device {} has no slice {} at generation {}", slice.device, slice.index, self.generation))?;
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

    /// The chunks a stripe has
    #[must_use]
    pub fn width(&self) -> usize {
        self.view.width
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
