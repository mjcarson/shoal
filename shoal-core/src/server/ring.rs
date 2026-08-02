//! The tablet map for Shoal
//!
//! This determines on what shards data lies.
//!
//! A partition key names a tablet, and a tablet names the shard that owns it. The second
//! step is a lookup in a table rather than a hash, which is the whole point: ownership is
//! data, so a tablet can be moved. A hash can only be recomputed, which is why the ring
//! this replaced could not express a migration, a replica set, or a rebalance away from a
//! shard that had grown hot.
//!
//! The map is built whole from the shard count before any shard starts, so no shard ever
//! routes against a partial one.

use tracing::{event, Level};

use super::shard::ShardInfo;
use super::ServerError;
use crate::server::errors::ShoalError;

/// The number of bits of a partition key that name its tablet
///
/// Taken from the top of the key rather than the bottom so that a tablet can later be
/// split in two by consuming one more bit: its keys stay contiguous and no other tablet
/// is disturbed. A tablet id taken modulo the tablet count could not be split at all.
const TABLET_BITS: u32 = 12;

/// The number of tablets the partition key space is cut into
///
/// This has to be far larger than any shard count for the split to be even, and small
/// enough that the map stays resident — 4096 `u16`s is 8 KiB.
const TABLET_COUNT: usize = 1 << TABLET_BITS;

/// The tablet map for Shoal
#[derive(Clone)]
pub struct Ring {
    /// The shard that owns each tablet, indexed by tablet id
    ///
    /// Stored rather than derived, so that a tablet can later be moved by writing to this
    /// map instead of by changing a hash every shard would have to agree about.
    tablets: Vec<u16>,
    /// A map of node info
    pub shards: Vec<ShardInfo>,
}

impl Ring {
    /// Build the tablet map for a node with this many shards
    ///
    /// Every shard builds the same map from the same shard count, so this is complete
    /// before a single query can be routed against it. That is what makes
    /// [`Ring::find_shard`] total.
    ///
    /// # Arguments
    ///
    /// * `shard_count` - The number of shards on this node
    ///
    /// # Errors
    ///
    /// This will fail if there are no shards to own tablets, or if there are more shards
    /// than a tablet can name.
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_core::server::ring::Ring;
    ///
    /// // every tablet is owned as soon as the map exists
    /// let ring = Ring::new(4)?;
    /// // so routing any key at all names a live shard
    /// ring.find_shard(1234);
    /// # Ok::<(), shoal_core::server::ServerError>(())
    /// ```
    pub fn new(shard_count: usize) -> Result<Self, ServerError> {
        // a node with no shards has nothing that could own a tablet
        if shard_count == 0 {
            return Err(ServerError::Shoal(ShoalError::NoShards));
        }
        // a tablet names its owner in a u16, so that is the most shards we can place
        if shard_count > usize::from(u16::MAX) {
            return Err(ServerError::Shoal(ShoalError::TooManyShards {
                shards: shard_count,
            }));
        }
        // build the info for every shard on this node, in shard id order
        let shards = (0..shard_count).map(ShardInfo::new).collect();
        // hand each tablet to a shard in turn, so the counts differ by at most one
        let tablets = (0..TABLET_COUNT)
            .map(|tablet| {
                // truncation cannot happen here since we rejected a larger shard count above
                #[allow(clippy::cast_possible_truncation)]
                let owner = (tablet % shard_count) as u16;
                owner
            })
            .collect();
        Ok(Ring { tablets, shards })
    }

    /// Get the tablet a partition key belongs to
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition to find the tablet for
    fn tablet_of(partition: u64) -> usize {
        // take the high bits of the key, which is what leaves room for a later split
        //
        // this cannot exceed TABLET_COUNT, since we keep only TABLET_BITS of the key
        #[allow(clippy::cast_possible_truncation)]
        let tablet = (partition >> (u64::BITS - TABLET_BITS)) as usize;
        tablet
    }

    /// Add a shard to our tablet map
    ///
    /// Every shard on this node is already placed by [`Ring::new`], so a join naming one
    /// of them is the broadcast arriving for a shard we built ourselves and is ignored.
    /// The seam is kept for the multi node case, where a join will name a shard this node
    /// did not build.
    ///
    /// # Arguments
    ///
    /// * `shard` - The shard to add
    pub fn add(&mut self, shard: ShardInfo) {
        // a shard we already placed is this nodes own join coming back to us
        if self.shards.iter().any(|known| known.name == shard.name) {
            return;
        }
        // anything else is a shard we cannot give tablets to, since moving a tablet needs
        // a rebalancer and its data moved with it, and neither exists yet
        event!(
            Level::WARN,
            msg = "Ignoring a join from an unknown shard, as rebalancing is unimplemented",
            shard = shard.name,
        );
    }

    /// Get a shard for this partition
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition to look for
    pub fn find_shard(&self, partition: u64) -> &ShardInfo {
        // find the tablet holding this partition
        let tablet = Self::tablet_of(partition);
        // get the shard that owns that tablet
        //
        // neither index can be out of bounds: `new` fills a tablet for every id this can
        // produce and rejects an empty shard list, and `add` never places an owner it
        // has no info for
        let owner = usize::from(self.tablets[tablet]);
        &self.shards[owner]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node with no shards has nothing that could own a tablet
    ///
    /// This is the state that used to panic when it was routed against. It is now
    /// unbuildable, which is why nothing below has to test routing against it.
    #[test]
    fn empty_ring_is_not_constructable() {
        // a map with no shards to own tablets is not a map we can build
        assert!(Ring::new(0).is_err());
    }

    /// Routing must answer for every key, including both ends of the space
    #[test]
    fn find_shard_handles_the_whole_key_space() {
        // build a map for a typical deployment
        let ring = Ring::new(16).expect("failed to build a ring");
        // both ends of the key space have to name a shard
        ring.find_shard(0);
        ring.find_shard(u64::MAX);
        // and so does everything in between
        for sample in 0..10_000u64 {
            // spread our samples over the whole key space
            ring.find_shard(sample.wrapping_mul(u64::MAX / 10_000));
        }
    }

    /// Tablets have to be split as evenly as the shard count allows
    ///
    /// This is what the vnode ring failed to do: its busiest shard owned 3.54x the mean.
    #[test]
    fn tablets_are_evenly_owned() {
        // check the shard counts a node is plausibly configured with
        for shard_count in [1, 2, 3, 7, 16, 31, 64] {
            // build the map for this many shards
            let ring = Ring::new(shard_count).expect("failed to build a ring");
            // count the tablets each shard was given
            let mut owned = vec![0usize; shard_count];
            // walk every tablet in the map
            for owner in &ring.tablets {
                // record that this shard owns another tablet
                owned[usize::from(*owner)] += 1;
            }
            // find the largest and smallest share handed out
            let max = *owned.iter().max().expect("no shards");
            let min = *owned.iter().min().expect("no shards");
            // an even split can only differ by the remainder, which is at most one tablet
            assert!(
                max - min <= 1,
                "{shard_count} shards own between {min} and {max} tablets",
            );
        }
    }

    /// No shard may be left without tablets to own
    #[test]
    fn every_shard_owns_a_tablet() {
        // check every shard count up to more than a large machine has
        for shard_count in 1..=64 {
            // build the map for this many shards
            let ring = Ring::new(shard_count).expect("failed to build a ring");
            // every shard has to appear somewhere in the map
            for id in 0..shard_count {
                // truncation cannot happen for a shard count this small
                #[allow(clippy::cast_possible_truncation)]
                let id = id as u16;
                // look for a tablet this shard was given
                assert!(
                    ring.tablets.contains(&id),
                    "shard {id} of {shard_count} owns no tablets",
                );
            }
        }
    }

    /// Two shards must agree about who owns a key without having to coordinate
    ///
    /// The ring this replaced stored an arrival order index, so two shards genuinely held
    /// different maps and agreeing needed an argument. Building from the shard count means
    /// the maps are identical instead.
    #[test]
    fn find_shard_is_stable_across_rings() {
        // build two maps the way two different shards would
        let left = Ring::new(16).expect("failed to build a ring");
        let right = Ring::new(16).expect("failed to build a ring");
        // both have to route every key the same way
        for sample in 0..10_000u64 {
            // spread our samples over the whole key space
            let key = sample.wrapping_mul(u64::MAX / 10_000);
            // the two maps have to name the same shard for it
            assert_eq!(
                left.find_shard(key).mesh_id(),
                right.find_shard(key).mesh_id(),
                "two rings disagree about who owns {key}",
            );
        }
    }

    /// A tablet id has to come from the top of the key, so a split stays incremental
    ///
    /// Splitting a tablet means consuming one more bit. That only divides the tablet
    /// itself if the bits below the id are the ones that vary within it.
    #[test]
    fn tablet_id_comes_from_the_high_bits() {
        // take a key at the base of some tablet
        let base = 0x1234_0000_0000_0000u64;
        // every key sharing its high bits has to share its tablet
        for low in 0..1000u64 {
            // vary only the bits below the tablet id
            assert_eq!(
                Ring::tablet_of(base),
                Ring::tablet_of(base | low),
                "a low bit changed which tablet {base} belongs to",
            );
        }
        // and changing the high bits has to leave the tablet behind
        assert_ne!(Ring::tablet_of(base), Ring::tablet_of(base << 4));
    }
}
