//! The token/partition ring for Shoal
//!
//! This determines on what shards data lies.

use gxhash::GxHasher;
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::ops::Bound::{Excluded, Included};

use super::shard::ShardInfo;

/// Each node in the cluster should have 1000 parts of the ring
const RING_JUMP: u64 = u64::MAX / 1000;

/// The token/partition ring for Shoal
#[derive(Default, Clone)]
pub struct Ring {
    /// The ring detailing where shards should be at
    ring: BTreeMap<u64, usize>,
    /// A map of node info
    pub shards: Vec<ShardInfo>,
}

impl Ring {
    /// Add a shard to our ring
    pub fn add(&mut self, shard: ShardInfo) {
        // build a default hasher
        let mut hasher = GxHasher::default();
        // add our hards name
        hasher.write(shard.name.as_bytes());
        // add a shard to our ring
        self.shards.push(shard);
        // get this shards id in our shard list
        let shard_id = self.shards.len();
        // get our first vnode entry
        let mut vnode = hasher.finish();
        // calculate 1000 entries onto our ring
        for _ in 0..1000 {
            // add our ring jump to this entry
            vnode = vnode.wrapping_add(RING_JUMP);
            // add this virtual node to our ring
            self.ring.insert(vnode, shard_id - 1);
        }
    }

    /// Get a shard for this partition
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition to look for
    pub fn find_shard(&self, partition: u64) -> &ShardInfo {
        // get the index for the shard that contains our data
        let (_, index) = match self
            .ring
            .range((Included(&partition), Included(&u64::MAX)))
            .next()
        {
            Some((vnode, index)) => (vnode, index),
            None => self
                .ring
                .range((Included(&0), Excluded(&partition)))
                .next()
                .unwrap(),
        };
        &self.shards[*index]
    }
}
