//! The keys a cell writes, and keys chosen for the tablets of one group
//!
//! Every key is numbered within a space, and every cell writes in a space of its own, so no two
//! cells ever touch one row and a cell that needs rows nobody has read finds them. Within a
//! cell, worker `w` of `d` takes numbers `w, w + d, w + 2d, …`, so no two workers ever have a
//! write to one row in flight and a conditional update is never refused by a sibling.
//!
//! A tablet group serves every tablet whose replica set is one ordered set of shards, and a
//! tablet is the top twelve bits of a key's partition hash (`Ring::tablet_of`). So a cell aimed
//! at one group skips every number whose key hashes outside that group's tablets: about one in
//! eighteen lands inside on the lab, three nodes of six shards.

use shoal::server::ring::{Ring, TABLET_COUNT};
use shoal::shared::traits::PartitionKeySupport;

use crate::stats::mix;
use crate::{ObjectMeta, StripeMeta};

/// A stripe row's key: its consumer, its object and its index
pub type StripeKey = (u64, u128, u64);

/// How many stripes an object's numbered keys are cut into, so stripes share objects
pub const STRIPES_PER_OBJECT: u64 = 16;

/// The stripe key numbered `n` in a space
///
/// The space is the consumer, and every sixteen numbers share an object, whose id is spread
/// over all 128 bits as a minted id would be.
///
/// # Arguments
///
/// * `space` - The space, which is the consumer's id
/// * `n` - The key's number in it
#[must_use]
pub fn stripe_key(space: u64, n: u64) -> StripeKey {
    // the object this number falls in, and its id spread over 128 bits
    let object = n / STRIPES_PER_OBJECT;
    let high = mix(space.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ object);
    let low = mix(high ^ object.rotate_left(17));
    (space, (u128::from(high) << 64) | u128::from(low), n % STRIPES_PER_OBJECT)
}

/// The object row key numbered `n` in a space: the stand-in for a path's hash
///
/// SplitMix64's output function is a bijection, so two numbers below 2^40 in two spaces never
/// share a key.
///
/// # Arguments
///
/// * `space` - The space
/// * `n` - The key's number in it
#[must_use]
pub fn object_key(space: u64, n: u64) -> u64 {
    // the space in the high bits, the number in the low, mixed into a path's hash
    mix((space << 40) ^ n)
}

/// The partition hash a stripe key routes by, as the client and every node compute it
///
/// # Arguments
///
/// * `key` - The key
#[must_use]
pub fn stripe_hash(key: &StripeKey) -> u64 {
    <StripeMeta as PartitionKeySupport>::get_partition_key_from_values(key)
}

/// The partition hash an object key routes by, as the client and every node compute it
///
/// # Arguments
///
/// * `key` - The key
#[must_use]
pub fn object_hash(key: u64) -> u64 {
    <ObjectMeta as PartitionKeySupport>::get_partition_key_from_values(&key)
}

/// The tablet a partition hash lands in
///
/// # Arguments
///
/// * `hash` - The partition hash
#[must_use]
pub fn tablet_of(hash: u64) -> u16 {
    // the ring's own function, so a key aimed at a group lands where the nodes route it
    u16::try_from(Ring::tablet_of(hash)).expect("a tablet fits sixteen bits")
}

/// A set of tablets: the ones one group serves
#[derive(Debug, Clone)]
pub struct Tablets {
    /// Whether each tablet is in the set, by tablet
    member: Vec<bool>,
}

impl Tablets {
    /// The set of these tablets
    ///
    /// # Arguments
    ///
    /// * `tablets` - The tablets
    #[must_use]
    pub fn of(tablets: &[u16]) -> Self {
        // a flag for every tablet, set for the ones named
        let mut member = vec![false; TABLET_COUNT];
        for tablet in tablets {
            member[usize::from(*tablet)] = true;
        }
        Tablets { member }
    }

    /// Whether a partition hash lands in one of these tablets
    ///
    /// # Arguments
    ///
    /// * `hash` - The partition hash
    #[must_use]
    pub fn holds(&self, hash: u64) -> bool {
        self.member[usize::from(tablet_of(hash))]
    }
}

/// Which keys a worker takes: its numbers in a space, and the tablets they must land in
#[derive(Debug, Clone)]
pub struct Cursor {
    /// The space
    space: u64,
    /// The next number to try
    next: u64,
    /// How far apart this worker's numbers are: the cell's workers
    stride: u64,
    /// The tablets a key must land in, or none for any
    tablets: Option<std::sync::Arc<Tablets>>,
}

impl Cursor {
    /// A worker's keys in a space
    ///
    /// # Arguments
    ///
    /// * `space` - The space
    /// * `worker` - The worker's place among the cell's workers
    /// * `workers` - How many workers the cell has
    /// * `tablets` - The tablets every key must land in, or none for any
    #[must_use]
    pub fn new(
        space: u64,
        worker: usize,
        workers: usize,
        tablets: Option<std::sync::Arc<Tablets>>,
    ) -> Self {
        Cursor {
            space,
            next: worker as u64,
            stride: workers.max(1) as u64,
            tablets,
        }
    }

    /// The next stripe key this worker takes
    pub fn next_stripe(&mut self) -> StripeKey {
        loop {
            // this worker's next number, and the key it names
            let key = stripe_key(self.space, self.next);
            self.next += self.stride;
            // kept when it lands where the cell is aimed
            match &self.tablets {
                Some(tablets) if !tablets.holds(stripe_hash(&key)) => continue,
                _ => return key,
            }
        }
    }

    /// The next object key this worker takes
    pub fn next_object(&mut self) -> u64 {
        loop {
            // this worker's next number, and the key it names
            let key = object_key(self.space, self.next);
            self.next += self.stride;
            // kept when it lands where the cell is aimed
            match &self.tablets {
                Some(tablets) if !tablets.holds(object_hash(key)) => continue,
                _ => return key,
            }
        }
    }
}

/// The first `count` stripe keys of a space, numbered from zero, sorted by the group they land in
///
/// # Arguments
///
/// * `space` - The space
/// * `count` - How many keys were written in it
/// * `groups` - Each group's tablets
#[must_use]
pub fn stripes_by_group(space: u64, count: u64, groups: &[Tablets]) -> Vec<Vec<StripeKey>> {
    // a list for every group, filled in number order
    let mut by_group = vec![Vec::new(); groups.len()];
    for n in 0..count {
        let key = stripe_key(space, n);
        let hash = stripe_hash(&key);
        if let Some(index) = groups.iter().position(|tablets| tablets.holds(hash)) {
            by_group[index].push(key);
        }
    }
    by_group
}

/// The first `count` object keys of a space, numbered from zero, sorted by the group they land in
///
/// # Arguments
///
/// * `space` - The space
/// * `count` - How many keys were written in it
/// * `groups` - Each group's tablets
#[must_use]
pub fn objects_by_group(space: u64, count: u64, groups: &[Tablets]) -> Vec<Vec<u64>> {
    // a list for every group, filled in number order
    let mut by_group = vec![Vec::new(); groups.len()];
    for n in 0..count {
        let key = object_key(space, n);
        let hash = object_hash(key);
        if let Some(index) = groups.iter().position(|tablets| tablets.holds(hash)) {
            by_group[index].push(key);
        }
    }
    by_group
}

#[cfg(test)]
mod tests {
    use super::*;
    use shoal::shared::traits::PartitionKeySupport;

    /// A key's hash from its values is the hash of the row that holds it
    ///
    /// The cells aim at a group by hashing keys the way the nodes route rows; a row's own
    /// `get_partition_key` is what an insert is routed by.
    #[test]
    fn a_keys_hash_is_its_rows() {
        // a stripe row and its key
        let key = stripe_key(7, 12_345);
        let row = crate::shape::stripe_row(key, 0);
        assert_eq!(stripe_hash(&key), row.get_partition_key());
        // and an object row
        let key = object_key(9, 777);
        let row = crate::shape::object_row(key, 0, 0, 777);
        assert_eq!(object_hash(key), row.get_partition_key());
    }

    /// A tablet is the top twelve bits of the hash, as the ring takes it
    #[test]
    fn a_tablet_is_the_top_twelve_bits() {
        let hash = 0xABC0_0000_0000_0001;
        assert_eq!(tablet_of(hash), 0xABC);
        assert_eq!(TABLET_COUNT, 4096);
    }

    /// A cursor aimed at some tablets only yields keys in them, and two workers never share one
    #[test]
    fn a_cursor_stays_in_its_tablets_and_its_lane() {
        // a set of a tenth of the tablets
        let chosen: Vec<u16> = (0..4096).filter(|tablet| tablet % 10 == 3).collect();
        let tablets = std::sync::Arc::new(Tablets::of(&chosen));
        let mut first = Cursor::new(1, 0, 2, Some(tablets.clone()));
        let mut second = Cursor::new(1, 1, 2, Some(tablets.clone()));
        let mut seen = std::collections::HashSet::new();
        for _ in 0..500 {
            for key in [first.next_stripe(), second.next_stripe()] {
                assert!(tablets.holds(stripe_hash(&key)));
                assert!(seen.insert(key), "a key was taken twice");
            }
        }
    }

    /// Keys in two spaces never collide, and objects share sixteen stripes
    #[test]
    fn spaces_are_apart() {
        assert_ne!(object_key(1, 5), object_key(2, 5));
        assert_ne!(stripe_key(1, 5), stripe_key(2, 5));
        assert_eq!(stripe_key(1, 0).1, stripe_key(1, 15).1);
        assert_ne!(stripe_key(1, 0).1, stripe_key(1, 16).1);
    }
}
