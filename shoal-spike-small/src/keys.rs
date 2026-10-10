//! The keys a cell writes, chosen in the tablets of the groups it is aimed at
//!
//! The form of X10's (`shoal-spike-rows/src/keys.rs`). Every key is numbered within a space, and
//! every cell writes in a space of its own, so no cell ever finds a row another left. A cell gives
//! each worker [`KEYS_PER_WORKER`] keys of its own, which it writes in turn, so no two workers ever
//! have a write to one row in flight and a conditional commit is never refused by a sibling. Each
//! key also names a chunk slot on every holder, the same slot in every cell, so a holder's chunks
//! are written ahead once and every apply and fold lands in a file already allocated.
//!
//! A tablet group serves every tablet whose replica set is one ordered set of shards, and a
//! tablet is the top twelve bits of a key's partition hash (`Ring::tablet_of`). A cell aimed at
//! some groups skips every number whose key hashes outside their tablets.

use std::sync::Arc;

use shoal::server::ring::{Ring, TABLET_COUNT};
use shoal::shared::traits::PartitionKeySupport;

use crate::stats::mix;
use crate::{StripeMeta, StripeRow};

/// A stripe row's key: its consumer, its object and its index
pub type StripeKey = (u64, u128, u64);

/// How many keys each worker writes in turn
///
/// Enough that a worker never writes a key again while the holders still apply its last write
/// to it, at any depth: a worker's own deferred work is bounded by its keys.
pub const KEYS_PER_WORKER: usize = 8;

/// The most workers a cell runs, which is the deepest depth
pub const MAX_WORKERS: usize = 32;

/// How many chunk slots every holder keeps: one a key of the deepest cell
pub const SLOTS: u32 = (KEYS_PER_WORKER * MAX_WORKERS) as u32;

/// The stripe key numbered `n` in a space
///
/// The space is the consumer, and every number is an object of its own, its id spread over all
/// 128 bits as a minted id would be.
///
/// # Arguments
///
/// * `space` - The space, which is the consumer's id
/// * `n` - The key's number in it
#[must_use]
pub fn stripe_key(space: u64, n: u64) -> StripeKey {
    // the object's id spread over 128 bits
    let high = mix(space.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ n);
    let low = mix(high ^ n.rotate_left(17));
    (space, (u128::from(high) << 64) | u128::from(low), n % 16)
}

/// The row key numbered `n` in a space
///
/// SplitMix64's output function is a bijection, so two numbers below 2^40 in two spaces never
/// share a key.
///
/// # Arguments
///
/// * `space` - The space
/// * `n` - The key's number in it
#[must_use]
pub fn row_key(space: u64, n: u64) -> u64 {
    // the space in the high bits, the number in the low, mixed
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

/// The partition hash a row key routes by, as the client and every node compute it
///
/// # Arguments
///
/// * `key` - The key
#[must_use]
pub fn row_hash(key: u64) -> u64 {
    <StripeRow as PartitionKeySupport>::get_partition_key_from_values(&key)
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

/// A set of tablets: the ones the groups a cell is aimed at serve
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

    /// Every tablet
    #[must_use]
    pub fn all() -> Self {
        Tablets {
            member: vec![true; TABLET_COUNT],
        }
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

/// One key a worker writes: the stripe and the row that stand for it, and its slot
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Key {
    /// The stripe row the staged and inline paths commit to
    pub stripe: StripeKey,
    /// The row the first path overwrites
    pub row: u64,
    /// The chunk slot every holder keeps for it
    pub slot: u32,
}

/// Every worker's keys for a cell: `workers` hands of [`KEYS_PER_WORKER`]
///
/// Stripe keys land in `stripes`' tablets and row keys in `rows`', each numbered from zero in the
/// cell's space; worker `w`'s `k`th key has slot `w × KEYS_PER_WORKER + k`.
///
/// # Arguments
///
/// * `space` - The cell's space
/// * `workers` - How many workers
/// * `stripes` - The tablets the stripe keys must land in
/// * `rows` - The tablets the row keys must land in
#[must_use]
pub fn hands(space: u64, workers: usize, stripes: &Arc<Tablets>, rows: &Arc<Tablets>) -> Vec<Vec<Key>> {
    // the first keys of the space that land where the cell is aimed, of each kind
    let count = workers * KEYS_PER_WORKER;
    let stripe_keys: Vec<StripeKey> = (0..)
        .map(|n| stripe_key(space, n))
        .filter(|key| stripes.holds(stripe_hash(key)))
        .take(count)
        .collect();
    let row_keys: Vec<u64> = (0..)
        .map(|n| row_key(space, n))
        .filter(|key| rows.holds(row_hash(*key)))
        .take(count)
        .collect();
    // dealt out a worker at a time, each key with its slot
    (0..workers)
        .map(|worker| {
            (0..KEYS_PER_WORKER)
                .map(|k| {
                    let at = worker * KEYS_PER_WORKER + k;
                    Key {
                        stripe: stripe_keys[at],
                        row: row_keys[at],
                        slot: u32::try_from(at).expect("a slot fits"),
                    }
                })
                .collect()
        })
        .collect()
}

/// A cell's space: a number no other cell of any round uses
///
/// # Arguments
///
/// * `round` - The round
/// * `size` - The write's size
/// * `depth` - The cell's depth
/// * `path` - The path's index
#[must_use]
pub fn space(round: u32, size: usize, depth: usize, path: usize) -> u64 {
    // the four facts packed, then mixed so neighbouring cells are far apart; kept below 2^24 so
    // `row_key`'s shift keeps the space and the number apart
    mix((u64::from(round) << 40) ^ ((size as u64) << 8) ^ ((depth as u64) << 2) ^ path as u64) >> 40
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// A key's hash from its values is the hash of the row that holds it
    #[test]
    fn a_keys_hash_is_its_rows() {
        // a stripe row and its key
        let key = stripe_key(7, 12_345);
        let row = crate::shape::stripe_row(key, 0);
        assert_eq!(stripe_hash(&key), row.get_partition_key());
        // and a row of bytes
        let row = crate::shape::row(row_key(9, 777), 1, 4096);
        assert_eq!(row_hash(row.key), row.get_partition_key());
    }

    /// Every worker's keys land in their tablets, no two keys or slots are shared, and the
    /// slots of the deepest cell are every slot a holder keeps
    #[test]
    fn hands_share_no_key_or_slot() {
        // a tenth of the tablets for stripes, another tenth for rows
        let stripes = Arc::new(Tablets::of(&(0..4096).filter(|t| t % 10 == 3).collect::<Vec<u16>>()));
        let rows = Arc::new(Tablets::of(&(0..4096).filter(|t| t % 10 == 7).collect::<Vec<u16>>()));
        let hands = hands(space(1, 4096, 32, 1), MAX_WORKERS, &stripes, &rows);
        assert_eq!(hands.len(), MAX_WORKERS);
        let mut seen_stripes = HashSet::new();
        let mut seen_rows = HashSet::new();
        let mut seen_slots = HashSet::new();
        for key in hands.iter().flatten() {
            assert!(stripes.holds(stripe_hash(&key.stripe)));
            assert!(rows.holds(row_hash(key.row)));
            assert!(seen_stripes.insert(key.stripe), "a stripe was dealt twice");
            assert!(seen_rows.insert(key.row), "a row was dealt twice");
            assert!(seen_slots.insert(key.slot), "a slot was dealt twice");
            assert!(key.slot < SLOTS);
        }
        assert_eq!(seen_slots.len(), SLOTS as usize);
    }

    /// Two cells never share a space, and a space keeps clear of a row key's number
    #[test]
    fn cells_have_spaces_of_their_own() {
        let mut seen = HashSet::new();
        for round in 1..=4 {
            for size in [4096, 8192, 16_384, 32_768, 65_536, 131_072, 262_144] {
                for depth in [1, 32] {
                    for path in 0..4 {
                        let space = space(round, size, depth, path);
                        assert!(space < 1 << 24);
                        assert!(seen.insert(space), "two cells share space {space}");
                    }
                }
            }
        }
    }

    /// A tablet is the top twelve bits of the hash, as the ring takes it
    #[test]
    fn a_tablet_is_the_top_twelve_bits() {
        assert_eq!(tablet_of(0xABC0_0000_0000_0001), 0xABC);
        assert_eq!(TABLET_COUNT, 4096);
    }
}
