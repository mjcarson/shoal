//! The draw a rendezvous candidate scores an item by
//!
//! Placement is a persisted fact in all but name: a stripe chunk sits where the function said at
//! its generation, so every node has to compute the same answer from the same map, on every cpu
//! and from every build, for as long as the chunk is there. That rules out two things a first
//! implementation would reach for:
//!
//! - **a hash whose definition is its crate's code.** X5 found gxhash's output stable across cpus
//!   and builds but its two majors in disagreement ([checksums](../../../docs/src/object-storage/checksums.md)).
//!   The draw here is SplitMix64's output function, defined in five lines
//! - **libm's logarithm.** Weighted rendezvous compares `ln(u) / w` across items, and `ln` is not
//!   required to be correctly rounded, so two libms may disagree in the last bit and two nodes on
//!   a near tie may pick different slices. Ceph's `straw2` takes its logarithm from a fixed-point
//!   table (`crush_ln`) for this reason. This one does the same: a table of `log2(1 + i/4096)`
//!   with linear interpolation, and every operation after it a plain IEEE multiply, which is
//!   correctly rounded everywhere
//!
//! The table here is computed at start from `f64::log2`. An implementation freezes it as
//! constants, as Ceph's `crush_ln_table.h` does, and checks them against a published list.

/// The odd constant SplitMix64 steps by, the nearest to 2^64 over the golden ratio
pub const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

/// The bits of the mantissa the logarithm's table is indexed by
const TABLE_BITS: u32 = 12;

/// The bits of the mantissa after the index that interpolate between two entries
const LERP_BITS: u32 = 20;

/// The fraction bits of a fixed-point logarithm
const FRAC_BITS: u32 = 48;

/// SplitMix64's output function: a bijection on 64 bits with full avalanche
///
/// # Arguments
///
/// * `z` - The value to mix
#[inline]
#[must_use]
pub fn mix64(mut z: u64) -> u64 {
    // three xor-shifts and two odd multiplies, as Steele, Lea and Flood give it
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The key an item is drawn by in one round, mixed once so a lookup mixes only the pair
///
/// # Arguments
///
/// * `key` - The item's placement key
/// * `round` - The round of draws, zero for a candidate that draws once
#[inline]
#[must_use]
pub fn item_key(key: u64, round: u32) -> u64 {
    // each round is an independent draw over the same items
    mix64(key.wrapping_add(u64::from(round).wrapping_mul(GOLDEN)))
}

/// The key a placement group is drawn by, mixed once a lookup
///
/// # Arguments
///
/// * `pg` - The placement group's key
#[inline]
#[must_use]
pub fn group_key(pg: u64) -> u64 {
    // offset first, so a placement group key of zero does not mix to zero
    mix64(pg ^ GOLDEN)
}

/// The draw for one placement group and one item in one round, from their mixed keys
///
/// # Arguments
///
/// * `group` - The placement group's mixed key, from [`group_key`]
/// * `item` - The item's mixed key for the round, from [`item_key`]
#[inline]
#[must_use]
pub fn draw(group: u64, item: u64) -> u64 {
    // a third mix, so that the xor of two mixed keys is not what is compared
    mix64(group ^ item)
}

/// `-log2` of a draw taken as a fraction of 2^64, in fixed point
pub struct Log2Table {
    /// `log2(1 + i / 4096)` for `i` in `0..=4096`, with `FRAC_BITS` fraction bits
    entries: Vec<u64>,
}

impl Default for Log2Table {
    /// Build the table
    fn default() -> Self {
        Self::new()
    }
}

impl Log2Table {
    /// Build the table from `f64::log2`, which an implementation would freeze as constants
    #[must_use]
    pub fn new() -> Self {
        // one entry past the last index, so interpolation never reads beyond the end
        let scale = (1u64 << FRAC_BITS) as f64;
        let entries = (0..=(1u64 << TABLE_BITS))
            .map(|i| {
                let x = 1.0 + i as f64 / (1u64 << TABLE_BITS) as f64;
                // the last entry is exactly one, which the rounding below gives too
                (x.log2() * scale).round() as u64
            })
            .collect();
        Log2Table { entries }
    }

    /// `-log2(u / 2^64)` with `FRAC_BITS` fraction bits, for a draw `u`
    ///
    /// Zero has no logarithm, so the lowest bit is set first; the error that adds is below
    /// 2^-63 of the value, far under the table's own.
    ///
    /// # Arguments
    ///
    /// * `u` - The draw
    #[inline]
    #[must_use]
    pub fn neglog2(&self, u: u64) -> u64 {
        // the integer part of log2(u) is where the top bit is
        let u = u | 1;
        let lz = u.leading_zeros();
        // the mantissa, normalized so its top bit is set: 1.xxx in [1, 2)
        let m = u << lz;
        // the next TABLE_BITS bits index the table, and the LERP_BITS after them interpolate
        let index = ((m >> (63 - TABLE_BITS)) & ((1 << TABLE_BITS) - 1)) as usize;
        let rest = (m >> (63 - TABLE_BITS - LERP_BITS)) & ((1 << LERP_BITS) - 1);
        let lo = self.entries[index];
        let hi = self.entries[index + 1];
        // the step between entries is under 2^37, so the product stays under 2^57
        let frac = lo + (((hi - lo) * rest) >> LERP_BITS);
        // log2(u) = 63 - lz + frac, so -log2(u / 2^64) = 1 + lz - frac
        ((1 + u64::from(lz)) << FRAC_BITS) - frac
    }

    /// The score of a draw against an item's inverse weight; the lowest score wins
    ///
    /// # Arguments
    ///
    /// * `u` - The draw
    /// * `inv_weight` - One over the item's weight
    #[inline]
    #[must_use]
    pub fn score(&self, u: u64, inv_weight: f64) -> f64 {
        // the conversion and the multiply are each correctly rounded, so this is the same
        // everywhere IEEE 754 is; Rust never contracts the pair into a fused multiply-add
        self.neglog2(u) as f64 * inv_weight
    }
}

/// The one table every view reads, built on first use as an implementation's constants would be
#[must_use]
pub fn log2_table() -> &'static Log2Table {
    // built once for the process, as constants would be compiled in once
    static TABLE: std::sync::OnceLock<Log2Table> = std::sync::OnceLock::new();
    TABLE.get_or_init(Log2Table::new)
}

/// The score libm would give, for comparing with the table's: `-ln(u / 2^64) / w`
///
/// # Arguments
///
/// * `u` - The draw
/// * `inv_weight` - One over the item's weight
#[inline]
#[must_use]
pub fn libm_score(u: u64, inv_weight: f64) -> f64 {
    // the same shift of zero the table makes, so the two differ in the logarithm alone
    let x = (u | 1) as f64 / 18_446_744_073_709_551_616.0;
    -x.ln() * inv_weight
}

/// A seeded generator for everything the simulation mints: SplitMix64 itself
#[derive(Debug, Clone)]
pub struct SplitMix {
    /// The state, stepped by `GOLDEN` each draw
    state: u64,
}

impl SplitMix {
    /// A generator from a seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The seed
    #[must_use]
    pub fn new(seed: u64) -> Self {
        SplitMix { state: seed }
    }

    /// The next 64 bits
    pub fn next_u64(&mut self) -> u64 {
        // step, then mix the new state
        self.state = self.state.wrapping_add(GOLDEN);
        mix64(self.state)
    }

    /// A float in `[0, 1)`
    pub fn next_f64(&mut self) -> f64 {
        // the top 53 bits, which is every bit an f64 mantissa holds
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}
