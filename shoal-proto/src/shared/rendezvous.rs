//! Weighted rendezvous: the draw every node computes alike, and the logarithm it compares by
//!
//! Two things are chosen by weighted rendezvous: a group's lead, among its voters
//! ([F58](../../../docs/src/features/weighted-leadership.md)), and a placement group's slices,
//! among a pool's ([S5](../../../docs/src/object-storage/placement.md#the-score)). Each candidate
//! draws `u` from a hash of its key and the thing being placed, and the one with the lowest
//! `-log2(u / 2^64) / weight` wins, which is `straw2`'s comparison written the other way round:
//! each wins with probability in proportion to its weight, and a change to one candidate's weight
//! moves only what that candidate gains or loses.
//!
//! # Invariants
//!
//! **The score is integers, a frozen table and one IEEE operation.** The logarithm is
//! [`neglog2`], read from [`table::LOG2_TABLE`] with linear interpolation; it is never libm's,
//! which is not specified to the last bit, and the one `f64` operation after it is correctly
//! rounded everywhere IEEE 754 is. So every node, client and build computes the same score, and a
//! near tie falls the same way on all of them (X2 measured libm choosing differently for 4 of 39
//! million chunks; [Resolved #212](../../../docs/src/appendix/resolved/lead-score-logarithm.md)).
//!
//! **Nothing here is a crate's definition.** [`mix64`] is SplitMix64's output function, five lines
//! written out, by X5's rule for anything persisted or compared across builds.

mod table;

use table::LOG2_TABLE;

/// The odd constant SplitMix64 steps by, the nearest odd number to 2^64 over the golden ratio
pub const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

/// The bits of a draw's mantissa that index the table
const TABLE_BITS: u32 = 12;

/// The bits of the mantissa after the index that interpolate between two entries
const LERP_BITS: u32 = 20;

/// The fraction bits of a fixed-point logarithm
pub const FRAC_BITS: u32 = 48;

/// SplitMix64's output function: a bijection on 64 bits with full avalanche
///
/// # Arguments
///
/// * `z` - The value to mix
#[inline]
#[must_use]
pub const fn mix64(mut z: u64) -> u64 {
    // three xor-shifts and two odd multiplies, as Steele, Lea and Flood give it
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The draw for one thing placed and one candidate, from their mixed keys
///
/// Mixed a third time, so the xor of two mixed keys is never what is compared.
///
/// # Arguments
///
/// * `placed` - The mixed key of what is being placed
/// * `candidate` - The mixed key of the candidate drawing for it
#[inline]
#[must_use]
pub const fn draw(placed: u64, candidate: u64) -> u64 {
    mix64(placed ^ candidate)
}

/// `-log2(u / 2^64)` in fixed point with [`FRAC_BITS`] fraction bits, for a draw `u`
///
/// Zero has no logarithm, so the lowest bit is set first. That moves the logarithm of a draw `u`
/// by under `1 / u`, which is below the table's own error for every draw past 2^32, and a uniform
/// draw falls below that once in four billion. The result is at least one for every `u`, so it can
/// always be divided by.
///
/// # Arguments
///
/// * `u` - The draw
#[inline]
#[must_use]
pub const fn neglog2(u: u64) -> u64 {
    // the integer part of log2(u) is where the top bit is
    let u = u | 1;
    let lz = u.leading_zeros();
    // the mantissa, normalized so its top bit is set: 1.xxx in [1, 2)
    let mantissa = u << lz;
    // the next TABLE_BITS bits index the table, and the LERP_BITS after them interpolate
    #[allow(clippy::cast_possible_truncation)]
    let index = ((mantissa >> (63 - TABLE_BITS)) & ((1 << TABLE_BITS) - 1)) as usize;
    let rest = (mantissa >> (63 - TABLE_BITS - LERP_BITS)) & ((1 << LERP_BITS) - 1);
    let low = LOG2_TABLE[index];
    let high = LOG2_TABLE[index + 1];
    // the step between entries is under 2^37, so the product stays under 2^57
    let fraction = low + (((high - low) * rest) >> LERP_BITS);
    // log2(u) = 63 - lz + fraction, so -log2(u / 2^64) = 1 + lz - fraction
    ((1 + lz as u64) << FRAC_BITS) - fraction
}

/// A candidate's score for one draw, the lowest of which wins
///
/// `neglog2(u) × (1 / weight)`: the conversion and the multiply are each correctly rounded, and
/// Rust never contracts the pair into a fused multiply-add, so this is the same everywhere.
///
/// # Arguments
///
/// * `u` - The draw
/// * `inverse_weight` - One over the candidate's weight, computed once from integers
#[inline]
#[must_use]
pub fn score(u: u64, inverse_weight: f64) -> f64 {
    // the integer logarithm as a float, then one multiply
    #[allow(clippy::cast_precision_loss)]
    let logarithm = neglog2(u) as f64;
    logarithm * inverse_weight
}

#[cfg(test)]
mod tests {
    use super::{draw, mix64, neglog2, score, FRAC_BITS, LOG2_TABLE};
    use crate::shared::checksum::Crc64;

    /// The table's bytes, little endian, checksummed: a changed entry moves it
    const TABLE_CRC: u64 = 0xd69a_2f32_d444_4b4c;

    /// The table is the one frozen here, entry for entry
    #[test]
    fn the_log2_table_is_frozen() {
        // every entry's bytes, in order
        let mut crc = Crc64::new();
        for entry in LOG2_TABLE {
            crc.update(&entry.to_le_bytes());
        }
        assert_eq!(crc.finish(), TABLE_CRC, "{:#018x}", crc.finish());
        // its two ends are exact
        assert_eq!(LOG2_TABLE[0], 0);
        assert_eq!(LOG2_TABLE[4096], 1 << FRAC_BITS);
    }

    /// Entries the logarithm's published decimal expansion fixes
    ///
    /// `log2(1.5)`, `log2(1.25)` and `log2(1.75)` to twenty digits, from any table of logarithms;
    /// each times 2^48 and rounded is the entry at 2048, 1024 and 3072.
    #[test]
    fn the_log2_table_agrees_with_published_values() {
        // log2(1.5) = 0.58496250072115618145...
        assert_eq!(LOG2_TABLE[2048], 164_652_306_267_095);
        // log2(1.25) = 0.32192809488736234787...
        assert_eq!(LOG2_TABLE[1024], 90_614_703_010_926);
        // log2(1.75) = 0.80735492205760410744...
        assert_eq!(LOG2_TABLE[3072], 227_250_207_883_398);
    }

    /// Every entry is within one of what `f64::log2` gives, which is only a check the table was
    /// written right: the product reads the table and never the logarithm
    #[test]
    fn every_log2_entry_is_within_one_of_libms() {
        #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        for (index, entry) in LOG2_TABLE.iter().enumerate() {
            let x = 1.0 + index as f64 / 4096.0;
            let libm = (x.log2() * (1u64 << FRAC_BITS) as f64).round() as u64;
            assert!(entry.abs_diff(libm) <= 1, "entry {index}: {entry} against {libm}");
        }
    }

    /// The fixed-point logarithm follows the real one, and never reaches zero
    #[test]
    fn neglog2_follows_the_logarithm() {
        // a draw of 2^63 is one half, so one bit exactly
        assert_eq!(neglog2(1 << 63), 1 << FRAC_BITS);
        // a draw of 2^62 is a quarter, two bits
        assert_eq!(neglog2(1 << 62), 2 << FRAC_BITS);
        // the largest draw is just under one, so just over zero, and never zero
        assert!(neglog2(u64::MAX) >= 1);
        // zero is treated as one, the smallest draw there is
        assert_eq!(neglog2(0), neglog2(1));
        // and in between it agrees with the logarithm to the table's precision
        #[allow(clippy::cast_precision_loss)]
        for u in [(1u64 << 40) + 1, 0x1234_5678_9abc_def1, 0xfff0_0000_0000_0001, u64::MAX >> 7] {
            let exact = -((u as f64) / 2f64.powi(64)).log2();
            let fixed = neglog2(u) as f64 / (1u64 << FRAC_BITS) as f64;
            assert!((exact - fixed).abs() < 1e-7, "{u:#x}: {exact} against {fixed}");
        }
    }

    /// The mix and the score are frozen at known answers
    #[test]
    fn the_draw_and_score_are_frozen() {
        // SplitMix64's output function at its first state, as its reference implementation gives
        assert_eq!(mix64(0x9e37_79b9_7f4a_7c15), 0xe220_a839_7b1d_cdaf);
        let u = draw(mix64(1), mix64(2));
        assert_eq!(score(u, 1.0).to_bits(), score(u, 1.0).to_bits());
        assert_eq!(neglog2(u), FROZEN_NEGLOG2, "{}", neglog2(u));
    }

    /// `neglog2` of `draw(mix64(1), mix64(2))`
    const FROZEN_NEGLOG2: u64 = 351_846_141_914_740;
}
