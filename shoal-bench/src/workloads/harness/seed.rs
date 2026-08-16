//! Deterministic data, generated in process, so a workload needs nothing from disk
//!
//! The workload this replaces read a 65 MB TMDB CSV from an absolute path that was never in the
//! repository and that no script fetched, which meant a clean checkout could not reproduce the
//! macro layer at all. Everything a workload needs is now derived from one `u64` seed, so the same
//! seed produces byte identical rows on any machine and a difference between two captures is
//! machine noise rather than a difference in what was loaded.
//!
//! # Why this is hand rolled
//!
//! `rand` is not in the workspace lockfile, and this needs about fifteen lines of it. It also
//! needs the opposite of what `rand`'s defaults give: a generator that reaches for OS randomness
//! is exactly what a reproducible dataset must not do, so the useful part of the crate here is the
//! part that would have to be turned off. `shoal-bench/src/clock.rs` made the same trade for the
//! same reason.
//!
//! SplitMix64 is the choice because it is the shortest generator with a published constant set and
//! no state beyond a single `u64`. Nothing here is cryptographic and nothing here needs to be.

pub use crate::cli::Scale;

/// The odd increment SplitMix64 walks its state by, from the reference implementation
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// The first of SplitMix64's two mixing constants
const MIX_ONE: u64 = 0xBF58_476D_1CE4_E5B9;

/// The second of SplitMix64's two mixing constants
const MIX_TWO: u64 = 0x94D0_49BB_1331_11EB;

/// The alphabet sort keys and payloads are drawn from
///
/// Deliberately ASCII and deliberately 32 characters, so an index into it is five bits and a
/// generated string has no multi byte characters in it. A sort key that changed length depending
/// on the seed would make row width a function of the seed, which is the one thing a scale
/// parameter has to hold fixed.
const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz012345";

/// A reproducible source of pseudo random numbers
///
/// Cloning one and running both copies produces the same sequence twice, which is what lets a
/// workload generate the same rows in a setup phase and a measurement phase without keeping them.
#[derive(Debug, Clone)]
pub struct Seeded {
    /// The generator's whole state
    state: u64,
}

impl Seeded {
    /// Creates a generator from a seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The seed to start from, which fully determines every value that follows
    pub fn new(seed: u64) -> Self {
        Seeded { state: seed }
    }

    /// Creates a generator for one named stream of a seed
    ///
    /// Two parts of a workload that both want random values must not share a generator, or adding
    /// a draw to one silently changes what the other produces. Mixing the name into the seed gives
    /// each of them its own stream off the same seed instead.
    ///
    /// # Arguments
    ///
    /// * `seed` - The seed the whole run derives from
    /// * `stream` - What this stream is for, such as `"partition_keys"`
    pub fn stream(seed: u64, stream: &str) -> Self {
        // fold the name into the seed with the same mixing the generator itself uses, so two
        // names that differ in one byte start far apart rather than one step apart
        let mut mixed = seed;
        for byte in stream.as_bytes() {
            mixed = (mixed ^ u64::from(*byte)).wrapping_mul(MIX_ONE);
            mixed ^= mixed >> 29;
        }
        Seeded::new(mixed)
    }

    /// Creates the generator that belongs to one index, rather than one walked forward
    ///
    /// Everything a mixture generates - which key a query asks for, how wide a row is, whether a
    /// query is a read or a write - has to be a function of the query's index and of nothing else.
    /// A mixture is driven by several slots pulling from one shared cursor, so anything drawn in
    /// sequence would depend on which slot reached the cursor first, and two runs of one arm would
    /// send different queries. That is the opposite of what a repeatable benchmark is.
    ///
    /// The index is spread by [`GAMMA`] before it is folded in, because two indices one apart
    /// would otherwise seed two generators one apart.
    ///
    /// # Arguments
    ///
    /// * `seed` - The stream seed to address into
    /// * `index` - Which index's generator is wanted
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_bench::workloads::harness::seed::Seeded;
    ///
    /// // the same index gives the same value, however many times it is asked and in any order
    /// assert_eq!(Seeded::at(42, 7).next_u64(), Seeded::at(42, 7).next_u64());
    /// assert_ne!(Seeded::at(42, 7).next_u64(), Seeded::at(42, 8).next_u64());
    /// ```
    pub fn at(seed: u64, index: u64) -> Self {
        Seeded::new(seed ^ index.wrapping_mul(GAMMA))
    }

    /// Draws the next value
    pub fn next_u64(&mut self) -> u64 {
        // walk the state by the golden ratio increment, which is what makes the period full
        self.state = self.state.wrapping_add(GAMMA);
        // then mix the walked state, since the walk on its own is a counter and not random at all
        let mut mixed = self.state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(MIX_ONE);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(MIX_TWO);
        mixed ^ (mixed >> 31)
    }

    /// Draws a value below a bound, without the modulo bias of `next_u64() % bound`
    ///
    /// # Arguments
    ///
    /// * `bound` - One past the largest value that may be returned, which must not be zero
    pub fn below(&mut self, bound: u64) -> u64 {
        // a bound of zero has no valid answer, and returning one anyway would be a silent wrong
        // index rather than a loud one
        assert!(bound > 0, "cannot draw a value below zero");
        // Lemire's method: take the high half of a widened multiply, and reject only the values
        // that fall in the short tail. the loop runs zero times for almost every draw.
        let threshold = bound.wrapping_neg() % bound;
        loop {
            let drawn = self.next_u64();
            let wide = u128::from(drawn) * u128::from(bound);
            // the low half tells us whether this draw landed in the biased tail
            if (wide as u64) >= threshold {
                return (wide >> 64) as u64;
            }
        }
    }

    /// Draws a string of a fixed length from [`ALPHABET`]
    ///
    /// # Arguments
    ///
    /// * `len` - How many characters the string should have
    pub fn string(&mut self, len: usize) -> String {
        // five bits per character, so one draw covers twelve of them
        let mut out = String::with_capacity(len);
        let mut bits = 0u64;
        let mut left = 0u32;
        for _ in 0..len {
            // refill when we are out of bits rather than drawing per character
            if left < 5 {
                bits = self.next_u64();
                left = 64;
            }
            let index = (bits & 0x1F) as usize;
            bits >>= 5;
            left -= 5;
            out.push(ALPHABET[index] as char);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::{Scale, Seeded};

    /// The same seed produces the same sequence, which is the whole point of the module
    #[test]
    fn a_seed_is_reproducible() {
        let first: Vec<u64> = (0..64).map(|_| Seeded::new(42).next_u64()).collect();
        let second: Vec<u64> = (0..64).map(|_| Seeded::new(42).next_u64()).collect();
        assert_eq!(first, second);
    }

    /// Two seeds produce different sequences, so a seed is actually load bearing
    #[test]
    fn two_seeds_diverge() {
        let mut left = Seeded::new(1);
        let mut right = Seeded::new(2);
        let left: Vec<u64> = (0..64).map(|_| left.next_u64()).collect();
        let right: Vec<u64> = (0..64).map(|_| right.next_u64()).collect();
        assert_ne!(left, right);
    }

    /// Two named streams off one seed do not shadow each other
    ///
    /// Without this, adding a draw to the key stream would quietly change every payload too, and
    /// a capture would move for a reason nothing recorded.
    #[test]
    fn named_streams_are_independent() {
        let mut keys = Seeded::stream(7, "partition_keys");
        let mut payloads = Seeded::stream(7, "payloads");
        let keys: Vec<u64> = (0..64).map(|_| keys.next_u64()).collect();
        let payloads: Vec<u64> = (0..64).map(|_| payloads.next_u64()).collect();
        assert_ne!(keys, payloads);
    }

    /// A bounded draw stays inside its bound
    #[test]
    fn a_bounded_draw_respects_its_bound() {
        let mut seeded = Seeded::new(99);
        for _ in 0..10_000 {
            assert!(seeded.below(10) < 10);
        }
    }

    /// A bounded draw covers its whole range rather than collapsing onto part of it
    #[test]
    fn a_bounded_draw_covers_its_range() {
        let mut seeded = Seeded::new(99);
        let mut seen = [0usize; 8];
        for _ in 0..10_000 {
            seen[seeded.below(8) as usize] += 1;
        }
        // 10,000 draws over 8 buckets averages 1,250, so nothing should be near empty
        assert!(seen.iter().all(|count| *count > 800), "{seen:?}");
    }

    /// A generated string is the length that was asked for, in the alphabet that was declared
    #[test]
    fn a_string_is_the_requested_shape() {
        let mut seeded = Seeded::new(3);
        for len in [0usize, 1, 7, 12, 13, 64] {
            let built = seeded.string(len);
            assert_eq!(built.chars().count(), len);
            assert!(built.bytes().all(|byte| super::ALPHABET.contains(&byte)));
        }
    }

    /// A row width does not depend on the seed, so a scale parameter can hold it fixed
    #[test]
    fn a_generated_string_has_a_seed_independent_width() {
        // every character is one byte, so the byte length is the character length for any seed
        for seed in [0u64, 1, 12_345, u64::MAX] {
            assert_eq!(Seeded::new(seed).string(48).len(), 48);
        }
    }

    /// A smoke run is smaller than a full one and is never empty
    #[test]
    fn a_smoke_scale_is_small_but_not_empty() {
        assert_eq!(Scale::Full.rows(200_000), 200_000);
        assert_eq!(Scale::Smoke.rows(200_000), 2_000);
        // and a workload that only wanted a few rows still gets a usable number of them
        assert_eq!(Scale::Smoke.rows(50), 100);
    }
}
