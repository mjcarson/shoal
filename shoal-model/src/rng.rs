//! The seeded random source every generated schedule is derived from
//!
//! A hand written SplitMix64 rather than the `rand` crate, so that a seed means the same
//! schedule for as long as this file exists: a dependency's algorithm can change under a version
//! bump, and a saved seed that no longer reproduces its failure is worth nothing.

/// A SplitMix64 generator
///
/// The reference algorithm from Steele, Lea and Flood's "Fast splittable pseudorandom number
/// generators", which is also what Java's `SplittableRandom` and the `rand` crate's `SplitMix64`
/// implement. Sixty-four bits of state and three constants.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    /// The generator state, advanced by the golden gamma on every draw
    state: u64,
}

impl SplitMix64 {
    /// Seed a generator
    ///
    /// # Arguments
    ///
    /// * `seed` - The seed; the same seed always produces the same sequence
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Draw the next sixty-four bits
    pub fn next_u64(&mut self) -> u64 {
        // advance the state by the golden gamma
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        // and mix the new state down to an output
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Draw a number below a bound
    ///
    /// A modulo rather than a rejection loop: the bias is negligible for the bounds a schedule
    /// uses, and a modulo is one draw, which keeps the sequence of draws per step fixed.
    ///
    /// # Arguments
    ///
    /// * `bound` - The exclusive upper bound, which must be at least one
    pub fn below(&mut self, bound: u64) -> u64 {
        // a bound of zero has no answer, and a schedule that asks for one is a bug in its generator
        assert!(bound > 0, "a draw below zero has no answer");
        self.next_u64() % bound
    }

    /// Pick an index with probability proportional to its weight
    ///
    /// # Arguments
    ///
    /// * `weights` - The weight of each index; zero weights are never chosen
    pub fn weighted(&mut self, weights: &[u32]) -> usize {
        // the total is what the draw is taken below
        let total: u64 = weights.iter().map(|weight| u64::from(*weight)).sum();
        assert!(total > 0, "every weight was zero");
        let mut draw = self.below(total);
        // walk the cumulative weights until the draw falls inside one
        for (index, weight) in weights.iter().enumerate() {
            let weight = u64::from(*weight);
            if draw < weight {
                return index;
            }
            draw -= weight;
        }
        // unreachable: the draw is below the total, so some weight absorbed it
        weights.len() - 1
    }
}

#[cfg(test)]
mod tests {
    use super::SplitMix64;

    /// The reference vectors for seed zero, so a change to the constants cannot go unnoticed
    #[test]
    fn seed_zero_matches_the_reference_sequence() {
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(rng.next_u64(), 0x06C4_5D18_8009_454F);
    }

    /// Two generators with one seed draw the same numbers, which is the whole point
    #[test]
    fn the_same_seed_draws_the_same_numbers() {
        let mut left = SplitMix64::new(42);
        let mut right = SplitMix64::new(42);
        for _ in 0..1000 {
            assert_eq!(left.next_u64(), right.next_u64());
        }
    }

    /// A weighted pick never lands on a zero weight and stays inside the bound
    #[test]
    fn weighted_picks_respect_zero_weights_and_bounds() {
        let mut rng = SplitMix64::new(7);
        for _ in 0..1000 {
            let index = rng.weighted(&[0, 3, 0, 5]);
            assert!(index == 1 || index == 3, "picked a zero weight: {index}");
            assert!(rng.below(4) < 4);
        }
    }
}
