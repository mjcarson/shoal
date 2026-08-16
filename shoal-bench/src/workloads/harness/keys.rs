//! Which key a query asks for, when they are not all equally likely
//!
//! Every workload before [F17](../../../../docs/src/features/workload-grid.md) walked its keys
//! with a stride coprime to the key count, which visits every key exactly once before repeating.
//! That is a uniform access pattern with no variance at all, and it is the honest default: it
//! defeats every cache in the system, so a number taken under it is a number the system can always
//! produce.
//!
//! It is also not what most published numbers measure. YCSB's default key chooser is a **scrambled
//! Zipfian**, under which a small set of keys takes most of the traffic and the working set
//! collapses onto whatever fits in memory. The gap between the two is often larger than any
//! difference this repository has ever tried to adjudicate, which is why the grid runs uniform and
//! a separate sweep quantifies the gap once rather than baking it into every number.
//!
//! # These are YCSB's generators, not generators like YCSB's
//!
//! [`KeyDistribution::Zipfian`] is YCSB's `ScrambledZipfianGenerator` and
//! [`KeyDistribution::Latest`] is its `SkewedLatestGenerator`, with YCSB's constant
//! (`theta = 0.99`) and YCSB's FNV-1a scramble. A generator that was merely Zipfian-shaped would
//! produce numbers that look comparable to a published YCSB figure and are not - the same failure
//! [F8](../../../../docs/src/features/purpose-built-workloads.md) records for a workload merely
//! shaped like `tmdb`.
//!
//! # What this costs to build
//!
//! The Zipfian constant needs `zeta(n, theta)`, a sum over every key. That is one pass over the
//! key space per arm, done once when the generator is built and never during sampling.

use crate::workloads::harness::seed::Seeded;

/// The skew YCSB's core workloads use, and the one every published Zipfian figure assumes
const THETA: f64 = 0.99;

/// FNV-1a's 64 bit offset basis, as YCSB's `Utils.fnvhash64` uses it
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a's 64 bit prime
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Which keys a workload's queries ask for
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyDistribution {
    /// Every key equally often
    Uniform,
    /// A few keys most of the time, scattered across the key space
    Zipfian,
    /// The most recently written keys most of the time
    Latest,
}

impl KeyDistribution {
    /// What this distribution is called, in identifiers and in the artifact
    pub fn as_str(&self) -> &'static str {
        // the stored name is the displayed name, so there is one spelling to remember
        match self {
            KeyDistribution::Uniform => "uniform",
            KeyDistribution::Zipfian => "zipfian",
            KeyDistribution::Latest => "latest",
        }
    }

    /// What this distribution is called in the artifact, when it is not the default
    ///
    /// `None` for uniform, which leaves `distribution` off the artifact and keeps every workload
    /// that predates this module byte-identical.
    pub fn artifact_name(&self) -> Option<String> {
        match self {
            KeyDistribution::Uniform => None,
            other => Some(other.as_str().to_string()),
        }
    }
}

/// Draws keys from a key space under one distribution
///
/// Built once per arm and then asked per query. Holds the Zipfian constants, which are what make
/// building it more expensive than using it.
///
/// # Every key is a function of the query's index
///
/// [`Keys::at`] takes `&self` and derives its draw from the index it is given, rather than walking
/// a generator forward. That is not a style choice. A mixture is driven by several slots pulling
/// from one shared cursor, so a generator walked in sequence would hand out a different key
/// sequence depending on which slot reached the cursor first - two runs of the same arm would send
/// different queries, and the arm would stop being repeatable in exactly the way the grid exists to
/// be. Indexing means query 12,345 asks for the same key on every run, at every depth, on every
/// machine.
#[derive(Debug, Clone)]
pub struct Keys {
    /// Which distribution this draws from
    distribution: KeyDistribution,
    /// How many keys there are, which every draw is bounded by
    count: u64,
    /// `zeta(count, theta)`, the Zipfian normalising constant
    zetan: f64,
    /// `1 / (1 - theta)`, precomputed because it is used on every draw
    alpha: f64,
    /// The Zipfian generator's `eta` term
    eta: f64,
    /// The seed every draw is derived from, folded with this chooser's stream name
    seed: u64,
}

impl Keys {
    /// Builds a key chooser over a key space
    ///
    /// # Arguments
    ///
    /// * `distribution` - Which distribution to draw from
    /// * `count` - How many keys there are
    /// * `seed` - The seed the whole run derives from
    /// * `stream` - What this stream is for, so two choosers in one workload do not shadow
    pub fn new(distribution: KeyDistribution, count: u64, seed: u64, stream: &str) -> Self {
        // a key space of zero has no key to return, and a chooser over it is a bug at the call
        // site rather than something to paper over here
        assert!(count > 0, "cannot draw a key from an empty key space");
        // the Zipfian constants, which a uniform chooser never reads. computing them anyway keeps
        // this one struct rather than three, and costs one pass over the key space per arm
        let zetan = zeta(count, THETA);
        let zeta_two = zeta(2, THETA);
        let alpha = 1.0 / (1.0 - THETA);
        // YCSB's eta, which is what maps a uniform draw onto the Zipfian curve
        let eta = (1.0 - (2.0 / count as f64).powf(1.0 - THETA)) / (1.0 - zeta_two / zetan);
        Keys {
            distribution,
            count,
            zetan,
            alpha,
            eta,
            // the stream name is folded in once here rather than on every draw, so two choosers
            // over one seed disagree without either of them paying for it per query
            seed: Seeded::stream(seed, stream).next_u64(),
        }
    }

    /// The key the query at an index asks for, in `0..count`
    ///
    /// # Arguments
    ///
    /// * `index` - Which query's key is wanted
    pub fn at(&self, index: u64) -> u64 {
        match self.distribution {
            // every key equally often, without the modulo bias of a plain remainder
            KeyDistribution::Uniform => self.draw(index).below(self.count),
            // YCSB scrambles the Zipfian value so the hot keys are scattered rather than adjacent,
            // which stops the skew from also being a locality effect in the key space
            KeyDistribution::Zipfian => fnv(self.zipf(index)) % self.count,
            // recency: the skew runs from the newest key backwards, so it is deliberately *not*
            // scrambled - the whole property being measured is that the hot keys are adjacent
            KeyDistribution::Latest => self.count - 1 - self.zipf(index),
        }
    }

    /// The generator for one index
    ///
    /// # Arguments
    ///
    /// * `index` - Which query to build a generator for
    fn draw(&self, index: u64) -> Seeded {
        // one generator per index, so the answer depends on the index and on nothing else that
        // happened during the run
        Seeded::at(self.seed, index)
    }

    /// The raw Zipfian value for one index, in `0..count`
    ///
    /// YCSB's `ZipfianGenerator.nextLong`, constant for constant, with the uniform draw it needs
    /// taken from the index rather than from a walked generator.
    ///
    /// # Arguments
    ///
    /// * `index` - Which query's value is wanted
    fn zipf(&self, index: u64) -> u64 {
        // a uniform double in [0, 1), built from the top 53 bits so every value is representable
        let u = (self.draw(index).next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        let uz = u * self.zetan;
        // the two shortcuts YCSB takes for the head of the curve, which is where most draws land
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(THETA) {
            return 1;
        }
        // and the closed form for the rest of it
        let drawn = (self.count as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
        // floating point at the tail can land one past the end, which would be an out of range key
        drawn.min(self.count - 1)
    }
}

/// The generalised harmonic number `sum(1/i^theta)` over a key space
///
/// # Arguments
///
/// * `count` - How many keys there are
/// * `theta` - The skew
fn zeta(count: u64, theta: f64) -> f64 {
    // one pass over the key space, which is why this is done when a chooser is built rather than
    // when it is used
    let mut sum = 0.0;
    for i in 1..=count {
        sum += 1.0 / (i as f64).powf(theta);
    }
    sum
}

/// FNV-1a over the eight bytes of a value, as YCSB hashes its Zipfian output
///
/// # Arguments
///
/// * `value` - The value to hash
fn fnv(value: u64) -> u64 {
    // byte at a time over the little endian representation, which is the order YCSB walks
    let mut hash = FNV_OFFSET;
    for byte in value.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::{KeyDistribution, Keys};

    /// Every distribution stays inside the key space it was given
    ///
    /// The Zipfian closed form can land one past the end at the tail, and an out of range key is a
    /// query for a row that was never seeded - which reads as a miss rather than as a bug.
    #[test]
    fn every_draw_lands_in_the_key_space() {
        for distribution in [
            KeyDistribution::Uniform,
            KeyDistribution::Zipfian,
            KeyDistribution::Latest,
        ] {
            for count in [1u64, 2, 3, 1_000, 200_000] {
                let keys = Keys::new(distribution, count, 42, "test");
                for index in 0..2_000 {
                    assert!(keys.at(index) < count, "{distribution:?} over {count}");
                }
            }
        }
    }

    /// A uniform chooser spreads its draws, and a Zipfian one does not
    ///
    /// This is the property the skew sweep exists to quantify, so it is worth pinning rather than
    /// assuming: under Zipfian a small share of the key space must take most of the traffic.
    #[test]
    fn zipfian_concentrates_where_uniform_does_not() {
        let count = 10_000u64;
        let draws = 100_000;
        // what share of the draws land on the busiest one percent of keys
        let share = |distribution| {
            let keys = Keys::new(distribution, count, 42, "test");
            let mut seen = vec![0u32; count as usize];
            for index in 0..draws {
                seen[keys.at(index) as usize] += 1;
            }
            seen.sort_unstable_by(|left, right| right.cmp(left));
            let hot: u32 = seen[..(count / 100) as usize].iter().sum();
            f64::from(hot) / f64::from(draws as u32)
        };
        let uniform = share(KeyDistribution::Uniform);
        let zipfian = share(KeyDistribution::Zipfian);
        // uniform puts about one percent of its traffic on one percent of its keys
        assert!(uniform < 0.05, "uniform put {uniform} on the hot one percent");
        // and YCSB's theta puts a large majority of it there
        assert!(zipfian > 0.4, "zipfian put only {zipfian} on the hot one percent");
    }

    /// The recency chooser favours the newest keys, which is what makes it a recency chooser
    #[test]
    fn latest_favours_the_end_of_the_key_space() {
        let count = 10_000u64;
        let keys = Keys::new(KeyDistribution::Latest, count, 42, "test");
        let mut recent = 0u32;
        for index in 0..10_000 {
            // the newest one percent of the key space
            if keys.at(index) >= count - count / 100 {
                recent += 1;
            }
        }
        assert!(
            f64::from(recent) / 10_000.0 > 0.4,
            "only {recent} draws in ten thousand were recent"
        );
    }

    /// A chooser is reproducible, and two named streams of one seed are not the same chooser
    #[test]
    fn a_chooser_is_reproducible_and_stream_scoped() {
        let draw = |stream| {
            let keys = Keys::new(KeyDistribution::Zipfian, 5_000, 42, stream);
            (0..256).map(|index| keys.at(index)).collect::<Vec<u64>>()
        };
        assert_eq!(draw("reads"), draw("reads"));
        assert_ne!(draw("reads"), draw("writes"));
    }

    /// Uniform is the default and says so by leaving itself off the artifact
    #[test]
    fn only_a_skewed_distribution_names_itself_in_the_artifact() {
        assert_eq!(KeyDistribution::Uniform.artifact_name(), None);
        assert_eq!(
            KeyDistribution::Zipfian.artifact_name(),
            Some("zipfian".to_string())
        );
        assert_eq!(
            KeyDistribution::Latest.artifact_name(),
            Some("latest".to_string())
        );
    }
}
