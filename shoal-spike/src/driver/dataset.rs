//! The object dataset's two shapes, as types the driver would have
//!
//! S15 names two: a folder of real files, a workload somebody has, and a description - how many
//! objects, a distribution of sizes, a seed - a workload nobody has yet, repeatable to the byte.
//! These are the types M13 would add to `shoal-loadgen`, written here to be measured and thrown
//! away. A description is integers alone: its sizes are drawn without a float, so no build's libm
//! can move one (item 212 is a float that did), and its bytes come from a [`Generator`] whose
//! definition is published.

use std::path::PathBuf;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::generate::{mix, Generator, GAMMA};

/// A bucket's dataset
#[derive(Debug, Clone)]
pub enum ObjectDataset {
    /// A folder of real files, each an object
    Folder(FolderDataset),
    /// A description the driver makes objects from
    Described(Description),
}

/// A folder of real files: what a scan judged before a host is touched would hold
#[derive(Debug, Clone)]
pub struct FolderDataset {
    /// Where the folder is
    pub root: PathBuf,
    /// Every file, as the path under the root and its size, in the order the scan found them
    pub files: Vec<(String, u64)>,
    /// The digest a capture keeps: SHA-256 over every file's path, size and own digest
    pub digest: String,
}

/// How large a described object is
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SizeDistribution {
    /// Every object one size
    Fixed(u64),
    /// Uniform between two sizes, both included
    Uniform {
        /// The smallest
        low: u64,
        /// The largest
        high: u64,
    },
    /// A power of two between two, each as likely, then uniform inside it: sizes spread over
    /// orders of magnitude without a logarithm
    Doublings {
        /// The smallest power of two
        low: u64,
        /// The largest
        high: u64,
    },
    /// Sizes with weights, as a table of what a workload holds
    Table(Vec<(u64, u32)>),
}

/// A described dataset: everything a driver needs to make every object, byte for byte
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Description {
    /// How many objects
    pub objects: u64,
    /// How large each is
    pub sizes: SizeDistribution,
    /// The seed every size, path and byte is drawn from
    pub seed: u64,
    /// The generator its bytes come from, by name and version of its definition
    pub generator: String,
}

impl Description {
    /// The size of an object, from its index alone
    ///
    /// # Arguments
    ///
    /// * `object` - The object's index
    #[must_use]
    pub fn size_of(&self, object: u64) -> u64 {
        // one draw for each object, from its own place in the seed's stream
        let draw = mix(self.seed ^ object.wrapping_mul(GAMMA) ^ 0x5349_5a45);
        match &self.sizes {
            SizeDistribution::Fixed(size) => *size,
            SizeDistribution::Uniform { low, high } => low + below(draw, high - low + 1),
            SizeDistribution::Doublings { low, high } => {
                // a doubling first, then a place in it
                let (first, last) = (low.trailing_zeros(), high.trailing_zeros());
                let doubling = u64::from(first) + below(draw, u64::from(last - first + 1));
                let floor = 1u64 << doubling;
                floor + below(mix(draw), floor)
            }
            SizeDistribution::Table(rows) => {
                // a weight drawn, then the row it lands in
                let total: u64 = rows.iter().map(|(_, weight)| u64::from(*weight)).sum();
                let mut at = below(draw, total);
                for (size, weight) in rows {
                    if at < u64::from(*weight) {
                        return *size;
                    }
                    at -= u64::from(*weight);
                }
                rows.last().map_or(0, |(size, _)| *size)
            }
        }
    }

    /// The path of an object, from its index alone: two levels of fan out, then the index
    ///
    /// # Arguments
    ///
    /// * `object` - The object's index
    #[must_use]
    pub fn path_of(&self, object: u64) -> String {
        // the fan out from a mix, so neighbours in index do not share a prefix
        let spread = mix(self.seed ^ object);
        format!("{:02x}/{:02x}/{object:016x}", spread & 0xff, (spread >> 8) & 0xff)
    }

    /// The digest a capture keeps, over the description's canonical text
    #[must_use]
    pub fn digest(&self) -> String {
        // every field in a fixed order, so equal descriptions digest alike
        let sizes = match &self.sizes {
            SizeDistribution::Fixed(size) => format!("fixed {size}"),
            SizeDistribution::Uniform { low, high } => format!("uniform {low} {high}"),
            SizeDistribution::Doublings { low, high } => format!("doublings {low} {high}"),
            SizeDistribution::Table(rows) => {
                let rows: Vec<String> = rows.iter().map(|(size, weight)| format!("{size}:{weight}")).collect();
                format!("table {}", rows.join(","))
            }
        };
        let text = format!(
            "objects {}\nsizes {sizes}\nseed {}\ngenerator {}\n",
            self.objects, self.seed, self.generator
        );
        Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// Every object's path and size, in index order, and the bytes they hold together
    #[must_use]
    pub fn expand(&self) -> (Vec<(String, u64)>, u64) {
        // one pass, as a preload's plan would make it
        let mut total = 0;
        let objects = (0..self.objects)
            .map(|object| {
                let size = self.size_of(object);
                total += size;
                (self.path_of(object), size)
            })
            .collect();
        (objects, total)
    }
}

/// A value below a bound, from one draw: Lemire's method, as `shoal-loadgen`'s `Seeded::below`
///
/// # Arguments
///
/// * `draw` - The draw
/// * `bound` - The bound, above zero
#[must_use]
fn below(draw: u64, bound: u64) -> u64 {
    ((u128::from(draw) * u128::from(bound)) >> 64) as u64
}

/// Where a driver gets an object's bytes, whichever shape its dataset is
pub trait BodySource: Send + Sync {
    /// An object's length
    ///
    /// # Arguments
    ///
    /// * `object` - The object's index
    fn len(&self, object: u64) -> u64;

    /// Fill a buffer with an object's bytes from an offset
    ///
    /// # Arguments
    ///
    /// * `object` - The object's index
    /// * `offset` - Where in it
    /// * `buf` - The buffer
    fn fill(&self, object: u64, offset: u64, buf: &mut [u8]);
}

/// A description's bodies: sizes drawn, bytes made
pub struct DescribedBodies {
    /// The description
    pub description: Description,
    /// Its generator
    pub generator: Arc<dyn Generator>,
}

impl BodySource for DescribedBodies {
    /// An object's length, drawn
    ///
    /// # Arguments
    ///
    /// * `object` - The object's index
    fn len(&self, object: u64) -> u64 {
        self.description.size_of(object)
    }

    /// Fill a buffer with an object's bytes, made
    ///
    /// # Arguments
    ///
    /// * `object` - The object's index
    /// * `offset` - Where in it
    /// * `buf` - The buffer
    fn fill(&self, object: u64, offset: u64, buf: &mut [u8]) {
        self.generator.fill(object, offset, buf);
    }
}

/// The descriptions X13 times the expansion of, one of each shape
///
/// # Arguments
///
/// * `seed` - The seed
/// * `objects` - How many objects each holds
#[must_use]
pub fn examples(seed: u64, objects: u64) -> Vec<Description> {
    // four shapes a bucket's workload could take, at 1 MiB, 4 KiB to 64 MiB and a mixed table
    let generator = "aes-ctr/1".to_string();
    vec![
        Description { objects, sizes: SizeDistribution::Fixed(1 << 20), seed, generator: generator.clone() },
        Description { objects, sizes: SizeDistribution::Uniform { low: 4096, high: 1 << 20 }, seed, generator: generator.clone() },
        Description { objects, sizes: SizeDistribution::Doublings { low: 4096, high: 64 << 20 }, seed, generator: generator.clone() },
        Description {
            objects,
            sizes: SizeDistribution::Table(vec![(4096, 50), (64 << 10, 30), (1 << 20, 15), (64 << 20, 5)]),
            seed,
            generator,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A size is the same however many times it is asked for, and inside its distribution
    #[test]
    fn sizes_are_repeatable_and_bounded() {
        for description in examples(0x5831_3133, 10_000) {
            let (first, total) = description.expand();
            let (second, again) = description.expand();
            assert_eq!(first, second);
            assert_eq!(total, again);
            for (_, size) in &first {
                match &description.sizes {
                    SizeDistribution::Fixed(fixed) => assert_eq!(size, fixed),
                    SizeDistribution::Uniform { low, high } => assert!(size >= low && size <= high),
                    SizeDistribution::Doublings { low, high } => assert!(*size >= *low && *size < high * 2),
                    SizeDistribution::Table(rows) => assert!(rows.iter().any(|(row, _)| row == size)),
                }
            }
        }
    }

    /// Every doubling is drawn, and the table's weights are kept within a few percent
    #[test]
    fn draws_cover_their_distributions() {
        let examples = examples(7, 100_000);
        let doublings = &examples[2];
        let mut seen = std::collections::BTreeSet::new();
        for object in 0..doublings.objects {
            seen.insert(63 - doublings.size_of(object).leading_zeros());
        }
        assert_eq!(seen.len(), 15, "4 KiB to 64 MiB is fifteen doublings");
        let table = &examples[3];
        let small = (0..table.objects).filter(|&object| table.size_of(object) == 4096).count();
        assert!((48_000..52_000).contains(&small), "half the table is 4 KiB: {small}");
    }

    /// Two descriptions that differ in anything digest apart, and equal ones alike
    #[test]
    fn digests_follow_every_field() {
        let base = examples(1, 1000).remove(0);
        assert_eq!(base.digest(), base.clone().digest());
        let mut seed = base.clone();
        seed.seed = 2;
        let mut objects = base.clone();
        objects.objects = 1001;
        let mut generator = base.clone();
        generator.generator = "splitmix/1".to_string();
        for other in [seed, objects, generator] {
            assert_ne!(base.digest(), other.digest());
        }
    }
}
