//! A bucket's objects as a benchmark drives them ([F80](../../docs/src/features/object-wire.md))
//!
//! A table's dataset is a file of rows; a bucket's is a **description**
//! ([S15](../../docs/src/object-storage/performance.md#what-the-driver-gains)): how many objects, a
//! distribution of their sizes drawn without a float, a seed, and the generator their bytes come
//! from by its definition's name and version. Every object's size, path and bytes are derived from
//! its index, so any object, and any range of it, is made alone, and a read is checked against bytes
//! made again rather than against a copy kept ([X13](../../docs/src/object-storage/benchmark-shape.md)).
//!
//! - **The generator is `splitmix64-ctr/1`**: word `i` of object `o` is `mix(seed ^ o·γ + (i+1)·γ)`,
//!   little endian, the state `Seeded::at(seed, o)` starts from. Its digests are frozen here as X13
//!   measured them on every host and build; a second generator is a new name, never a change to this.
//! - **An object arm** is a mix of puts, gets and stats at an object size and a concurrency:
//!   `{mix}/s{size}/c{concurrency}/none`. Its puts make new objects past the preloaded ones, and its
//!   gets and stats ask only for objects that were preloaded, so a miss is the cluster's failure.
//! - **Every acknowledged object is read back** after its arm and compared byte for byte with the
//!   bytes made again; a missing or differing object fails the run. Where an arm's objects are
//!   deleted between arms rather than the cluster reset, a sample of them is read back after each
//!   arm and the whole preload at the end ([F94](../../docs/src/features/bench-object-endpoints.md)).
//! - **The store is an endpoint** ([`ObjectEndpoint`]): a Shoal member's client, or another
//!   store's S3 endpoint behind the `s3` feature, so every store is driven by this one loop.
//!
//! # Invariants
//!
//! **Nothing here names a bucket.** A dataset's buckets are the schema's, found by name, and every
//! operation goes through an endpoint's untyped calls, as nothing in the driver names a table.
//! **The time of an operation is the same span for every endpoint**: from before its bytes are
//! made to its answer, with a get's comparison after the time is taken.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shoal::shared::pools::ByteSize;

use crate::driver::{ArmClock, ArmOutcome};
use crate::progress::{BenchEvent, Phase, Progress};
use crate::results::{CleanupFacts, SecondPhase, SecondSample, VerifyFacts};
use crate::seed::Seeded;
use crate::spec::ReadLevel;
use crate::window::{OpKind, Outcome, Window};

pub mod endpoint;
mod member;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "s3")]
pub mod s3_local;
#[cfg(feature = "s3")]
pub mod sigv4;

pub use endpoint::{EndpointError, ErrorKind, ObjectEndpoint};

/// The generator every described dataset of this build names
pub const GENERATOR: &str = "splitmix64-ctr/1";

/// SplitMix64's golden gamma, the step its counter advances by
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// How many bytes an operation's bytes are made or checked in at a time
const MAKE_PIECE: usize = 1 << 20;

/// SplitMix64's output function
///
/// # Arguments
///
/// * `state` - The state to mix
#[must_use]
pub fn mix(state: u64) -> u64 {
    // Vigna's finalizer, the one `Seeded` uses
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The word an object's stream holds at an index: `mix(seed ^ o·γ + (i+1)·γ)`
///
/// # Arguments
///
/// * `seed` - The dataset's seed
/// * `object` - The object
/// * `index` - The word
#[must_use]
pub fn word(seed: u64, object: u64, index: u64) -> u64 {
    // the object's starting state, then the step count to the word
    let key = seed ^ object.wrapping_mul(GAMMA);
    mix(key.wrapping_add(index.wrapping_add(1).wrapping_mul(GAMMA)))
}

/// Fill a buffer with an object's bytes from an offset: `splitmix64-ctr/1`
///
/// # Arguments
///
/// * `seed` - The dataset's seed
/// * `object` - The object
/// * `offset` - Where in the object the buffer starts
/// * `buf` - The buffer
pub fn fill(seed: u64, object: u64, offset: u64, buf: &mut [u8]) {
    // a head that starts inside a word takes that word's tail
    let mut index = offset / 8;
    let skip = (offset % 8) as usize;
    let mut at = 0;
    if skip != 0 {
        let head = word(seed, object, index).to_le_bytes();
        let take = (8 - skip).min(buf.len());
        buf[..take].copy_from_slice(&head[skip..skip + take]);
        at = take;
        index += 1;
    }
    // whole words, the state stepped by γ for each
    let key = seed ^ object.wrapping_mul(GAMMA);
    let mut state = key.wrapping_add(index.wrapping_add(1).wrapping_mul(GAMMA));
    let mut chunks = buf[at..].chunks_exact_mut(8);
    for chunk in &mut chunks {
        chunk.copy_from_slice(&mix(state).to_le_bytes());
        state = state.wrapping_add(GAMMA);
    }
    // a tail shorter than a word takes that word's head
    let tail = chunks.into_remainder();
    if !tail.is_empty() {
        let len = tail.len();
        tail.copy_from_slice(&mix(state).to_le_bytes()[..len]);
    }
}

/// A value below a bound from one draw: Lemire's method, as X13 drew its sizes
///
/// # Arguments
///
/// * `draw` - The draw
/// * `bound` - The bound, above zero
#[must_use]
fn below(draw: u64, bound: u64) -> u64 {
    ((u128::from(draw) * u128::from(bound)) >> 64) as u64
}

/// How large a described object is
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WrittenSizes", into = "WrittenSizes")]
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
    /// A power of two between two, each as likely, then uniform inside it
    Doublings {
        /// The smallest power of two
        low: u64,
        /// The largest
        high: u64,
    },
    /// Sizes with weights, as a table of what a workload holds
    Table(Vec<(u64, u32)>),
}

/// A size distribution as it is written: exactly one of its four keys
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WrittenSizes {
    /// Every object one size
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed: Option<ByteSize>,
    /// Uniform between two sizes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uniform: Option<[ByteSize; 2]>,
    /// A power of two between two, then uniform inside it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doublings: Option<[ByteSize; 2]>,
    /// Sizes with weights
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<Vec<(ByteSize, u32)>>,
}

impl TryFrom<WrittenSizes> for SizeDistribution {
    type Error = String;

    /// Read a distribution that names exactly one of its forms
    ///
    /// # Arguments
    ///
    /// * `written` - What was written
    fn try_from(written: WrittenSizes) -> Result<Self, Self::Error> {
        match written {
            WrittenSizes {
                fixed: Some(size),
                uniform: None,
                doublings: None,
                table: None,
            } => Ok(SizeDistribution::Fixed(size.get())),
            WrittenSizes {
                fixed: None,
                uniform: Some([low, high]),
                doublings: None,
                table: None,
            } => Ok(SizeDistribution::Uniform {
                low: low.get(),
                high: high.get(),
            }),
            WrittenSizes {
                fixed: None,
                uniform: None,
                doublings: Some([low, high]),
                table: None,
            } => Ok(SizeDistribution::Doublings {
                low: low.get(),
                high: high.get(),
            }),
            WrittenSizes {
                fixed: None,
                uniform: None,
                doublings: None,
                table: Some(rows),
            } => Ok(SizeDistribution::Table(
                rows.into_iter()
                    .map(|(size, weight)| (size.get(), weight))
                    .collect(),
            )),
            _ => Err("sizes names exactly one of fixed, uniform, doublings or table".to_string()),
        }
    }
}

impl From<SizeDistribution> for WrittenSizes {
    /// Write a distribution as its one key
    ///
    /// # Arguments
    ///
    /// * `sizes` - The distribution
    fn from(sizes: SizeDistribution) -> Self {
        match sizes {
            SizeDistribution::Fixed(size) => WrittenSizes {
                fixed: Some(ByteSize(size)),
                ..WrittenSizes::default()
            },
            SizeDistribution::Uniform { low, high } => WrittenSizes {
                uniform: Some([ByteSize(low), ByteSize(high)]),
                ..WrittenSizes::default()
            },
            SizeDistribution::Doublings { low, high } => WrittenSizes {
                doublings: Some([ByteSize(low), ByteSize(high)]),
                ..WrittenSizes::default()
            },
            SizeDistribution::Table(rows) => WrittenSizes {
                table: Some(
                    rows.into_iter()
                        .map(|(size, weight)| (ByteSize(size), weight))
                        .collect(),
                ),
                ..WrittenSizes::default()
            },
        }
    }
}

/// The generator a description names when it names none
fn default_generator() -> String {
    GENERATOR.to_string()
}

/// A described dataset: everything a driver needs to make every object, byte for byte
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Description {
    /// How many objects
    pub objects: u64,
    /// How large each is
    pub sizes: SizeDistribution,
    /// The seed every size, path and byte is drawn from
    #[serde(default)]
    pub seed: u64,
    /// The generator its bytes come from, by name and version of its definition
    #[serde(default = "default_generator")]
    pub generator: String,
}

impl Description {
    /// Read a description from its file
    ///
    /// # Arguments
    ///
    /// * `path` - The file
    ///
    /// # Errors
    ///
    /// A file that cannot be read, is not a description, or describes nothing a driver can make.
    pub fn read(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
        let description: Description =
            serde_yaml::from_str(&text).map_err(|error| error.to_string())?;
        description.check()?;
        Ok(description)
    }

    /// Whether a driver can make every object this describes, and why not
    ///
    /// # Errors
    ///
    /// The first thing it cannot make.
    pub fn check(&self) -> Result<(), String> {
        // a generator this build has, by its versioned name
        if self.generator != GENERATOR {
            return Err(format!(
                "generator {} is not one this build makes; it makes {GENERATOR}",
                self.generator
            ));
        }
        if self.objects == 0 {
            return Err("a description of no objects describes nothing to drive".to_string());
        }
        // a distribution every draw of which is a size
        match &self.sizes {
            SizeDistribution::Fixed(_) => Ok(()),
            SizeDistribution::Uniform { low, high } if low <= high => Ok(()),
            SizeDistribution::Doublings { low, high }
                if low.is_power_of_two() && high.is_power_of_two() && low <= high =>
            {
                Ok(())
            }
            SizeDistribution::Table(rows)
                if !rows.is_empty() && rows.iter().any(|(_, weight)| *weight > 0) =>
            {
                Ok(())
            }
            other => Err(format!(
                "{other:?} is not a distribution a size can be drawn from"
            )),
        }
    }

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
        format!(
            "{:02x}/{:02x}/{object:016x}",
            spread & 0xff,
            (spread >> 8) & 0xff
        )
    }

    /// The digest a capture keeps and compares by, over the description's canonical text
    #[must_use]
    pub fn digest(&self) -> String {
        // every field in a fixed order, so equal descriptions digest alike
        let sizes = match &self.sizes {
            SizeDistribution::Fixed(size) => format!("fixed {size}"),
            SizeDistribution::Uniform { low, high } => format!("uniform {low} {high}"),
            SizeDistribution::Doublings { low, high } => format!("doublings {low} {high}"),
            SizeDistribution::Table(rows) => {
                let rows: Vec<String> = rows
                    .iter()
                    .map(|(size, weight)| format!("{size}:{weight}"))
                    .collect();
                format!("table {}", rows.join(","))
            }
        };
        let text = format!(
            "objects {}\nsizes {sizes}\nseed {}\ngenerator {}\n",
            self.objects, self.seed, self.generator
        );
        crate::read::hex(&Sha256::digest(text.as_bytes()))
    }
}

/// One bucket's described dataset, as a folder holds it: `<Bucket>.yaml`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectDataset {
    /// The bucket, by its marker's name
    pub bucket: String,
    /// The file it was read from
    pub path: PathBuf,
    /// What it describes
    pub description: Description,
    /// The description's digest
    pub digest: String,
}

/// What an object arm does: puts, gets, stats and writes in place by weight
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectMix {
    /// What it is called in an arm's id
    pub name: String,
    /// The weight of gets
    pub get: u32,
    /// The weight of puts
    pub put: u32,
    /// The weight of stats
    pub stat: u32,
    /// The weight of writes in place into a preloaded object
    /// ([F86](../../docs/src/features/replicated-pools.md))
    pub patch: u32,
    /// The bytes one write in place writes, at a 4 KiB boundary of its object
    pub patch_bytes: u64,
}

/// The bytes a write in place writes unless a mix names another size
pub const PATCH_BYTES: u64 = 4096;

impl ObjectMix {
    /// The mixes an object arm runs when nobody names any: all puts, all gets, and half of each
    #[must_use]
    pub fn defaults() -> Vec<ObjectMix> {
        ["put100", "get100", "rw50"]
            .iter()
            .map(|name| name.parse().expect("a named mix parses"))
            .collect()
    }

    /// Whether the mix changes what the cluster holds
    ///
    /// A write in place rewrites bytes its object already holds, so what reads back is unchanged,
    /// but every one is a write through the stripe's commit.
    #[must_use]
    pub fn writes(&self) -> bool {
        self.put > 0 || self.patch > 0
    }

    /// Whether it asks for objects that were preloaded
    #[must_use]
    pub fn reads(&self) -> bool {
        self.get > 0 || self.stat > 0 || self.patch > 0
    }

    /// Every weight together
    #[must_use]
    pub fn total(&self) -> u32 {
        self.get + self.put + self.stat + self.patch
    }
}

impl std::str::FromStr for ObjectMix {
    type Err = String;

    /// Read a mix by its name - `put100`, `get100`, `stat100`, `rw50`, `patch100` (4 KiB writes in
    /// place), `patch100-32k` - or as weights, `get:N,put:M,stat:K,patch:P`
    ///
    /// # Arguments
    ///
    /// * `text` - The mix
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let named = |get, put, stat| ObjectMix {
            name: text.to_string(),
            get,
            put,
            stat,
            patch: 0,
            patch_bytes: PATCH_BYTES,
        };
        match text {
            "put100" => return Ok(named(0, 100, 0)),
            "get100" => return Ok(named(100, 0, 0)),
            "stat100" => return Ok(named(0, 0, 100)),
            "rw50" => return Ok(named(50, 50, 0)),
            "patch100" => {
                return Ok(ObjectMix {
                    patch: 100,
                    ..named(0, 0, 0)
                })
            }
            "patch100-32k" => {
                return Ok(ObjectMix {
                    patch: 100,
                    patch_bytes: 32 << 10,
                    ..named(0, 0, 0)
                })
            }
            _ => {}
        }
        // weights, named in a fixed order whatever order they were written in
        let mut mix = named(0, 0, 0);
        for part in text.split(',') {
            let (kind, weight) = part.split_once(':').ok_or_else(|| {
                format!("{text:?} is not an object mix: name one or write get:N,put:M,stat:K")
            })?;
            let weight: u32 = weight
                .trim()
                .parse()
                .map_err(|_| format!("{weight:?} in {text:?} is not a weight"))?;
            match kind.trim() {
                "get" => mix.get = weight,
                "put" => mix.put = weight,
                "stat" => mix.stat = weight,
                "patch" => mix.patch = weight,
                other => return Err(format!("{other:?} in {text:?} is not get, put, stat or patch")),
            }
        }
        if mix.total() == 0 {
            return Err(format!("{text:?} weighs nothing"));
        }
        // a mix with no writes in place keeps the name it had before they existed
        mix.name = format!("get{}-put{}-stat{}", mix.get, mix.put, mix.stat);
        if mix.patch > 0 {
            mix.name.push_str(&format!("-patch{}", mix.patch));
        }
        Ok(mix)
    }
}

impl TryFrom<String> for ObjectMix {
    type Error = String;

    /// Read a mix from its spelling
    ///
    /// # Arguments
    ///
    /// * `text` - The mix
    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl From<ObjectMix> for String {
    /// Spell a mix as its weights, which read back as the same mix
    ///
    /// # Arguments
    ///
    /// * `mix` - The mix
    fn from(mix: ObjectMix) -> Self {
        match mix.name.as_str() {
            "put100" | "get100" | "stat100" | "rw50" | "patch100" | "patch100-32k" => mix.name,
            _ if mix.patch > 0 => format!("get:{},put:{},stat:{},patch:{}", mix.get, mix.put, mix.stat, mix.patch),
            _ => format!("get:{},put:{},stat:{}", mix.get, mix.put, mix.stat),
        }
    }
}

/// The size label an object arm's id carries: `s4k`, `s1m`, or `sd` for the sizes described
///
/// # Arguments
///
/// * `size` - The size every object of the arm is, or none for the description's own
#[must_use]
pub fn size_label(size: Option<u64>) -> String {
    match size {
        None => "sd".to_string(),
        Some(size) => {
            // the largest power of two unit that leaves a whole number, in lower case
            for (shift, unit) in [(30, "g"), (20, "m"), (10, "k")] {
                if size != 0 && size % (1u64 << shift) == 0 {
                    return format!("s{}{unit}", size >> shift);
                }
            }
            format!("s{size}")
        }
    }
}

/// One object arm: a mix at a size and a concurrency
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectArm {
    /// What it does
    pub mix: ObjectMix,
    /// The size every object it puts and reads is, or none for each description's own sizes
    pub size: Option<u64>,
    /// How many operations are in flight at once, over every member
    pub concurrency: usize,
}

/// How many of each description's objects the preload puts, at each size
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ObjectPreload {
    /// A share of the description's objects, in percent
    Percent(f64),
    /// This many objects, or every one a description holds if it holds fewer; before
    /// [F94](../../docs/src/features/bench-object-endpoints.md) a count was read as half of them
    Objects(u64),
}

impl From<crate::feed::Preload> for ObjectPreload {
    /// A spec's preload as an object arm reads it: a count is that many objects of each
    /// description, where before F94 it was half, and a share a share of them
    ///
    /// # Arguments
    ///
    /// * `preload` - The spec's preload, or a file's own
    fn from(preload: crate::feed::Preload) -> Self {
        match preload {
            crate::feed::Preload::Percent(percent) => ObjectPreload::Percent(percent),
            crate::feed::Preload::Rows(objects) => ObjectPreload::Objects(objects),
        }
    }
}

impl From<f64> for ObjectPreload {
    /// A share of each description's objects, in percent
    ///
    /// # Arguments
    ///
    /// * `percent` - The share
    fn from(percent: f64) -> Self {
        ObjectPreload::Percent(percent)
    }
}

impl ObjectPreload {
    /// How many objects a description's preload puts
    ///
    /// # Arguments
    ///
    /// * `description` - The description
    #[must_use]
    pub fn count(&self, description: &Description) -> u64 {
        // a share of the description, or a count of objects
        let count = match self {
            ObjectPreload::Percent(percent) => (description.objects as f64 * percent / 100.0).ceil() as u64,
            ObjectPreload::Objects(objects) => *objects,
        };
        // at least one, so a read always has something to ask for
        count.clamp(1, description.objects)
    }
}

/// The objects one size makes of one dataset: a namespace of paths of their own
#[derive(Debug)]
struct Namespace {
    /// How many objects the preload put
    preloaded: AtomicU64,
    /// The next object a put makes, past the preloaded ones
    next: AtomicU64,
    /// Every object a put was acknowledged for
    acked: Mutex<BTreeSet<u64>>,
    /// Whether a put ran past the description's last object and started over, so it put an
    /// object that was put before
    wrapped: AtomicBool,
}

/// What one operation is, once picked
#[derive(Debug, Clone, Copy)]
enum Op {
    /// Put the object
    Put,
    /// Read the whole object
    Get,
    /// Stat it
    Stat,
    /// Write part of it in place
    Patch,
}

/// Drives a store's buckets through its endpoints: a Shoal cluster's members' clients, or
/// another store's S3 endpoints ([F94](../../docs/src/features/bench-object-endpoints.md))
pub struct ObjectDriver<E: ObjectEndpoint> {
    /// One endpoint a member, in the order streams are spread over them
    clients: Vec<Arc<E>>,
    /// Every described bucket, in the schema's order
    datasets: Vec<ObjectDataset>,
    /// How many of each description's objects the preload puts
    preload: ObjectPreload,
    /// A bucket's own preload, over `preload` for the rest
    /// ([F104](../../docs/src/features/memory-in-a-capture.md))
    preloads: BTreeMap<String, ObjectPreload>,
    /// The level an arm's gets and stats are read at; a read back is always the default
    /// ([F95](../../docs/src/features/bench-object-read-levels.md))
    level: ReadLevel,
    /// The objects each dataset and size made, by both
    spaces: Mutex<HashMap<(usize, Option<u64>), Arc<Namespace>>>,
}

impl<E: ObjectEndpoint> ObjectDriver<E> {
    /// A driver over these endpoints and these buckets
    ///
    /// # Arguments
    ///
    /// * `clients` - One endpoint a member
    /// * `datasets` - Every described bucket
    /// * `preload` - How many of each description's objects are put before any arm
    ///
    /// # Panics
    ///
    /// When there is no endpoint or no dataset.
    #[must_use]
    pub fn new(clients: Vec<Arc<E>>, datasets: Vec<ObjectDataset>, preload: impl Into<ObjectPreload>) -> Self {
        assert!(!clients.is_empty(), "a driver needs a client");
        assert!(
            !datasets.is_empty(),
            "an object driver needs a described bucket"
        );
        ObjectDriver {
            clients,
            datasets,
            preload: preload.into(),
            preloads: BTreeMap::new(),
            level: ReadLevel::Default,
            spaces: Mutex::new(HashMap::new()),
        }
    }

    /// Preload some buckets by their own preloads, the rest by the driver's
    /// ([F104](../../docs/src/features/memory-in-a-capture.md))
    ///
    /// # Arguments
    ///
    /// * `preloads` - Each bucket's own preload, by its marker's name
    #[must_use]
    pub fn preloading(mut self, preloads: BTreeMap<String, ObjectPreload>) -> Self {
        self.preloads = preloads;
        self
    }

    /// Read every arm's gets and stats at a level, which a read back never is
    ///
    /// # Arguments
    ///
    /// * `level` - The level, the server's default unless a spec names one
    #[must_use]
    pub fn reading_at(mut self, level: ReadLevel) -> Self {
        self.level = level;
        self
    }

    /// The buckets this driver drives
    #[must_use]
    pub fn datasets(&self) -> &[ObjectDataset] {
        &self.datasets
    }

    /// Drive the same buckets, remembering every object acknowledged so far, through other
    /// endpoints: the members of a cluster started again
    ///
    /// # Arguments
    ///
    /// * `clients` - One endpoint a member
    ///
    /// # Panics
    ///
    /// When there is no endpoint.
    pub fn reconnect(&mut self, clients: Vec<Arc<E>>) {
        assert!(!clients.is_empty(), "a driver needs a client");
        self.clients = clients;
    }

    /// Why this driver cannot run an arm, if it cannot: a write in place through endpoints
    /// that have none
    ///
    /// # Arguments
    ///
    /// * `arm` - The arm
    #[must_use]
    pub fn refuses(&self, arm: &ObjectArm) -> Option<String> {
        // a write in place is the one operation an endpoint may not have
        if arm.mix.patch > 0 && !self.clients.iter().all(|client| client.writes_in_place()) {
            return Some(format!(
                "{} writes objects in place, which this store's endpoints cannot",
                arm.mix.name
            ));
        }
        None
    }

    /// How many objects a put was acknowledged for, over every bucket and size
    #[must_use]
    pub fn acknowledged(&self) -> u64 {
        let spaces = self.spaces.lock().expect("the namespaces");
        spaces
            .values()
            .map(|space| space.acked.lock().expect("the acknowledged").len() as u64)
            .sum()
    }

    /// Whether any put since the last time this was asked ran past its description's last
    /// object and put one again, which leaves the flag clear for the next run
    #[must_use]
    pub fn took_wrap(&self) -> bool {
        // every namespace's flag taken, none of them skipped
        let spaces = self.spaces.lock().expect("the namespaces");
        spaces
            .values()
            .map(|space| space.wrapped.swap(false, Ordering::AcqRel))
            .fold(false, |any, wrapped| any | wrapped)
    }

    /// The namespace of one dataset at one size, made empty the first time it is asked for
    ///
    /// # Arguments
    ///
    /// * `dataset` - The dataset's place
    /// * `size` - The size, or none for the description's own
    fn space(&self, dataset: usize, size: Option<u64>) -> Arc<Namespace> {
        let mut spaces = self.spaces.lock().expect("the namespaces");
        spaces
            .entry((dataset, size))
            .or_insert_with(|| {
                Arc::new(Namespace {
                    preloaded: AtomicU64::new(0),
                    next: AtomicU64::new(0),
                    acked: Mutex::new(BTreeSet::new()),
                    wrapped: AtomicBool::new(false),
                })
            })
            .clone()
    }

    /// Put every bucket's preload at one size, once: the objects its gets and stats ask for
    ///
    /// Returns the preload's windows and how long it took, or nothing if it was already put.
    ///
    /// # Arguments
    ///
    /// * `size` - The size, or none for each description's own
    /// * `concurrency` - How many puts are in flight at once
    /// * `progress` - Where its progress is sent
    pub async fn preload(
        &self,
        size: Option<u64>,
        concurrency: usize,
        progress: &Progress,
    ) -> Option<(Vec<Window>, Duration)> {
        // every namespace of this size not yet preloaded, with how many it puts
        let mut work = Vec::new();
        for (at, dataset) in self.datasets.iter().enumerate() {
            let space = self.space(at, size);
            if space.preloaded.load(Ordering::Acquire) == 0 {
                // the bucket's own preload, or the driver's (F104)
                let preload = self.preloads.get(&dataset.bucket).unwrap_or(&self.preload);
                work.push((at, space, preload.count(&dataset.description)));
            }
        }
        if work.is_empty() {
            return None;
        }
        let total: u64 = work.iter().map(|(_, _, count)| count).sum();
        progress.send(BenchEvent::Phase(Phase::Preload { done: 0, total }));
        let started = Instant::now();
        let windows = Arc::new(Mutex::new(Window::default()));
        for (at, space, count) in work {
            // the preload's objects shared over the streams in index order
            let cursor = Arc::new(AtomicU64::new(0));
            let mut handles = Vec::with_capacity(concurrency);
            for stream in 0..concurrency.max(1) {
                let client = self.clients[stream % self.clients.len()].clone();
                let dataset = self.datasets[at].clone();
                let space = space.clone();
                let cursor = cursor.clone();
                let windows = windows.clone();
                handles.push(tokio::spawn(async move {
                    loop {
                        let object = cursor.fetch_add(1, Ordering::AcqRel);
                        if object >= count {
                            return;
                        }
                        let (outcome, sent, _) = put(&*client, &dataset, size, object).await;
                        if matches!(outcome, Outcome::Ok(_)) {
                            space.acked.lock().expect("the acknowledged").insert(object);
                        }
                        let mut window = windows.lock().expect("the window");
                        window.bytes_sent += sent;
                        window.record(&OpKind::Supplied(Arc::from("put")), outcome);
                    }
                }));
            }
            for handle in handles {
                let _ = handle.await;
            }
            // later puts make objects past the preloaded ones
            space.preloaded.store(count, Ordering::Release);
            space.next.store(count, Ordering::Release);
        }
        let window = std::mem::take(&mut *windows.lock().expect("the window"));
        Some((vec![window], started.elapsed()))
    }

    /// Run one object arm until its time is up or it is stopped
    ///
    /// # Arguments
    ///
    /// * `arm` - The arm
    /// * `warmup` - How long it runs before it is measured
    /// * `duration` - How long it is measured
    /// * `seed` - The run's seed, which every pick is drawn from
    /// * `stream_name` - The name the picks' stream is drawn under, the arm's id and run
    /// * `clock` - The arm's clock, started by the caller
    /// * `progress` - Where each second is sent
    #[allow(clippy::too_many_arguments)]
    pub async fn run_arm(
        &self,
        arm: &ObjectArm,
        warmup: Duration,
        duration: Duration,
        seed: u64,
        stream_name: &str,
        clock: Arc<ArmClock>,
        progress: &Progress,
    ) -> ArmOutcome {
        // every stream's seconds, and the namespaces this arm's size reads and writes
        let streams = arm.concurrency.max(1);
        let seconds: Arc<Vec<Mutex<Vec<Window>>>> =
            Arc::new((0..streams).map(|_| Mutex::new(Vec::new())).collect());
        let spaces: Arc<Vec<Arc<Namespace>>> = Arc::new(
            (0..self.datasets.len())
                .map(|at| self.space(at, arm.size))
                .collect(),
        );
        let pick_seed = Seeded::stream(seed, stream_name).next_u64();
        // every stream's gets and stats at the driver's level (F95)
        let level = self.level;
        let mut handles = Vec::with_capacity(streams);
        for stream in 0..streams {
            let client = self.clients[stream % self.clients.len()].clone();
            let datasets = self.datasets.clone();
            let spaces = spaces.clone();
            let seconds = seconds.clone();
            let clock = clock.clone();
            let mix = arm.mix.clone();
            let size = arm.size;
            handles.push(tokio::spawn(async move {
                let mut index = 0u64;
                while clock.live() {
                    // one draw picks the bucket, the operation and the object
                    let mut draw = Seeded::at(pick_seed, ((stream as u64) << 40) | index);
                    index += 1;
                    let at = draw.below(datasets.len() as u64) as usize;
                    let dataset = &datasets[at];
                    let space = &spaces[at];
                    let total = u64::from(mix.total());
                    let weight = draw.below(total);
                    let op = if weight < u64::from(mix.get) {
                        Op::Get
                    } else if weight < u64::from(mix.get + mix.put) {
                        Op::Put
                    } else if weight < u64::from(mix.get + mix.put + mix.stat) {
                        Op::Stat
                    } else {
                        Op::Patch
                    };
                    let second = clock.second();
                    let (kind, outcome, sent, received) = match op {
                        Op::Put => {
                            // the next object past the preload; one past the description starts
                            // over, and the run is marked as having put an object twice
                            let next = space.next.fetch_add(1, Ordering::AcqRel);
                            if next >= dataset.description.objects {
                                space.wrapped.store(true, Ordering::Release);
                            }
                            let object = next % dataset.description.objects;
                            let (outcome, sent, received) =
                                put(&*client, dataset, size, object).await;
                            if matches!(outcome, Outcome::Ok(_)) {
                                space.acked.lock().expect("the acknowledged").insert(object);
                            }
                            ("put", outcome, sent, received)
                        }
                        Op::Get | Op::Stat => {
                            // only an object the preload put, so a miss is the cluster's
                            let preloaded = space.preloaded.load(Ordering::Acquire).max(1);
                            let object = draw.below(preloaded);
                            let whole = matches!(op, Op::Get);
                            let (outcome, received) =
                                read(&*client, dataset, size, object, whole, level).await;
                            (if whole { "get" } else { "stat" }, outcome, 0, received)
                        }
                        Op::Patch => {
                            // a preloaded object, at a 4 KiB boundary its bytes reach past
                            let preloaded = space.preloaded.load(Ordering::Acquire).max(1);
                            let object = draw.below(preloaded);
                            let room = object_len(dataset, size, object).saturating_sub(mix.patch_bytes) / PATCH_BYTES;
                            let offset = draw.below(room.max(1)) * PATCH_BYTES;
                            let (outcome, sent) =
                                patch(&*client, dataset, size, object, offset, mix.patch_bytes).await;
                            ("patch", outcome, sent, 0)
                        }
                    };
                    let mut mine = seconds[stream].lock().expect("a stream's seconds");
                    while mine.len() <= second {
                        mine.push(Window::default());
                    }
                    let window = &mut mine[second];
                    let kind = OpKind::Supplied(Arc::from(kind));
                    window.bytes_sent += sent;
                    window.record_received(Some(&kind), received);
                    window.record(&kind, outcome);
                }
            }));
        }
        // each second reported as it closes, with how busy the driver was
        let done = Arc::new(AtomicBool::new(false));
        let reporter = {
            let seconds = seconds.clone();
            let clock = clock.clone();
            let progress = progress.clone();
            let done = done.clone();
            let warmup_secs = warmup.as_secs() as usize;
            let measured_end = warmup_secs + duration.as_secs() as usize;
            tokio::spawn(async move {
                let mut cpu = Vec::new();
                let mut ticks = crate::driver::cpu_ticks();
                let mut reported = 0usize;
                while !done.load(Ordering::Relaxed) {
                    let next = clock.started() + Duration::from_secs(reported as u64 + 1);
                    tokio::time::sleep_until(
                        tokio::time::Instant::from_std(next) + Duration::from_millis(5),
                    )
                    .await;
                    let now = crate::driver::cpu_ticks();
                    let busy = match (ticks, now) {
                        (Some(before), Some(after)) => {
                            after.saturating_sub(before) as f64 / crate::driver::TICKS_PER_SEC
                                * 100.0
                        }
                        _ => 0.0,
                    };
                    ticks = now;
                    cpu.push(busy);
                    let phase = if reported < warmup_secs {
                        SecondPhase::Warmup
                    } else if reported < measured_end {
                        SecondPhase::Measure
                    } else {
                        SecondPhase::Drain
                    };
                    let summary = second_of(&seconds, reported).summary(Duration::from_secs(1));
                    progress.send(BenchEvent::Second(SecondSample {
                        at: reported as u64,
                        phase,
                        summary,
                        driver_cpu_pct: busy,
                    }));
                    reported += 1;
                }
                cpu
            })
        };
        for handle in handles {
            let _ = handle.await;
        }
        done.store(true, Ordering::Relaxed);
        let cpu = reporter.await.unwrap_or_default();
        // every stream's seconds, added up
        let len = seconds
            .iter()
            .map(|mine| mine.lock().expect("a stream's seconds").len())
            .max()
            .unwrap_or(0);
        let summed = (0..len).map(|second| second_of(&seconds, second)).collect();
        ArmOutcome {
            seconds: summed,
            cpu,
            ended_early: clock.ended_early(),
            deadline: clock.deadline(),
            feeds: Vec::new(),
        }
    }

    /// Read back every object a put was acknowledged for, at every size, and compare its bytes
    ///
    /// # Arguments
    ///
    /// * `concurrency` - How many reads are in flight at once
    /// * `progress` - Where the phase is said
    pub async fn verify(&self, concurrency: usize, progress: &Progress) -> VerifyFacts {
        progress.send(BenchEvent::Phase(Phase::Verify));
        // every acknowledged object, by its dataset and size
        let work: Vec<(usize, Option<u64>, u64)> = {
            let spaces = self.spaces.lock().expect("the namespaces");
            let mut work = Vec::new();
            for ((at, size), space) in spaces.iter() {
                for object in space.acked.lock().expect("the acknowledged").iter() {
                    work.push((*at, *size, *object));
                }
            }
            work.sort_unstable();
            work
        };
        self.check_all(work, concurrency).await
    }

    /// Read back a sample of what the last arm put at its size, past each namespace's preload,
    /// and compare its bytes: at most `limit` objects and `bytes` bytes, spread evenly over them
    ///
    /// What a store did with a run's puts is proved by reading some of them back, where reading
    /// all of them would take longer than the run did. The whole preload is read back at the
    /// end of a capture by [`ObjectDriver::verify`].
    ///
    /// # Arguments
    ///
    /// * `size` - The arm's size, or none for each description's own
    /// * `limit` - The most objects read back
    /// * `bytes` - The most bytes read back
    /// * `concurrency` - How many reads are in flight at once
    /// * `progress` - Where the phase is said
    pub async fn verify_new(
        &self,
        size: Option<u64>,
        limit: usize,
        bytes: u64,
        concurrency: usize,
        progress: &Progress,
    ) -> VerifyFacts {
        progress.send(BenchEvent::Phase(Phase::Verify));
        // every object put past the preload at this size, in index order
        let mut put: Vec<(usize, Option<u64>, u64)> = Vec::new();
        for at in 0..self.datasets.len() {
            let space = self.space(at, size);
            let preloaded = space.preloaded.load(Ordering::Acquire);
            let acked = space.acked.lock().expect("the acknowledged");
            put.extend(acked.range(preloaded..).map(|object| (at, size, *object)));
        }
        // as many as the bounds allow, taken at an even stride so the sample spans the run
        let mut room = bytes;
        let mut taken = 0usize;
        let stride = put.len().div_ceil(limit.max(1)).max(1);
        let mut work = Vec::new();
        for item in put.iter().step_by(stride) {
            let len = object_len(&self.datasets[item.0], item.1, item.2);
            if taken >= limit || len > room {
                break;
            }
            room -= len;
            taken += 1;
            work.push(*item);
        }
        self.check_all(work, concurrency).await
    }

    /// Read back a list of objects and compare each one's bytes
    ///
    /// # Arguments
    ///
    /// * `work` - The objects, by dataset, size and index
    /// * `concurrency` - How many reads are in flight at once
    async fn check_all(&self, work: Vec<(usize, Option<u64>, u64)>, concurrency: usize) -> VerifyFacts {
        // the list shared over the streams in order
        let work = Arc::new(work);
        let cursor = Arc::new(AtomicU64::new(0));
        let facts = Arc::new(Mutex::new(VerifyFacts::default()));
        let mut handles = Vec::with_capacity(concurrency);
        for stream in 0..concurrency.max(1) {
            let client = self.clients[stream % self.clients.len()].clone();
            let datasets = self.datasets.clone();
            let work = work.clone();
            let cursor = cursor.clone();
            let facts = facts.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    let next = cursor.fetch_add(1, Ordering::AcqRel) as usize;
                    let Some((at, size, object)) = work.get(next).copied() else {
                        return;
                    };
                    let checked = check(&*client, &datasets[at], size, object).await;
                    let mut facts = facts.lock().expect("the facts");
                    facts.checked += 1;
                    match checked {
                        Checked::Same => {}
                        Checked::Missing => facts.lost += 1,
                        Checked::Different => facts.misread += 1,
                        Checked::Failed(code) => *facts.errors.entry(code).or_default() += 1,
                    }
                }
            }));
        }
        for handle in handles {
            let _ = handle.await;
        }
        let facts = facts.lock().expect("the facts").clone();
        facts
    }

    /// Delete every object an arm put, at every size, past each namespace's preload, and forget
    /// it was acknowledged ([F94](../../docs/src/features/bench-object-endpoints.md))
    ///
    /// Run between arms, outside every window, so a store's devices hold its preload and no more
    /// when the next arm starts. Later puts keep making objects past the last one put, so no
    /// path is ever put again after its delete.
    ///
    /// # Arguments
    ///
    /// * `concurrency` - How many deletes are in flight at once
    /// * `progress` - Where the phase is said
    pub async fn clean(&self, concurrency: usize, progress: &Progress) -> CleanupFacts {
        progress.send(BenchEvent::Phase(Phase::Cleanup));
        let started = Instant::now();
        // every object put past a preload, taken out of what is acknowledged
        let work: Vec<(usize, Option<u64>, u64)> = {
            let spaces = self.spaces.lock().expect("the namespaces");
            let mut work = Vec::new();
            for ((at, size), space) in spaces.iter() {
                let preloaded = space.preloaded.load(Ordering::Acquire);
                let mut acked = space.acked.lock().expect("the acknowledged");
                let put = acked.split_off(&preloaded);
                work.extend(put.into_iter().map(|object| (*at, *size, object)));
            }
            work.sort_unstable();
            work
        };
        // shared over the streams in order, each delete counted
        let work = Arc::new(work);
        let cursor = Arc::new(AtomicU64::new(0));
        let facts = Arc::new(Mutex::new(CleanupFacts::default()));
        let mut handles = Vec::with_capacity(concurrency);
        for stream in 0..concurrency.max(1) {
            let client = self.clients[stream % self.clients.len()].clone();
            let datasets = self.datasets.clone();
            let work = work.clone();
            let cursor = cursor.clone();
            let facts = facts.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    let next = cursor.fetch_add(1, Ordering::AcqRel) as usize;
                    let Some((at, size, object)) = work.get(next).copied() else {
                        return;
                    };
                    let path = object_path(&datasets[at], size, object);
                    let deleted = client.delete(&datasets[at], &path).await;
                    let mut facts = facts.lock().expect("the facts");
                    match deleted {
                        Ok(()) => facts.deleted += 1,
                        Err(error) => *facts.errors.entry(error.code).or_default() += 1,
                    }
                }
            }));
        }
        for handle in handles {
            let _ = handle.await;
        }
        let mut facts = facts.lock().expect("the facts").clone();
        facts.secs = started.elapsed().as_secs_f64();
        facts
    }
}

/// One second of every stream, added up
///
/// # Arguments
///
/// * `seconds` - Every stream's seconds
/// * `second` - Which second
fn second_of(seconds: &[Mutex<Vec<Window>>], second: usize) -> Window {
    let mut sum = Window::default();
    for mine in seconds {
        if let Some(window) = mine.lock().expect("a stream's seconds").get(second) {
            sum.add(window);
        }
    }
    sum
}

/// The path an object has in its namespace: its size's label, then the description's path
///
/// # Arguments
///
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
#[must_use]
pub fn object_path(dataset: &ObjectDataset, size: Option<u64>, object: u64) -> String {
    format!(
        "{}/{}",
        size_label(size),
        dataset.description.path_of(object)
    )
}

/// The length of an object in its namespace
///
/// # Arguments
///
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
#[must_use]
pub fn object_len(dataset: &ObjectDataset, size: Option<u64>, object: u64) -> u64 {
    size.unwrap_or_else(|| dataset.description.size_of(object))
}

/// The generator's object an object of a namespace is made from, so two sizes' objects of one
/// index never hold the same bytes
///
/// # Arguments
///
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
fn stream_of(size: Option<u64>, object: u64) -> u64 {
    object ^ mix(size.unwrap_or(0)).wrapping_mul(GAMMA)
}

/// Make an object's bytes
///
/// # Arguments
///
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
#[must_use]
pub fn make(dataset: &ObjectDataset, size: Option<u64>, object: u64) -> Vec<u8> {
    // filled a piece at a time, as a stream would fill its frames
    let len = object_len(dataset, size, object) as usize;
    let mut bytes = vec![0u8; len];
    let seed = dataset.description.seed;
    let stream = stream_of(size, object);
    for (at, piece) in bytes.chunks_mut(MAKE_PIECE).enumerate() {
        fill(seed, stream, (at * MAKE_PIECE) as u64, piece);
    }
    bytes
}

/// The failure an endpoint's error is counted as, its message cut to a line
///
/// # Arguments
///
/// * `error` - The error
fn failed(error: &EndpointError) -> Outcome {
    Outcome::Failed {
        code: error.code.clone(),
        message: error.message.chars().take(300).collect(),
    }
}

/// Put one object, made as it goes
///
/// Returns how it ended, the bytes it sent, and the bytes it was answered with.
///
/// # Arguments
///
/// * `client` - The endpoint
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
async fn put<E: ObjectEndpoint>(
    client: &E,
    dataset: &ObjectDataset,
    size: Option<u64>,
    object: u64,
) -> (Outcome, u64, u64) {
    // the bytes are made on the stream that sends them, and timed with the put
    let started = Instant::now();
    let bytes = Bytes::from(make(dataset, size, object));
    let path = object_path(dataset, size, object);
    let sent = bytes.len() as u64;
    // the endpoint is handed a reference of its own, so the bytes are freed once the time is taken
    match client.put(dataset, &path, bytes.clone()).await {
        Ok(()) => (Outcome::Ok(started.elapsed()), sent, 0),
        Err(error) => (failed(&error), sent, 0),
    }
}

/// Write part of one object in place with the bytes it already holds there
///
/// Rewriting its own bytes keeps every later read and the verify pass exact, while every write
/// still stages, commits and applies, or rides its commit under the pool's small-write
/// threshold. Returns how it ended and the bytes it sent.
///
/// # Arguments
///
/// * `client` - The endpoint
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
/// * `offset` - Where in it the write starts
/// * `len` - How many bytes it writes, fewer past the object's end
async fn patch<E: ObjectEndpoint>(
    client: &E,
    dataset: &ObjectDataset,
    size: Option<u64>,
    object: u64,
    offset: u64,
    len: u64,
) -> (Outcome, u64) {
    let started = Instant::now();
    // the object's own bytes at the offset, made again on the stream that sends them
    let total = object_len(dataset, size, object);
    let end = (offset + len).min(total);
    let mut bytes = vec![0u8; (end.saturating_sub(offset)) as usize];
    fill(dataset.description.seed, stream_of(size, object), offset, &mut bytes);
    let bytes = Bytes::from(bytes);
    let sent = bytes.len() as u64;
    let path = object_path(dataset, size, object);
    match client.write_at(dataset, &path, offset, bytes.clone()).await {
        Ok(()) => (Outcome::Ok(started.elapsed()), sent),
        Err(error) => (failed(&error), sent),
    }
}

/// Read one object, whole or as a stat, and check what a get answered
///
/// Returns how it ended and the bytes it was answered with.
///
/// # Arguments
///
/// * `client` - The endpoint
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
/// * `whole` - Whether to read its bytes, or only stat it
/// * `level` - The level it is read at
async fn read<E: ObjectEndpoint>(
    client: &E,
    dataset: &ObjectDataset,
    size: Option<u64>,
    object: u64,
    whole: bool,
    level: ReadLevel,
) -> (Outcome, u64) {
    let started = Instant::now();
    let path = object_path(dataset, size, object);
    let answered = if whole {
        client.get(dataset, &path, level).await.map(Some)
    } else {
        client.stat(dataset, &path, level).await.map(|()| None)
    };
    match answered {
        Ok(bytes) => {
            let elapsed = started.elapsed();
            let received = bytes.as_ref().map_or(0, |bytes| bytes.len() as u64);
            // a get's bytes are the object's, made again, or the read is a failure
            if let Some(bytes) = bytes {
                if bytes != make(dataset, size, object) {
                    return (
                        Outcome::Failed {
                            code: "Misread".to_string(),
                            message: format!("{path} read back other bytes than were put"),
                        },
                        received,
                    );
                }
            }
            (Outcome::Ok(elapsed), received)
        }
        // a preloaded object not found is the store's failure
        Err(error) if error.kind == ErrorKind::NotFound => (Outcome::Miss, 0),
        Err(error) => (failed(&error), 0),
    }
}

/// What reading an acknowledged object back found
#[derive(Debug)]
enum Checked {
    /// Its bytes, exactly
    Same,
    /// Nothing at its path
    Missing,
    /// Other bytes than were put
    Different,
    /// The read failed, by this code
    Failed(String),
}

/// Read an acknowledged object back and compare it with its bytes made again
///
/// # Arguments
///
/// * `client` - The endpoint
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
async fn check<E: ObjectEndpoint>(
    client: &E,
    dataset: &ObjectDataset,
    size: Option<u64>,
    object: u64,
) -> Checked {
    let path = object_path(dataset, size, object);
    // an acknowledged object is judged by what the store committed, so the read back is at the
    // store's default whatever level the arms read at
    match client.get(dataset, &path, ReadLevel::Default).await {
        Ok(bytes) if bytes == make(dataset, size, object) => Checked::Same,
        Ok(_) => Checked::Different,
        Err(error) => match error.kind {
            ErrorKind::NotFound => Checked::Missing,
            // bytes that do not hold the checksum the store answered with were misread on the way
            ErrorKind::Corrupt => Checked::Different,
            ErrorKind::Retriable | ErrorKind::Other => Checked::Failed(error.code),
        },
    }
}

/// The describing files a dataset folder's buckets were read from, by bucket, for a capture
#[must_use]
pub fn digests(datasets: &[ObjectDataset]) -> BTreeMap<String, String> {
    datasets
        .iter()
        .map(|dataset| (dataset.bucket.clone(), dataset.digest.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Description, GENERATOR, ObjectMix, SizeDistribution, fill, size_label};

    /// `splitmix64-ctr/1` gives the bytes X13 recorded on titan, hyperion and europa, from every
    /// build: the CRC-64/NVME of three cells, near and far
    ///
    /// The cells and digests are X13's (`shoal-spike/src/driver/generate.rs`, `DIGEST_CELLS`, its
    /// `x13-*.json` records at seed `0x5831_3133`). A generator's bytes are what every described
    /// dataset's objects are; if this fails, the definition moved and is a new name.
    #[test]
    fn splitmix64_ctr_is_frozen() {
        let seed = 0x5831_3133;
        let cells: [(u64, u64, usize, u64); 3] = [
            (0, 0, 1 << 20, 0xbc0c_e121_7b84_7dea),
            (12_345, (3 << 30) + 5, 100 << 10, 0x98bc_4b40_9b81_4409),
            (u64::MAX, (1 << 40) - 7, 4096, 0x581f_87f0_10f0_87ba),
        ];
        for (object, offset, len, frozen) in cells {
            let mut bytes = vec![0u8; len];
            fill(seed, object, offset, &mut bytes);
            assert_eq!(
                shoal::shared::checksum::crc64(&bytes),
                frozen,
                "object {object} at {offset}"
            );
        }
    }

    /// A fill from any offset is that slice of a fill from zero
    #[test]
    fn a_fill_seeks() {
        let mut whole = vec![0u8; 1 << 16];
        fill(7, 9, 0, &mut whole);
        for (offset, len) in [
            (0usize, 1usize),
            (1, 7),
            (3, 4096),
            (4095, 2),
            (777, 30_000),
        ] {
            let mut part = vec![0u8; len];
            fill(7, 9, offset as u64, &mut part);
            assert_eq!(part, whole[offset..offset + len], "{offset}+{len}");
        }
    }

    /// A description reads from its file, sizes are drawn within their distribution, and a
    /// description of another generator or of nothing is refused
    #[test]
    fn descriptions_read_and_draw() {
        let text = "objects: 100\nsizes: {uniform: [4KiB, 64KiB]}\nseed: 7\n";
        let description: Description = serde_yaml::from_str(text).unwrap();
        assert_eq!(description.generator, GENERATOR);
        assert!(description.check().is_ok());
        for object in 0..100 {
            let size = description.size_of(object);
            assert!((4096..=65_536).contains(&size), "{size}");
            assert_eq!(
                size,
                description.size_of(object),
                "a size is drawn again alike"
            );
        }
        assert_ne!(description.path_of(1), description.path_of(2));
        // every form reads, and a key nobody knows is refused
        for sizes in [
            "{fixed: 1MiB}",
            "{doublings: [4KiB, 1MiB]}",
            "{table: [[4KiB, 3], [1MiB, 1]]}",
        ] {
            let text = format!("objects: 5\nsizes: {sizes}\n");
            let description: Description = serde_yaml::from_str(&text).unwrap();
            assert!(description.check().is_ok(), "{sizes}");
        }
        assert!(
            serde_yaml::from_str::<Description>("objects: 5\nsizes: {fixed: 1}\nseeds: 2\n")
                .is_err()
        );
        let other = Description {
            objects: 5,
            sizes: SizeDistribution::Fixed(1),
            seed: 0,
            generator: "aes-ctr/1".to_string(),
        };
        assert!(other.check().is_err());
        // and two descriptions alike digest alike
        assert_eq!(description.digest(), description.clone().digest());
    }

    /// Mixes read by name and by weights, and spell back as what they read from
    #[test]
    fn mixes_read_and_spell_back() {
        let named: ObjectMix = "rw50".parse().unwrap();
        assert_eq!((named.get, named.put, named.stat), (50, 50, 0));
        let weighed: ObjectMix = "stat:10,get:80,put:10".parse().unwrap();
        assert_eq!(weighed.name, "get80-put10-stat10");
        let again: ObjectMix = String::from(weighed.clone()).parse().unwrap();
        assert_eq!(again, weighed);
        // writes in place, named and weighed, a write and a read of what was preloaded (F86)
        let patch: ObjectMix = "patch100-32k".parse().unwrap();
        assert_eq!((patch.patch, patch.patch_bytes), (100, 32 << 10));
        assert!(patch.writes() && patch.reads());
        let mixed: ObjectMix = "get:50,patch:50".parse().unwrap();
        assert_eq!(mixed.name, "get50-put0-stat0-patch50");
        assert_eq!(String::from(mixed.clone()).parse::<ObjectMix>().unwrap(), mixed);
        assert!("scan:1".parse::<ObjectMix>().is_err());
        assert!("get:0".parse::<ObjectMix>().is_err());
        assert_eq!(size_label(Some(4096)), "s4k");
        assert_eq!(size_label(Some(16 << 20)), "s16m");
        assert_eq!(size_label(Some(1000)), "s1000");
        assert_eq!(size_label(None), "sd");
    }

    /// What a [`Memory`] store holds, by path, shared by every endpoint of it
    type Held = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, bytes::Bytes>>>;

    /// An endpoint of a store that keeps its objects in memory, counting what this endpoint was
    /// asked; every call yields once, as a call that waits on a socket would (F94)
    #[derive(Default)]
    struct Memory {
        /// Every object the store holds, by path
        objects: Held,
        /// How many operations this endpoint was asked to do
        calls: std::sync::atomic::AtomicU64,
        /// Whether it writes in place
        in_place: bool,
        /// Every read it was asked for, by kind and the level it was asked at (F95)
        levels: std::sync::Mutex<Vec<(&'static str, crate::spec::ReadLevel)>>,
    }

    impl Memory {
        /// Another endpoint of the same store
        fn another(&self) -> Memory {
            Memory {
                objects: self.objects.clone(),
                ..Memory::default()
            }
        }

        /// Count a call and give the runtime its turn
        async fn called(&self) {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    }

    impl super::ObjectEndpoint for Memory {
        /// As it was told
        fn writes_in_place(&self) -> bool {
            self.in_place
        }

        /// Keep the bytes
        async fn put(&self, _: &super::ObjectDataset, path: &str, bytes: bytes::Bytes) -> Result<(), super::EndpointError> {
            self.called().await;
            self.objects.lock().unwrap().insert(path.to_string(), bytes);
            Ok(())
        }

        /// Answer what it keeps, or nothing found
        async fn get(&self, _: &super::ObjectDataset, path: &str, level: crate::spec::ReadLevel) -> Result<bytes::Bytes, super::EndpointError> {
            self.called().await;
            self.levels.lock().unwrap().push(("get", level));
            self.objects.lock().unwrap().get(path).cloned().ok_or_else(|| {
                super::EndpointError::new(super::ErrorKind::NotFound, "NotFound", path)
            })
        }

        /// Whether it keeps it
        async fn stat(&self, _: &super::ObjectDataset, path: &str, level: crate::spec::ReadLevel) -> Result<(), super::EndpointError> {
            self.called().await;
            self.levels.lock().unwrap().push(("stat", level));
            match self.objects.lock().unwrap().contains_key(path) {
                true => Ok(()),
                false => Err(super::EndpointError::new(super::ErrorKind::NotFound, "NotFound", path)),
            }
        }

        /// Forget it
        async fn delete(&self, _: &super::ObjectDataset, path: &str) -> Result<(), super::EndpointError> {
            self.called().await;
            self.objects.lock().unwrap().remove(path);
            Ok(())
        }
    }

    /// A dataset of a bucket of this many 4 KiB objects
    ///
    /// # Arguments
    ///
    /// * `objects` - How many it describes
    fn dataset(objects: u64) -> super::ObjectDataset {
        let description = Description {
            objects,
            sizes: SizeDistribution::Fixed(4096),
            seed: 3,
            generator: GENERATOR.to_string(),
        };
        super::ObjectDataset {
            bucket: "Images".to_string(),
            path: "Images.yaml".into(),
            digest: description.digest(),
            description,
        }
    }

    /// Run one arm of a mix at a concurrency for a moment
    ///
    /// # Arguments
    ///
    /// * `driver` - The driver
    /// * `mix` - The mix
    /// * `concurrency` - How many operations are in flight
    async fn run(driver: &super::ObjectDriver<Memory>, mix: &str, concurrency: usize) -> crate::window::Window {
        let arm = super::ObjectArm {
            mix: mix.parse().unwrap(),
            size: Some(4096),
            concurrency,
        };
        let clock = crate::driver::ArmClock::start(std::time::Duration::from_millis(200));
        let outcome = driver
            .run_arm(&arm, std::time::Duration::ZERO, std::time::Duration::from_millis(200), 7, mix, clock, &crate::progress::Progress::none())
            .await;
        crate::window::Window::sum(&outcome.seconds)
    }

    /// The driver over an endpoint that is not a Shoal client drives every mix, counts each
    /// kind apart, reads back what it put, and spreads its streams over its endpoints in turn
    #[tokio::test]
    async fn a_memory_endpoint_drives_every_mix() {
        use std::sync::atomic::Ordering;
        let one = Memory::default();
        let (two, one) = (std::sync::Arc::new(one.another()), std::sync::Arc::new(one));
        let driver = super::ObjectDriver::new(vec![one.clone(), two.clone()], vec![dataset(1 << 20)], super::ObjectPreload::Objects(16));
        // the preload, sixteen objects shared over both endpoints' streams
        let (windows, _) = driver.preload(Some(4096), 2, &crate::progress::Progress::none()).await.unwrap();
        assert_eq!(crate::window::Window::sum(&windows).summary(std::time::Duration::from_secs(1)).kinds["put"].ok, 16);
        assert!(one.calls.load(Ordering::Relaxed) > 0 && two.calls.load(Ordering::Relaxed) > 0);
        for mix in ["put100", "get100", "stat100", "rw50"] {
            let window = run(&driver, mix, 4).await;
            let summary = window.summary(std::time::Duration::from_secs(1));
            // every operation it ran succeeded, under the kinds its mix names
            for (kind, counted) in &summary.kinds {
                assert!(counted.ok > 0 && counted.failed() == 0, "{mix} {kind}: {counted:?}");
            }
        }
        // four streams take the two endpoints in turn, so each was asked about as often
        let (asked_one, asked_two) = (one.calls.load(Ordering::Relaxed), two.calls.load(Ordering::Relaxed));
        assert!(asked_one.abs_diff(asked_two) * 4 <= asked_one.max(asked_two), "{asked_one} against {asked_two}");
        // and everything acknowledged reads back exactly, with nothing put twice
        let facts = driver.verify(4, &crate::progress::Progress::none()).await;
        assert_eq!((facts.lost, facts.misread), (0, 0), "{facts:?}");
        assert!(!driver.took_wrap());
    }

    /// An arm's gets and stats are asked at the driver's level, and the read back at the store's
    /// default whatever the arms read at (F95)
    #[tokio::test]
    async fn arms_read_at_the_drivers_level_and_the_read_back_at_the_default() {
        use crate::spec::ReadLevel;
        let memory = std::sync::Arc::new(Memory::default());
        let driver = super::ObjectDriver::new(vec![memory.clone()], vec![dataset(1 << 20)], super::ObjectPreload::Objects(8))
            .reading_at(ReadLevel::One);
        driver.preload(Some(4096), 2, &crate::progress::Progress::none()).await.unwrap();
        // every read an arm made, a get or a stat, was asked at one
        for mix in ["get100", "stat100"] {
            run(&driver, mix, 2).await;
        }
        let asked = std::mem::take(&mut *memory.levels.lock().unwrap());
        assert!(asked.iter().any(|(kind, _)| *kind == "get"), "{asked:?}");
        assert!(asked.iter().any(|(kind, _)| *kind == "stat"), "{asked:?}");
        assert!(asked.iter().all(|(_, level)| *level == ReadLevel::One), "{asked:?}");
        // and what was acknowledged is read back at the default
        let facts = driver.verify(2, &crate::progress::Progress::none()).await;
        assert_eq!((facts.lost, facts.misread), (0, 0), "{facts:?}");
        let asked = memory.levels.lock().unwrap().clone();
        assert!(!asked.is_empty(), "the read back read nothing");
        assert!(asked.iter().all(|entry| *entry == ("get", ReadLevel::Default)), "{asked:?}");
    }

    /// An object not found is a miss to a read and lost to a read back, and other bytes than
    /// were put are a misread
    #[tokio::test]
    async fn not_found_misses_and_corrupt_differs() {
        let memory = std::sync::Arc::new(Memory::default());
        let driver = super::ObjectDriver::new(vec![memory.clone()], vec![dataset(1 << 20)], super::ObjectPreload::Objects(4));
        driver.preload(Some(4096), 1, &crate::progress::Progress::none()).await.unwrap();
        // one object lost and one changed under the driver
        {
            let mut objects = memory.objects.lock().unwrap();
            let mut paths: Vec<String> = objects.keys().cloned().collect();
            paths.sort();
            objects.remove(&paths[0]);
            objects.insert(paths[1].clone(), bytes::Bytes::from_static(b"other bytes"));
        }
        let facts = driver.verify(2, &crate::progress::Progress::none()).await;
        assert_eq!((facts.checked, facts.lost, facts.misread), (4, 1, 1), "{facts:?}");
        // and a read of the lost one is a miss, of the changed one a misread
        let window = run(&driver, "get100", 1).await;
        let summary = window.summary(std::time::Duration::from_secs(1));
        assert!(summary.kinds["get"].misses > 0, "{summary:?}");
        assert!(summary.kinds["get"].errors.contains_key("Misread"), "{summary:?}");
    }

    /// Cleaning deletes what the arms put and nothing the preload put, a sample of what an arm
    /// put is read back, and later puts make objects past the last one put
    #[tokio::test]
    async fn cleanup_deletes_only_what_arms_put() {
        let memory = std::sync::Arc::new(Memory::default());
        let driver = super::ObjectDriver::new(vec![memory.clone()], vec![dataset(1 << 20)], super::ObjectPreload::Objects(8));
        driver.preload(Some(4096), 2, &crate::progress::Progress::none()).await.unwrap();
        run(&driver, "put100", 2).await;
        let put = memory.objects.lock().unwrap().len() - 8;
        assert!(put > 0);
        // a read back of at most five of them, and of no more bytes than three objects hold
        let sample = driver.verify_new(Some(4096), 5, 3 * 4096, 2, &crate::progress::Progress::none()).await;
        assert_eq!((sample.checked, sample.lost, sample.misread), (put.min(3) as u64, 0, 0), "{sample:?}");
        // the deletes take the arm's objects and leave the preload's
        let cleaned = driver.clean(3, &crate::progress::Progress::none()).await;
        assert_eq!(cleaned.deleted, put as u64);
        assert!(cleaned.errors.is_empty());
        assert_eq!(memory.objects.lock().unwrap().len(), 8);
        assert_eq!(driver.acknowledged(), 8);
        // and the next arm's puts are of paths never put before
        let before: std::collections::HashSet<String> = memory.objects.lock().unwrap().keys().cloned().collect();
        run(&driver, "put100", 1).await;
        let fresh = memory.objects.lock().unwrap().keys().filter(|path| !before.contains(*path)).count();
        assert!(fresh > 0);
        assert_eq!(driver.verify(2, &crate::progress::Progress::none()).await.lost, 0);
    }

    /// A put past the description's last object is marked, once a run, so a capture says its
    /// objects were put twice rather than counting them as new
    #[tokio::test]
    async fn a_wrapping_put_is_marked() {
        let memory = std::sync::Arc::new(Memory::default());
        let driver = super::ObjectDriver::new(vec![memory], vec![dataset(6)], super::ObjectPreload::Objects(4));
        driver.preload(Some(4096), 1, &crate::progress::Progress::none()).await.unwrap();
        assert!(!driver.took_wrap());
        // two objects past the preload, and then the description's start again
        run(&driver, "put100", 1).await;
        assert!(driver.took_wrap());
        // the flag is taken by the asking, so the next run starts clear
        assert!(!driver.took_wrap());
    }

    /// An arm that writes in place is refused by name through an endpoint that cannot, and a
    /// preload counted in objects puts that many, where before F94 a count put half of them
    #[tokio::test]
    async fn patch_is_refused_without_writes_in_place() {
        let s3_like = super::ObjectDriver::new(vec![std::sync::Arc::new(Memory::default())], vec![dataset(64)], 50.0);
        let arm = super::ObjectArm {
            mix: "patch100".parse().unwrap(),
            size: Some(4096),
            concurrency: 1,
        };
        assert!(s3_like.refuses(&arm).unwrap().contains("writes objects in place"));
        let put: super::ObjectArm = super::ObjectArm {
            mix: "put100".parse().unwrap(),
            ..arm.clone()
        };
        assert!(s3_like.refuses(&put).is_none());
        let in_place = super::ObjectDriver::new(
            vec![std::sync::Arc::new(Memory {
                in_place: true,
                ..Memory::default()
            })],
            vec![dataset(64)],
            50.0,
        );
        assert!(in_place.refuses(&arm).is_none());
        // a count of objects, and a share, and a count past the description
        let description = dataset(64).description;
        assert_eq!(super::ObjectPreload::Objects(10).count(&description), 10);
        assert_eq!(super::ObjectPreload::Percent(50.0).count(&description), 32);
        assert_eq!(super::ObjectPreload::Objects(1000).count(&description), 64);
    }
}
