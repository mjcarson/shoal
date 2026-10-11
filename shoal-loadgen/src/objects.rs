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
//!   bytes made again; a missing or differing object fails the run.
//!
//! # Invariants
//!
//! **Nothing here names a bucket.** A dataset's buckets are the schema's, found by name, and every
//! operation goes through the client's untyped [`Objects`], as nothing in the driver names a table.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shoal::client::{Errors, Shoal};
use shoal::shared::identity::BucketId;
use shoal::shared::pools::ByteSize;
use shoal::shared::protocol::object::ObjectOutcome;
use shoal::shared::traits::QuerySupport;

use crate::driver::{ArmClock, ArmOutcome};
use crate::progress::{BenchEvent, Phase, Progress};
use crate::results::{SecondPhase, SecondSample, VerifyFacts};
use crate::seed::Seeded;
use crate::window::{OpKind, Outcome, Window};

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

impl ObjectDataset {
    /// The bucket's identity
    #[must_use]
    pub fn id(&self) -> BucketId {
        BucketId::of(&self.bucket)
    }
}

/// What an object arm does: puts, gets and stats by weight
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
}

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
    #[must_use]
    pub fn writes(&self) -> bool {
        self.put > 0
    }

    /// Whether it reads objects that were preloaded
    #[must_use]
    pub fn reads(&self) -> bool {
        self.get > 0 || self.stat > 0
    }
}

impl std::str::FromStr for ObjectMix {
    type Err = String;

    /// Read a mix by its name - `put100`, `get100`, `stat100`, `rw50` - or as weights, `get:N,put:M,stat:K`
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
        };
        match text {
            "put100" => return Ok(named(0, 100, 0)),
            "get100" => return Ok(named(100, 0, 0)),
            "stat100" => return Ok(named(0, 0, 100)),
            "rw50" => return Ok(named(50, 50, 0)),
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
                other => return Err(format!("{other:?} in {text:?} is not get, put or stat")),
            }
        }
        if mix.get + mix.put + mix.stat == 0 {
            return Err(format!("{text:?} weighs nothing"));
        }
        mix.name = format!("get{}-put{}-stat{}", mix.get, mix.put, mix.stat);
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
            "put100" | "get100" | "stat100" | "rw50" => mix.name,
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

/// The objects one size makes of one dataset: a namespace of paths of their own
#[derive(Debug)]
struct Namespace {
    /// How many objects the preload put
    preloaded: AtomicU64,
    /// The next object a put makes, past the preloaded ones
    next: AtomicU64,
    /// Every object a put was acknowledged for
    acked: Mutex<BTreeSet<u64>>,
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
}

/// Drives a cluster's buckets through its members' clients
pub struct ObjectDriver<S: QuerySupport> {
    /// One client a member, in the order streams are spread over them
    clients: Vec<Arc<Shoal<S>>>,
    /// Every described bucket, in the schema's order
    datasets: Vec<ObjectDataset>,
    /// The share of each description's objects the preload puts, in percent
    preload_percent: f64,
    /// The objects each dataset and size made, by both
    spaces: Mutex<HashMap<(usize, Option<u64>), Arc<Namespace>>>,
}

impl<S> ObjectDriver<S>
where
    S: QuerySupport + Send + Sync + 'static,
{
    /// A driver over these members' clients and these buckets
    ///
    /// # Arguments
    ///
    /// * `clients` - One client a member
    /// * `datasets` - Every described bucket
    /// * `preload_percent` - The share of each description's objects put before any arm
    ///
    /// # Panics
    ///
    /// When there is no client or no dataset.
    #[must_use]
    pub fn new(
        clients: Vec<Arc<Shoal<S>>>,
        datasets: Vec<ObjectDataset>,
        preload_percent: f64,
    ) -> Self {
        assert!(!clients.is_empty(), "a driver needs a client");
        assert!(
            !datasets.is_empty(),
            "an object driver needs a described bucket"
        );
        ObjectDriver {
            clients,
            datasets,
            preload_percent,
            spaces: Mutex::new(HashMap::new()),
        }
    }

    /// The buckets this driver drives
    #[must_use]
    pub fn datasets(&self) -> &[ObjectDataset] {
        &self.datasets
    }

    /// Drive the same buckets, remembering every object acknowledged so far, through other
    /// clients: the members of a cluster started again
    ///
    /// # Arguments
    ///
    /// * `clients` - One client a member
    ///
    /// # Panics
    ///
    /// When there is no client.
    pub fn reconnect(&mut self, clients: Vec<Arc<Shoal<S>>>) {
        assert!(!clients.is_empty(), "a driver needs a client");
        self.clients = clients;
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
                })
            })
            .clone()
    }

    /// How many objects a description's preload puts
    ///
    /// # Arguments
    ///
    /// * `description` - The description
    fn preload_count(&self, description: &Description) -> u64 {
        // at least one, so a read always has something to ask for
        let count = (description.objects as f64 * self.preload_percent / 100.0).ceil() as u64;
        count.clamp(1, description.objects)
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
                work.push((at, space, self.preload_count(&dataset.description)));
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
                        let (outcome, sent, _) = put(&client, &dataset, size, object).await;
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
                    let total = u64::from(mix.get + mix.put + mix.stat);
                    let weight = draw.below(total);
                    let op = if weight < u64::from(mix.get) {
                        Op::Get
                    } else if weight < u64::from(mix.get + mix.put) {
                        Op::Put
                    } else {
                        Op::Stat
                    };
                    let second = clock.second();
                    let (kind, outcome, sent, received) = match op {
                        Op::Put => {
                            // the next object past the preload, wrapping over the description
                            let next = space.next.fetch_add(1, Ordering::AcqRel);
                            let object = next % dataset.description.objects;
                            let (outcome, sent, received) =
                                put(&client, dataset, size, object).await;
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
                                read(&client, dataset, size, object, whole).await;
                            (if whole { "get" } else { "stat" }, outcome, 0, received)
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
                    let checked = check(&client, &datasets[at], size, object).await;
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

/// The code an error is counted under
///
/// # Arguments
///
/// * `error` - The error
fn code_of(error: &Errors) -> String {
    match error {
        Errors::Server { code, .. } => format!("{code:?}"),
        Errors::Object { outcome, .. } => outcome.name().to_string(),
        other => format!("{other:?}").chars().take(40).collect(),
    }
}

/// Put one object, made as it goes
///
/// Returns how it ended, the bytes it sent, and the bytes it was answered with.
///
/// # Arguments
///
/// * `client` - The client
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
async fn put<S: QuerySupport>(
    client: &Shoal<S>,
    dataset: &ObjectDataset,
    size: Option<u64>,
    object: u64,
) -> (Outcome, u64, u64) {
    // the bytes are made on the stream that sends them, and timed with the put
    let started = Instant::now();
    let bytes = make(dataset, size, object);
    let path = object_path(dataset, size, object);
    let sent = bytes.len() as u64;
    match client.objects(dataset.id()).put(&path, &bytes).await {
        Ok(_) => (Outcome::Ok(started.elapsed()), sent, 0),
        Err(error) => (
            Outcome::Failed {
                code: code_of(&error),
                message: format!("{error:?}").chars().take(300).collect(),
            },
            sent,
            0,
        ),
    }
}

/// Read one object, whole or as a stat, and check what a get answered
///
/// Returns how it ended and the bytes it was answered with.
///
/// # Arguments
///
/// * `client` - The client
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
/// * `whole` - Whether to read its bytes, or only stat it
async fn read<S: QuerySupport>(
    client: &Shoal<S>,
    dataset: &ObjectDataset,
    size: Option<u64>,
    object: u64,
    whole: bool,
) -> (Outcome, u64) {
    let started = Instant::now();
    let path = object_path(dataset, size, object);
    let objects = client.objects(dataset.id());
    let answered = if whole {
        objects.get(&path).await.map(|(_, bytes)| Some(bytes))
    } else {
        objects.stat(&path).await.map(|_| None)
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
        // a preloaded object not found is the cluster's failure
        Err(Errors::Object {
            outcome: ObjectOutcome::NotFound,
            ..
        }) => (Outcome::Miss, 0),
        Err(error) => (
            Outcome::Failed {
                code: code_of(&error),
                message: format!("{error:?}").chars().take(300).collect(),
            },
            0,
        ),
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
/// * `client` - The client
/// * `dataset` - Its dataset
/// * `size` - The size, or none for the description's own
/// * `object` - Its index
async fn check<S: QuerySupport>(
    client: &Shoal<S>,
    dataset: &ObjectDataset,
    size: Option<u64>,
    object: u64,
) -> Checked {
    let path = object_path(dataset, size, object);
    match client.objects(dataset.id()).get(&path).await {
        Ok((_, bytes)) if bytes == make(dataset, size, object) => Checked::Same,
        Ok(_) => Checked::Different,
        Err(Errors::Object {
            outcome: ObjectOutcome::NotFound,
            ..
        }) => Checked::Missing,
        // bytes that do not hold the checksum the server answered with were misread on the way
        Err(Errors::Object {
            outcome: ObjectOutcome::ChecksumMismatch,
            ..
        }) => Checked::Different,
        Err(error) => Checked::Failed(code_of(&error)),
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
        assert!("scan:1".parse::<ObjectMix>().is_err());
        assert!("get:0".parse::<ObjectMix>().is_err());
        assert_eq!(size_label(Some(4096)), "s4k");
        assert_eq!(size_label(Some(16 << 20)), "s16m");
        assert_eq!(size_label(Some(1000)), "s1000");
        assert_eq!(size_label(None), "sd");
    }
}
