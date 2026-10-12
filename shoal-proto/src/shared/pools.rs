//! Storage pools and the bindings that make a bucket one of a pool's consumers
//!
//! A pool and its redundancy are bound in the deployment, not in the schema
//! ([S4](../../../docs/src/object-storage/pools-and-devices.md#pools-and-bindings-are-policy)):
//! a schema declares `posters: Bucket<Posters>`, and a deployment says which pool `Posters`
//! stores its objects in. Both are cluster policy - read from the bootstrapping node's
//! configuration, committed by the control group, and from then on a joining node's copy is
//! ignored, as the replication factor is - and a standalone node reads its own.
//!
//! ```yaml
//! storage:
//!   pools:
//!     fast:
//!       class: ssd
//!       redundancy: {replicas: 3}
//!       failure_domain: host
//!       inline: 16KiB
//!   buckets:
//!     Posters: {pool: fast}
//! ```
//!
//! The types here are what a node's configuration, the control group's state, a shard's map and
//! `shoaladm`'s inventory all hold, so none of them can read a pool another way.
//!
//! # Invariants
//!
//! **Nothing here reaches the schema fingerprint**, so rebinding a bucket is never a schema
//! change ([S2](../../../docs/src/object-storage/buckets.md#what-does-not-reach-the-fingerprint)).
//!
//! **A consumer id is minted once and never reused**: a binding keeps the id it was given, and a
//! new binding takes one past every id ever given ([`ObjectPolicy::bind`]).
//!
//! **Unknown keys are refused**, so a key from a build that knows more - `devices`, say - is never
//! read as a pool without them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::shared::grants::Served;
use crate::shared::objects::{Geometry, DEFAULT_INLINE_THRESHOLD};

/// The largest inline threshold a pool may set: the most bytes an object held in its row has
///
/// An inline object is a row, replicated through its tablet group's log at the cluster's factor.
/// 16 MiB is a quarter of a frame and two append batches' worth, far under the entry no peer frame
/// carries ([item 208](../../../docs/src/appendix/known-issues.md)), and is where M13's baseline of
/// candidate A stops ([F80](../../../docs/src/features/object-wire.md)).
pub const MAX_INLINE_THRESHOLD: u64 = 16 << 20;

/// The stripe a pool cuts an object into when its operator names none
///
/// Provisional until [Q20](../../../docs/src/object-storage/contract.md) fixes the geometry; no
/// pool holds a stripe before M15, so no object's stripes are cut by it yet.
pub const DEFAULT_STRIPE_BYTES: u64 = 4 << 20;

/// The chunk unit a pool checksums and writes in when its operator names none
///
/// Provisional as [`DEFAULT_STRIPE_BYTES`] is: X9 and X12 measured 64 KiB and 1 MiB units.
pub const DEFAULT_UNIT_BYTES: u64 = 64 << 10;

/// A count of bytes, written in a configuration as a number or with a unit
///
/// `16384`, `16KiB`, `16Ki`, `16K` and `16kb` are all 16,384: a unit is a power of two, as every
/// size in Shoal's configuration is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// The bytes
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Parse a size written with an optional unit
    ///
    /// # Arguments
    ///
    /// * `text` - The size
    ///
    /// # Errors
    ///
    /// Text that is not a number with a unit this reads, or a size past a `u64`.
    pub fn parse(text: &str) -> Result<Self, String> {
        // split the digits from whatever unit follows them
        let text = text.trim();
        let digits = text
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(text.len());
        let (number, unit) = text.split_at(digits);
        let number: u64 = number
            .parse()
            .map_err(|_| format!("{text:?} is not a size: it starts with no number"))?;
        // every unit is a power of two
        let shift = match unit.trim().to_ascii_lowercase().as_str() {
            "" | "b" => 0,
            "k" | "ki" | "kb" | "kib" => 10,
            "m" | "mi" | "mb" | "mib" => 20,
            "g" | "gi" | "gb" | "gib" => 30,
            "t" | "ti" | "tb" | "tib" => 40,
            other => {
                return Err(format!(
                    "{text:?} names a unit this does not read: {other:?}"
                ))
            }
        };
        number
            .checked_mul(1u64 << shift)
            .map(ByteSize)
            .ok_or_else(|| format!("{text:?} is past the largest size there is"))
    }
}

impl std::fmt::Display for ByteSize {
    /// Write the size in the largest unit that divides it
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // the largest power of two unit that leaves a whole number
        for (shift, unit) in [(40, "TiB"), (30, "GiB"), (20, "MiB"), (10, "KiB")] {
            if self.0 != 0 && self.0.is_multiple_of(1u64 << shift) {
                return write!(f, "{}{unit}", self.0 >> shift);
            }
        }
        write!(f, "{}", self.0)
    }
}

impl Serialize for ByteSize {
    /// Write the size as a string with its unit, as an operator would
    ///
    /// # Arguments
    ///
    /// * `serializer` - Where to write it
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    /// Read a size written as a number or as a string with a unit
    ///
    /// # Arguments
    ///
    /// * `deserializer` - Where to read it from
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        /// Either form a size is written in
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            /// A bare number of bytes
            Number(u64),
            /// A number with a unit
            Text(String),
        }
        match Written::deserialize(deserializer)? {
            Written::Number(bytes) => Ok(ByteSize(bytes)),
            Written::Text(text) => ByteSize::parse(&text).map_err(serde::de::Error::custom),
        }
    }
}

/// How a pool keeps an object's bytes
///
/// Written as a map with one key, `{replicas: 3}` or `{erasure: {data: 4, parity: 2}}`, which every
/// format a configuration is read from spells the same way ([`WrittenRedundancy`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WrittenRedundancy", into = "WrittenRedundancy")]
pub enum Redundancy {
    /// Whole copies, on this many failure domains
    Replicas(u8),
    /// Erasure coded stripes of `data` chunks and `parity` chunks
    Erasure {
        /// The data chunks of a stripe, `k`
        data: u8,
        /// The parity chunks of a stripe, `m`
        parity: u8,
    },
}

/// The erasure half of a redundancy as it is written
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WrittenErasure {
    /// The data chunks of a stripe
    pub data: u8,
    /// The parity chunks of a stripe
    pub parity: u8,
}

/// A redundancy as it is written: exactly one of its two keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WrittenRedundancy {
    /// Whole copies
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u8>,
    /// Erasure coded stripes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub erasure: Option<WrittenErasure>,
}

impl TryFrom<WrittenRedundancy> for Redundancy {
    type Error = String;

    /// Read a redundancy that names exactly one of its two forms
    ///
    /// # Arguments
    ///
    /// * `written` - What was written
    fn try_from(written: WrittenRedundancy) -> Result<Self, Self::Error> {
        match (written.replicas, written.erasure) {
            (Some(copies), None) => Ok(Redundancy::Replicas(copies)),
            (None, Some(erasure)) => Ok(Redundancy::Erasure {
                data: erasure.data,
                parity: erasure.parity,
            }),
            _ => Err(
                "a redundancy names exactly one of {replicas: N} or {erasure: {data: K, parity: M}}"
                    .to_string(),
            ),
        }
    }
}

impl From<Redundancy> for WrittenRedundancy {
    /// Write a redundancy as its one key
    ///
    /// # Arguments
    ///
    /// * `redundancy` - The redundancy
    fn from(redundancy: Redundancy) -> Self {
        match redundancy {
            Redundancy::Replicas(copies) => WrittenRedundancy {
                replicas: Some(copies),
                erasure: None,
            },
            Redundancy::Erasure { data, parity } => WrittenRedundancy {
                replicas: None,
                erasure: Some(WrittenErasure { data, parity }),
            },
        }
    }
}

impl Redundancy {
    /// How many chunks a stripe has, each on its own failure domain
    #[must_use]
    pub const fn width(self) -> u16 {
        match self {
            Redundancy::Replicas(copies) => copies as u16,
            Redundancy::Erasure { data, parity } => data as u16 + parity as u16,
        }
    }

    /// The most losses a stripe survives
    #[must_use]
    pub const fn tolerance(self) -> u8 {
        match self {
            Redundancy::Replicas(copies) => copies.saturating_sub(1),
            Redundancy::Erasure { parity, .. } => parity,
        }
    }

    /// The data chunks and parity chunks of a stripe, as an object's geometry records them
    ///
    /// A replicated stripe is the same protocol with one data chunk and no arithmetic.
    #[must_use]
    pub const fn chunks(self) -> (u8, u8) {
        match self {
            Redundancy::Replicas(copies) => (1, copies.saturating_sub(1)),
            Redundancy::Erasure { data, parity } => (data, parity),
        }
    }
}

impl std::fmt::Display for Redundancy {
    /// Write the redundancy as an operator would
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Redundancy::Replicas(copies) => write!(f, "{copies} replicas"),
            Redundancy::Erasure { data, parity } => write!(f, "{data}+{parity}"),
        }
    }
}

/// What no two chunks of a stripe may share
///
/// Every level but `device` is a member's, spelled as [`crate::shared::domains::LEVELS`] spells
/// it, so placement reads a device's host, rack or zone off its member's record
/// ([F83](../../../docs/src/features/rack-and-zone-domains.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FailureDomain {
    /// A device: two slices of one device are one domain
    Device,
    /// A host: every device of one host is one domain
    #[default]
    Host,
    /// A rack: every host of one rack is one domain; a member naming no rack is its host's
    Rack,
    /// A zone: every rack of one zone is one domain; a member naming no zone is its rack's
    Zone,
}

/// A storage pool: a class of devices, a redundancy and the rules every stripe in it keeps
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSpec {
    /// Which devices it is made of: every device carrying this class, `hdd`, `ssd` or any other
    pub class: String,
    /// How it keeps an object's bytes
    pub redundancy: Redundancy,
    /// What no two chunks of a stripe share
    #[serde(default)]
    pub failure_domain: FailureDomain,
    /// How many more losses an acknowledged write survives, one unless set
    ///
    /// At least one, except in a pool of one copy, which survives none and is what an example and
    /// a test use ([S4](../../../docs/src/object-storage/pools-and-devices.md#the-standalone-node)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub f: Option<u8>,
    /// The bytes of an object each stripe holds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stripe: Option<ByteSize>,
    /// The unit a stripe chunk is checksummed and written in
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<ByteSize>,
    /// The size at or under which an object stays in its row; zero keeps none inline
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<ByteSize>,
}

impl PoolSpec {
    /// A pool of a class and a redundancy, with every other fact at its default
    ///
    /// # Arguments
    ///
    /// * `class` - The class of its devices
    /// * `redundancy` - How it keeps an object's bytes
    #[must_use]
    pub fn new(class: &str, redundancy: Redundancy) -> Self {
        PoolSpec {
            class: class.to_string(),
            redundancy,
            failure_domain: FailureDomain::default(),
            f: None,
            stripe: None,
            unit: None,
            inline: None,
        }
    }

    /// Set the inline threshold
    ///
    /// # Arguments
    ///
    /// * `bytes` - The size at or under which an object stays in its row
    #[must_use]
    pub fn inline(mut self, bytes: u64) -> Self {
        self.inline = Some(ByteSize(bytes));
        self
    }

    /// Set the failure domain
    ///
    /// # Arguments
    ///
    /// * `domain` - What no two chunks of a stripe share
    #[must_use]
    pub fn failure_domain(mut self, domain: FailureDomain) -> Self {
        self.failure_domain = domain;
        self
    }

    /// How many more losses an acknowledged write survives
    #[must_use]
    pub fn losses(&self) -> u8 {
        // one unless set, and none for a pool of one copy
        self.f.unwrap_or(1).min(self.redundancy.tolerance())
    }

    /// The size at or under which an object stays in its row
    #[must_use]
    pub fn inline_threshold(&self) -> u64 {
        self.inline
            .map_or(DEFAULT_INLINE_THRESHOLD as u64, ByteSize::get)
    }

    /// The geometry an object created in this pool is cut by, fixed for its life
    #[must_use]
    pub fn geometry(&self) -> Geometry {
        // the pool's stripe and unit, and its redundancy's chunks
        let (data, parity) = self.redundancy.chunks();
        Geometry {
            stripe_bytes: self.stripe.map_or(DEFAULT_STRIPE_BYTES, ByteSize::get) as u32,
            unit_bytes: self.unit.map_or(DEFAULT_UNIT_BYTES, ByteSize::get) as u32,
            data,
            parity,
        }
    }

    /// Whether this pool's facts can be served, and why not
    ///
    /// # Arguments
    ///
    /// * `name` - The pool's name, for the refusal
    ///
    /// # Errors
    ///
    /// The first fact no pool can have.
    pub fn check(&self, name: &str) -> Result<(), PolicyError> {
        // a refusal names the pool and what about it cannot be
        let refuse = |why: String| {
            Err(PolicyError::BadPool {
                pool: name.to_string(),
                why,
            })
        };
        if self.class.trim().is_empty() {
            return refuse("its class is empty; name the class its devices carry".to_string());
        }
        match self.redundancy {
            Redundancy::Replicas(0) => return refuse("it keeps no replicas".to_string()),
            Redundancy::Erasure { data: 0, .. } => {
                return refuse("its stripes have no data chunks".to_string())
            }
            Redundancy::Erasure { parity: 0, .. } => {
                return refuse(
                    "its stripes have no parity chunks; a stripe of data alone survives no loss"
                        .to_string(),
                )
            }
            _ => (),
        }
        // f is at least one, and at most what the redundancy survives
        let tolerance = self.redundancy.tolerance();
        match self.f {
            Some(0) if tolerance > 0 => {
                return refuse(format!(
                    "f is 0; an acknowledged write in it has to survive a loss, and {} survives {tolerance}",
                    self.redundancy
                ))
            }
            Some(f) if f > tolerance => {
                return refuse(format!(
                    "f is {f}, past the {tolerance} losses {} survives",
                    self.redundancy
                ))
            }
            _ => (),
        }
        // the geometry: a unit that divides a stripe, both within a u32
        let geometry_stripe = self.stripe.map_or(DEFAULT_STRIPE_BYTES, ByteSize::get);
        let geometry_unit = self.unit.map_or(DEFAULT_UNIT_BYTES, ByteSize::get);
        if geometry_unit == 0 || geometry_stripe == 0 {
            return refuse("its stripe or its unit is zero bytes".to_string());
        }
        if geometry_stripe > u64::from(u32::MAX) {
            return refuse(format!(
                "its stripe of {} is past the 4 GiB a stripe can be",
                ByteSize(geometry_stripe)
            ));
        }
        if !geometry_stripe.is_multiple_of(geometry_unit) {
            return refuse(format!(
                "its unit of {} does not divide its stripe of {}",
                ByteSize(geometry_unit),
                ByteSize(geometry_stripe)
            ));
        }
        // and an inline threshold a row can carry
        if self.inline_threshold() > MAX_INLINE_THRESHOLD {
            return refuse(format!(
                "its inline threshold of {} is past the {} a row carries",
                ByteSize(self.inline_threshold()),
                ByteSize(MAX_INLINE_THRESHOLD)
            ));
        }
        Ok(())
    }
}

/// A binding as an operator writes it: the pool a bucket stores its objects in
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingSpec {
    /// The pool, by its name
    pub pool: String,
}

/// A binding once it is policy: its pool and the consumer id it was given
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    /// The pool, by its name
    pub pool: String,
    /// The consumer id minted when the binding was committed, in every stripe's key
    pub consumer: u64,
}

/// What a deployment writes about pools: the pools and each bucket's binding
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolsConf {
    /// Every pool, by name
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub pools: BTreeMap<String, PoolSpec>,
    /// Every bucket's binding, by the bucket's marker's name
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub buckets: BTreeMap<String, BindingSpec>,
}

impl PoolsConf {
    /// Whether nothing is written
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty() && self.buckets.is_empty()
    }

    /// Whether these pools and bindings can serve a schema, and why not
    ///
    /// Every pool's facts are ones a pool can have, every bucket the schema declares is bound,
    /// every binding names a bucket it declares, and every binding a pool that is defined. A
    /// bucket with no binding is refused by name: there is no default pool
    /// ([S4](../../../docs/src/object-storage/pools-and-devices.md#pools-and-bindings-are-policy)).
    ///
    /// # Arguments
    ///
    /// * `served` - What the schema declares
    ///
    /// # Errors
    ///
    /// The first thing that cannot be served.
    pub fn check(&self, served: &Served<'_>) -> Result<(), PolicyError> {
        // every pool on its own first
        for (name, pool) in &self.pools {
            pool.check(name)?;
        }
        // every binding names a bucket the schema declares and a pool that is defined
        for (bucket, binding) in &self.buckets {
            if !served.buckets.contains(&bucket.as_str()) {
                return Err(PolicyError::UnknownBucket {
                    bucket: bucket.clone(),
                    declared: served.buckets.iter().map(ToString::to_string).collect(),
                });
            }
            if !self.pools.contains_key(&binding.pool) {
                return Err(PolicyError::UndefinedPool {
                    bucket: bucket.clone(),
                    pool: binding.pool.clone(),
                    defined: self.pools.keys().cloned().collect(),
                });
            }
        }
        // and every bucket the schema declares is bound
        for bucket in &served.buckets {
            if !self.buckets.contains_key(*bucket) {
                return Err(PolicyError::Unbound {
                    bucket: (*bucket).to_string(),
                    defined: self.pools.keys().cloned().collect(),
                });
            }
        }
        Ok(())
    }
}

/// Pools and bindings as policy: what a node serves objects by
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ObjectPolicy {
    /// Every pool, by name
    #[serde(default)]
    pub pools: BTreeMap<String, PoolSpec>,
    /// Every bucket's binding, by the bucket's name
    #[serde(default)]
    pub bindings: BTreeMap<String, Binding>,
}

impl ObjectPolicy {
    /// Whether no pool and no binding is defined
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty() && self.bindings.is_empty()
    }

    /// Make a deployment's pools and bindings policy, minting a consumer id for each new binding
    ///
    /// A binding to a bucket `minted` already names keeps that id; any other takes one past the
    /// largest id `minted` holds, in the order of the buckets' names. `minted` is every id ever
    /// given - a node's marker or the control group's state - so no id is given twice.
    ///
    /// # Arguments
    ///
    /// * `conf` - What the deployment wrote
    /// * `minted` - Every consumer id given so far, by bucket
    #[must_use]
    pub fn bind(conf: &PoolsConf, minted: &BTreeMap<String, u64>) -> (Self, BTreeMap<String, u64>) {
        // every id given so far stays given
        let mut ids = minted.clone();
        let mut next = ids.values().copied().max().unwrap_or(0) + 1;
        let mut bindings = BTreeMap::new();
        for (bucket, spec) in &conf.buckets {
            // an id this bucket was given before, or the next one
            let consumer = match ids.get(bucket) {
                Some(id) => *id,
                None => {
                    let id = next;
                    next += 1;
                    ids.insert(bucket.clone(), id);
                    id
                }
            };
            bindings.insert(
                bucket.clone(),
                Binding {
                    pool: spec.pool.clone(),
                    consumer,
                },
            );
        }
        let policy = ObjectPolicy {
            pools: conf.pools.clone(),
            bindings,
        };
        (policy, ids)
    }

    /// The pool a bucket is bound to, by the bucket's name
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket's name
    #[must_use]
    pub fn pool_of(&self, bucket: &str) -> Option<(&Binding, &PoolSpec)> {
        // the binding, then the pool it names
        let binding = self.bindings.get(bucket)?;
        let pool = self.pools.get(&binding.pool)?;
        Some((binding, pool))
    }
}

/// Why a deployment's pools and bindings cannot be served
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    /// A pool has a fact no pool can have
    BadPool {
        /// The pool
        pool: String,
        /// What about it cannot be
        why: String,
    },
    /// A bucket the schema declares has no binding
    Unbound {
        /// The bucket
        bucket: String,
        /// The pools that are defined
        defined: Vec<String>,
    },
    /// A binding names a pool nobody defined
    UndefinedPool {
        /// The bucket
        bucket: String,
        /// The pool it names
        pool: String,
        /// The pools that are defined
        defined: Vec<String>,
    },
    /// A binding names a bucket the schema does not declare
    UnknownBucket {
        /// The bucket it names
        bucket: String,
        /// The buckets the schema declares
        declared: Vec<String>,
    },
}

impl std::fmt::Display for PolicyError {
    /// Write the refusal for a person, naming what to fix
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyError::BadPool { pool, why } => write!(f, "storage pool {pool}: {why}"),
            PolicyError::Unbound { bucket, defined } => write!(
                f,
                "bucket {bucket} is bound to no storage pool; there is no default pool, so bind it \
                 under storage.buckets to one of the pools defined, {defined:?}"
            ),
            PolicyError::UndefinedPool {
                bucket,
                pool,
                defined,
            } => write!(
                f,
                "bucket {bucket} is bound to storage pool {pool}, which is not defined; the pools \
                 defined are {defined:?}"
            ),
            PolicyError::UnknownBucket { bucket, declared } => write!(
                f,
                "storage.buckets binds {bucket}, which the schema does not declare; it declares \
                 {declared:?}"
            ),
        }
    }
}

impl std::error::Error for PolicyError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        BindingSpec, ByteSize, FailureDomain, ObjectPolicy, PolicyError, PoolSpec, PoolsConf,
        Redundancy, MAX_INLINE_THRESHOLD,
    };
    use crate::shared::grants::Served;

    /// A schema serving one bucket, Posters
    fn served() -> Served<'static> {
        Served {
            tables: vec!["Movie"],
            generated: vec![
                ("PostersObjectMeta", "Posters"),
                ("PostersStripeMeta", "Posters"),
            ],
            buckets: vec!["Posters"],
        }
    }

    /// Sizes read from every spelling an operator writes, and write back with their unit
    #[test]
    fn sizes_read_with_and_without_units() {
        for (text, bytes) in [
            ("16384", 16_384),
            ("16KiB", 16_384),
            ("16Ki", 16_384),
            ("16K", 16_384),
            ("16kb", 16_384),
            ("4MiB", 4 << 20),
            ("1 GiB", 1 << 30),
        ] {
            assert_eq!(ByteSize::parse(text).unwrap(), ByteSize(bytes), "{text}");
        }
        assert!(ByteSize::parse("MiB").is_err());
        assert!(ByteSize::parse("4 furlongs").is_err());
        assert_eq!(ByteSize(16 << 10).to_string(), "16KiB");
        assert_eq!(ByteSize(1000).to_string(), "1000");
    }

    /// The shape S4 writes is read, and a key nobody defined is refused
    #[test]
    fn the_documented_shape_is_read_and_unknown_keys_refused() {
        let yaml = r"
pools:
  bulk:
    class: hdd
    redundancy: {erasure: {data: 4, parity: 2}}
    failure_domain: host
  fast:
    class: ssd
    redundancy: {replicas: 3}
    failure_domain: device
    inline: 16KiB
buckets:
  Posters: {pool: bulk}
";
        let conf: PoolsConf = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            conf.pools["bulk"].redundancy,
            Redundancy::Erasure { data: 4, parity: 2 }
        );
        assert_eq!(conf.pools["fast"].failure_domain, FailureDomain::Device);
        assert_eq!(conf.pools["fast"].inline_threshold(), 16 << 10);
        // a rack and a zone are levels a pool can name, spelled as a member's levels are (F83)
        for (written, level, domain) in [
            ("rack", crate::shared::domains::RACK, FailureDomain::Rack),
            ("zone", crate::shared::domains::ZONE, FailureDomain::Zone),
            ("host", crate::shared::domains::HOST, FailureDomain::Host),
        ] {
            let pool: PoolSpec = serde_yaml::from_str(&format!(
                "{{class: hdd, redundancy: {{replicas: 3}}, failure_domain: {written}}}"
            ))
            .unwrap();
            assert_eq!(pool.failure_domain, domain);
            assert_eq!(serde_yaml::to_string(&domain).unwrap().trim(), level);
        }
        // and a level no member stands in is refused
        assert!(serde_yaml::from_str::<PoolSpec>(
            "{class: hdd, redundancy: {replicas: 3}, failure_domain: row}"
        )
        .is_err());
        assert_eq!(conf.buckets["Posters"].pool, "bulk");
        assert!(conf.check(&served()).is_ok());
        // a redundancy naming both forms is refused
        let both =
            "pools: {x: {class: ssd, redundancy: {replicas: 3, erasure: {data: 4, parity: 2}}}}";
        assert!(serde_yaml::from_str::<PoolsConf>(both).is_err());
        // and a pool writes back as it was read
        let again: PoolsConf =
            serde_yaml::from_str(&serde_yaml::to_string(&conf).unwrap()).unwrap();
        assert_eq!(again, conf);
        // a misspelled key in a pool is refused, not dropped
        let bad = "pools: {fast: {class: ssd, redundancy: {replicas: 3}, inlne: 4KiB}}";
        assert!(serde_yaml::from_str::<PoolsConf>(bad).is_err());
    }

    /// An unbound bucket, a binding to an undefined pool, and a binding of an undeclared bucket
    /// are each refused by name
    #[test]
    fn unbound_and_undefined_are_refused_by_name() {
        let fast = PoolSpec::new("ssd", Redundancy::Replicas(3));
        let mut conf = PoolsConf::default();
        conf.pools.insert("fast".to_string(), fast);
        // no binding at all
        let error = conf.check(&served()).unwrap_err();
        assert!(matches!(&error, PolicyError::Unbound { bucket, .. } if bucket == "Posters"));
        assert!(error.to_string().contains("Posters"));
        // a binding to a pool nobody defined
        conf.buckets.insert(
            "Posters".to_string(),
            BindingSpec {
                pool: "slow".to_string(),
            },
        );
        let error = conf.check(&served()).unwrap_err();
        assert!(matches!(&error, PolicyError::UndefinedPool { pool, .. } if pool == "slow"));
        // a binding of a bucket the schema does not declare
        conf.buckets.clear();
        conf.buckets.insert(
            "Postres".to_string(),
            BindingSpec {
                pool: "fast".to_string(),
            },
        );
        assert!(matches!(
            conf.check(&served()).unwrap_err(),
            PolicyError::UnknownBucket { .. }
        ));
    }

    /// A pool's facts are held to what a pool can be
    #[test]
    fn a_pools_facts_are_checked() {
        let good = PoolSpec::new("ssd", Redundancy::Replicas(3));
        assert!(good.check("fast").is_ok());
        assert_eq!(good.losses(), 1);
        // one copy survives nothing and is allowed, for examples and tests
        let one = PoolSpec::new("ssd", Redundancy::Replicas(1));
        assert!(one.check("one").is_ok());
        assert_eq!(one.losses(), 0);
        // no parity, f past the redundancy, a threshold past a row, a unit that does not divide
        let mut bad = vec![
            PoolSpec::new("ssd", Redundancy::Erasure { data: 4, parity: 0 }),
            PoolSpec::new("", Redundancy::Replicas(3)),
            good.clone().inline(MAX_INLINE_THRESHOLD + 1),
        ];
        let mut f = good.clone();
        f.f = Some(3);
        bad.push(f);
        let mut unit = good.clone();
        unit.unit = Some(ByteSize(3000));
        bad.push(unit);
        for pool in bad {
            assert!(
                matches!(pool.check("p"), Err(PolicyError::BadPool { .. })),
                "{pool:?}"
            );
        }
    }

    /// A binding keeps its consumer id, and a new one takes one past every id ever given
    #[test]
    fn consumer_ids_are_minted_once_and_never_reused() {
        let mut conf = PoolsConf::default();
        conf.pools.insert(
            "fast".to_string(),
            PoolSpec::new("ssd", Redundancy::Replicas(3)),
        );
        for bucket in ["Posters", "Thumbnails"] {
            conf.buckets.insert(
                bucket.to_string(),
                BindingSpec {
                    pool: "fast".to_string(),
                },
            );
        }
        // the first binding mints in name order
        let (policy, minted) = ObjectPolicy::bind(&conf, &BTreeMap::new());
        assert_eq!(policy.bindings["Posters"].consumer, 1);
        assert_eq!(policy.bindings["Thumbnails"].consumer, 2);
        // a bucket unbound keeps its id given, and a new one never takes it
        conf.buckets.remove("Thumbnails");
        conf.buckets.insert(
            "Avatars".to_string(),
            BindingSpec {
                pool: "fast".to_string(),
            },
        );
        let (policy, minted) = ObjectPolicy::bind(&conf, &minted);
        assert_eq!(policy.bindings["Posters"].consumer, 1);
        assert_eq!(policy.bindings["Avatars"].consumer, 3);
        assert_eq!(minted["Thumbnails"], 2);
        // and the pool's facts are what the bucket is served by
        let (binding, pool) = policy.pool_of("Posters").unwrap();
        assert_eq!(binding.pool, "fast");
        assert_eq!(pool.inline_threshold(), 16 << 10);
        assert!(policy.pool_of("Thumbnails").is_none());
    }
}
