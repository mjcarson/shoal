//! Spike X10: what a stripe row costs ([X10](../../docs/src/object-storage/spikes.md#x10-what-a-stripe-row-costs))
//!
//! The object store keeps its metadata in two tables a bucket generates
//! ([S3](../../docs/src/object-storage/objects.md#the-two-rows)): `ObjectMeta`, an entry an object
//! keyed by its path's hash, and `StripeMeta`, a row for each stripe written in place keyed by its
//! consumer, its object and its index. Neither exists yet. This crate declares tables shaped like
//! them as ordinary persistent unsorted tables, deploys them on the lab's cluster, and measures
//! what today's engine charges for them:
//!
//! - rows a second a tablet group commits, as inserts, overwrites and conditional updates;
//! - bytes a row on disk, in the WAL, in the indexes and in memory, cold and resident;
//! - what a conditional update of a row that is not in memory costs the write and its group;
//! - where an object held inline in its row stops being cheap, swept from 1 KiB to 1 MiB.
//!
//! Two programs are built from it, as `tmdb-dataset`'s are: `x10-node`, the server the spike's
//! inventory names, and `x10`, the driver, which also carries every `shoaladm` command for this
//! schema. Its records are merged by `x10 report` and its tables are on
//! `docs/src/object-storage/stripe-row-costs.md`. Like every spike's code it is thrown away; the
//! rows here are the spike's stand-ins for S3's, and nothing in them is a format.

pub mod cluster;
pub mod drive;
pub mod keys;
pub mod measure;
pub mod record;
pub mod report;
pub mod shape;
pub mod stats;
pub mod table;

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{FileSystem, PersistentUnsortedTable, ShoalUnsortedTable};

/// The geometry an object was created with, fixed for its life
///
/// S3 keeps it in the entry so that an offset maps to a stripe by division.
#[derive(
    Debug, Clone, PartialEq, Archive, Serialize, Deserialize, DeepSizeOf, serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct Geometry {
    /// The stripe's size in bytes
    pub stripe_bytes: u32,
    /// The chunk unit's size in bytes
    pub unit_bytes: u32,
    /// The data chunks a stripe is cut into
    pub data: u8,
    /// The parity chunks computed over them
    pub parity: u8,
}

/// Where an object's identity stands: current, being replaced, and ids not yet reclaimed
#[derive(
    Debug, Clone, PartialEq, Archive, Serialize, Deserialize, DeepSizeOf, serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct ObjectState {
    /// The id of a replacement in flight, if one is
    pub replacing: Option<u128>,
    /// Ids retired and not yet reclaimed
    pub retired: Vec<u128>,
}

/// One object at one path, as S3's `ObjectMeta` describes it
#[derive(
    Debug, Clone, PartialEq, Archive, Serialize, Deserialize, DeepSizeOf, serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct ObjectEntry {
    /// The whole path, compared on every read
    pub path: String,
    /// The current object at the path, minted when it was created and never reused
    pub object: u128,
    /// The object's length in bytes
    pub size: u64,
    /// The geometry it was created with
    pub geometry: Geometry,
    /// The counter every truncate moves
    pub epoch: u64,
    /// The `(length, epoch)` marks truncates left that are not yet reclaimed
    pub floors: Vec<(u64, u64)>,
    /// A replacement in flight and ids retired
    pub state: ObjectState,
    /// When it was created, as a clock said, for people
    pub created: u64,
    /// When it was last changed, as a clock said, for people
    pub modified: u64,
    /// A small bounded map of strings a caller attached
    pub user: Vec<(String, String)>,
    /// The whole object, when it is small enough to be held inline
    pub inline: Vec<u8>,
}

/// The metadata of every object whose path hashes to one key
///
/// S3's row is a short list, nearly always of one entry, so that two paths with one key are two
/// entries and neither refuses the other. `version` is the spike's stand-in for "every change to
/// the row is conditional on what its writer read": a conditional update names the version it
/// read and moves it on.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    ShoalUnsortedTable,
    DeepSizeOf,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Rows")]
pub struct ObjectMeta {
    /// The hash of the path, which the row is partitioned by
    #[shoal(partition)]
    pub key: u64,
    /// How many changes the row has committed, which a change is conditional on
    #[shoal(filter, update)]
    pub version: u64,
    /// The objects at the paths that share this key
    #[shoal(update)]
    pub entries: Vec<ObjectEntry>,
}

/// The label a stripe chunk carries: the row's sequence and a tag from the write's identity
#[derive(
    Debug, Clone, PartialEq, Archive, Serialize, Deserialize, DeepSizeOf, serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct Label {
    /// The sequence of the write that last changed the chunk
    pub sequence: u64,
    /// The tag derived from that write's request identity
    pub tag: u64,
}

/// A stripe written in place, as S3's `StripeMeta` describes it
///
/// Keyed by its consumer's id, its object's id and its index, a composite key of three fields
/// ([Resolved #92, #198](../../docs/src/appendix/resolved/composite-partition-key.md)). A commit
/// is an update conditional on the sequence the write was staged under
/// ([S7](../../docs/src/object-storage/write-path.md#what-breaks-without-the-condition)).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    ShoalUnsortedTable,
    DeepSizeOf,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Rows")]
pub struct StripeMeta {
    /// The consumer the stripe belongs to: its bucket
    #[shoal(partition)]
    pub consumer: u64,
    /// The object the stripe belongs to
    #[shoal(partition)]
    pub object: u128,
    /// The stripe's index in its object
    #[shoal(partition)]
    pub stripe: u64,
    /// How many writes to this stripe have committed, which a commit is conditional on
    #[shoal(filter, update)]
    pub sequence: u64,
    /// The label each stripe chunk must carry, one a chunk
    #[shoal(update)]
    pub labels: Vec<Label>,
    /// How much of the stripe holds bytes
    #[shoal(update)]
    pub length: u64,
    /// The object's truncate epoch when the row was last committed
    #[shoal(update)]
    pub epoch: u64,
    /// The holders that did not stage the last write, a bit a chunk
    #[shoal(update)]
    pub missed: u32,
}

/// A stripe row that also keeps an eight byte digest of each chunk
///
/// X5 left open whether the row keeps a digest of each stripe chunk
/// ([checksums](../../docs/src/object-storage/checksums.md#what-x5-does-not-settle)); this is
/// `StripeMeta` with one, so the two can be weighed side by side.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    ShoalUnsortedTable,
    DeepSizeOf,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Rows")]
pub struct StripeMetaDigest {
    /// The consumer the stripe belongs to: its bucket
    #[shoal(partition)]
    pub consumer: u64,
    /// The object the stripe belongs to
    #[shoal(partition)]
    pub object: u128,
    /// The stripe's index in its object
    #[shoal(partition)]
    pub stripe: u64,
    /// How many writes to this stripe have committed, which a commit is conditional on
    #[shoal(filter, update)]
    pub sequence: u64,
    /// The label each stripe chunk must carry, one a chunk
    #[shoal(update)]
    pub labels: Vec<Label>,
    /// How much of the stripe holds bytes
    #[shoal(update)]
    pub length: u64,
    /// The object's truncate epoch when the row was last committed
    #[shoal(update)]
    pub epoch: u64,
    /// The holders that did not stage the last write, a bit a chunk
    #[shoal(update)]
    pub missed: u32,
    /// A CRC-64/NVME digest of each stripe chunk, one a chunk
    #[shoal(update)]
    pub digests: Vec<u64>,
}

/// Rows written only to fill WAL segments, so the measured tables' segments seal and compact
///
/// A group's checkpoint moves only past a sealed segment its table has merged, and a restart
/// applies everything after it again, resident. Writing these after a load seals the segments
/// the load is in without touching the measured tables' groups.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    ShoalUnsortedTable,
    DeepSizeOf,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Rows")]
pub struct Filler {
    /// The filler row's key
    #[shoal(partition)]
    pub key: u64,
    /// Bytes nobody reads
    pub bytes: Vec<u8>,
}

/// The database
///
/// `#[shoal::db]` reads this struct and generates the client type, the query enum and the
/// response enum the driver sends with, and the schema the node serves.
#[shoal::db]
pub struct Rows {
    /// Objects, an entry each, keyed by their path's hash
    pub object_meta: PersistentUnsortedTable<ObjectMeta, FileSystem>,
    /// Stripes written in place, a row each
    pub stripe_meta: PersistentUnsortedTable<StripeMeta, FileSystem>,
    /// The same rows with a digest of each chunk
    pub stripe_meta_digest: PersistentUnsortedTable<StripeMetaDigest, FileSystem>,
    /// Rows that only fill WAL segments
    pub filler: PersistentUnsortedTable<Filler, FileSystem>,
}
