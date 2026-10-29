//! Spike X8: one small write, three ways ([X8](../../docs/src/object-storage/spikes.md#x8-one-small-write-three-ways))
//!
//! The object store writes part of a stripe in place by S7's candidate B: holders stage the new
//! bytes, one conditional commit of the stripe's row decides, and the holders apply after it
//! ([S7](../../docs/src/object-storage/write-path.md)). That is two durable rounds where a table
//! write pays one, so S7 keeps open a second way for small writes: the bytes ride inside the commit,
//! durable when the group's log is, and are folded into the stripe chunks afterwards. Q27 asks
//! whether that is worth having and below what size. This crate drives one small write, 4 KiB to
//! 256 KiB, three ways on today's engine and a holder process of its own:
//!
//! - **row**: an overwrite of a row holding exactly the write's bytes, the cheapest write through
//!   the log, with no read and no condition;
//! - **staged**: B. A `Quorum` read of the stripe's row through its leader, the bytes staged to a
//!   holder on each host, which journals them as X6 found best, a conditional commit of the small
//!   row once two of three have them, and an apply in place after the acknowledgement;
//! - **inline**: the same read, then the commit with the bytes inside the row, and a fold of them
//!   into each host's chunk after the acknowledgement.
//!
//! Four programs' worth of code are built from it: `x8-node`, the server the spike's inventories
//! name; `x8-holder`, a slice's journal and chunks on one glommio executor; and `x8`, the driver,
//! which also starts the holders and europa's local nodes and carries every `shoaladm` command for
//! this schema. Its records are merged by `x8 report` and its tables are on
//! `docs/src/object-storage/small-writes.md`. Like every spike's code it is thrown away; the rows
//! here are stand-ins for S3's, and nothing in them is a format.

pub mod bytes;
pub mod cluster;
pub mod deferred;
pub mod drive;
pub mod holder;
pub mod holders;
pub mod keys;
pub mod lane;
pub mod local;
pub mod measure;
pub mod paths;
pub mod record;
pub mod report;
pub mod shape;
pub mod stats;
pub mod table;
pub mod wire;

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{FileSystem, PersistentUnsortedTable, ShoalProjection, ShoalUnsortedTable};

/// A write's bytes as a row of their own, the first path
///
/// The key stands in for a stripe's identity and the bytes for exactly what one write carries; a
/// write is an insert of the whole row, which never reads what was there.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalUnsortedTable, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Small")]
pub struct StripeRow {
    /// The row's key, which it is partitioned by
    #[shoal(partition)]
    pub key: u64,
    /// The write's bytes, made from the key and the write that made them
    pub bytes: Vec<u8>,
}

/// The label a stripe chunk carries: the row's sequence and a tag from the write's identity
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize, DeepSizeOf, serde::Deserialize)]
#[rkyv(derive(Debug))]
pub struct Label {
    /// The sequence of the write that last changed the chunk
    pub sequence: u64,
    /// The tag derived from that write's request identity and its try
    pub tag: u64,
}

/// A stripe written in place, as S3's `StripeMeta` describes it, with room for a small write
///
/// X10's row (`shoal-spike-rows/src/lib.rs`) with one label a copy of a replicated pool of three,
/// and `pending`: the bytes of a write that rode inside its commit, held until they are folded
/// ([S7](../../docs/src/object-storage/write-path.md#small-writes)). A commit is an update
/// conditional on the sequence the write read
/// ([S7](../../docs/src/object-storage/write-path.md#what-breaks-without-the-condition)).
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalUnsortedTable, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Small")]
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
    /// The label each copy's chunk must carry
    #[shoal(update)]
    pub labels: Vec<Label>,
    /// How much of the stripe holds bytes
    #[shoal(update)]
    pub length: u64,
    /// The object's truncate epoch when the row was last committed
    #[shoal(update)]
    pub epoch: u64,
    /// The holders that did not stage the last write, a bit a copy
    #[shoal(update)]
    pub missed: u32,
    /// The bytes of the last write that rode inside its commit, empty when none did
    #[shoal(update)]
    pub pending: Vec<u8>,
}

/// A stripe row without the bytes a commit may hold, which is what a writer reads first
///
/// A stager reads the row's sequence, labels and missed holders; the bytes a previous small write
/// left in it are the readers' to overlay and not the writer's to fetch, so the read before a
/// commit never carries them.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalProjection)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "StripeMeta")]
pub struct StripeHead {
    /// The consumer the stripe belongs to
    #[shoal(partition)]
    pub consumer: u64,
    /// The object the stripe belongs to
    #[shoal(partition)]
    pub object: u128,
    /// The stripe's index in its object
    #[shoal(partition)]
    pub stripe: u64,
    /// How many writes to this stripe have committed
    pub sequence: u64,
    /// The label each copy's chunk must carry
    pub labels: Vec<Label>,
    /// How much of the stripe holds bytes
    pub length: u64,
    /// The object's truncate epoch when the row was last committed
    pub epoch: u64,
    /// The holders that did not stage the last write
    pub missed: u32,
}

/// Rows written only to fill WAL segments, so a cell's merges are not charged to the next
///
/// A segment is handed to a compactor once it is sealed, so the tail a cell leaves in it would be
/// merged in whatever cell fills it next. Writing these before a cell seals that tail first.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalUnsortedTable, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Small")]
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
pub struct Small {
    /// The first path's rows: a write's bytes each
    pub stripe_row: PersistentUnsortedTable<StripeRow, FileSystem>,
    /// The second and third paths' rows: a stripe each, read without its pending bytes
    #[shoal(projections(StripeHead))]
    pub stripe_meta: PersistentUnsortedTable<StripeMeta, FileSystem>,
    /// Rows that only fill WAL segments
    pub filler: PersistentUnsortedTable<Filler, FileSystem>,
}
