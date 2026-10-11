//! Spike X10: what a stripe row costs ([X10](../../docs/src/object-storage/spikes.md#x10-what-a-stripe-row-costs))
//!
//! The object store keeps its metadata in two tables a bucket generates
//! ([S3](../../docs/src/object-storage/objects.md#the-two-rows)): `ObjectMeta`, an entry an object
//! keyed by its path's hash, and `StripeMeta`, a row for each stripe written in place keyed by its
//! consumer, its object and its index. Neither exists yet. This crate declares tables shaped like
//! them as ordinary persistent unsorted tables, deploys them on the lab's cluster, and measures
//! what today's engine charges for them. Since M12 they exist, and the spike declares a bucket and
//! measures the rows it generates instead, to take X10's figures again on the rows as built
//! ([F79](../../docs/src/features/bucket-schema.md#performance)):
//!
//! - rows a second a tablet group commits, as inserts, overwrites and conditional updates;
//! - bytes a row on disk, in the WAL, in the indexes and in memory, cold and resident;
//! - what a conditional update of a row that is not in memory costs the write and its group;
//! - where an object held inline in its row stops being cheap, swept from 1 KiB to 1 MiB.
//!
//! Two programs are built from it, as `tmdb-dataset`'s are: `x10-node`, the server the spike's
//! inventory names, and `x10`, the driver, which also carries every `shoaladm` command for this
//! schema. Its records are merged by `x10 report` and its tables are on
//! `docs/src/object-storage/stripe-row-costs.md`. Like every spike's code it is thrown away. The
//! stand-ins it first measured, and the stripe row with a digest a chunk X1 left out, are in its
//! history at the commit that recorded X10.

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
use shoal::{Bucket, FileSystem, PersistentUnsortedTable, ShoalUnsortedTable};

// the types the generated rows hold, defined once in the protocol crate since F79
pub use shoal::shared::objects::{Attributes, Geometry, Label, ObjectEntry, ObjectState};

/// Names the bucket whose generated rows this spike drives
///
/// Since M12 ([F79](../../docs/src/features/bucket-schema.md)) the rows X10 priced are generated:
/// `SpikeObjectMeta` and `SpikeStripeMeta`, the two tables `Bucket<Spike>` brings. A client's
/// query of them is refused at the front door, so the node this crate builds is built with
/// `--features generated`, which opens them to the spike's driver and to nothing else.
pub struct Spike;

/// The object rows, by the name the measurements spell them
pub type ObjectMeta = SpikeObjectMeta;
/// A get of object rows
pub type ObjectMetaGet = SpikeObjectMetaGet;
/// A change to an object row
pub type ObjectMetaUpdate = SpikeObjectMetaUpdate;
/// The filter an object row's change is conditional on
pub type ObjectMetaFilter = SpikeObjectMetaFilter;
/// The stripe rows, by the name the measurements spell them
pub type StripeMeta = SpikeStripeMeta;
/// A get of stripe rows
pub type StripeMetaGet = SpikeStripeMetaGet;
/// A change to a stripe row
pub type StripeMetaUpdate = SpikeStripeMetaUpdate;
/// The filter a stripe row's commit is conditional on
pub type StripeMetaFilter = SpikeStripeMetaFilter;

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
    /// The bucket whose generated rows are measured: objects, an entry each, keyed by their
    /// path's hash, and stripes written in place, a row each
    pub spike: Bucket<Spike>,
    /// Rows that only fill WAL segments
    pub filler: PersistentUnsortedTable<Filler, FileSystem>,
}
