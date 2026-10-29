//! Spike X3: bytes through the tablet groups ([X3](../../docs/src/object-storage/spikes.md#x3-bytes-through-the-tablet-groups))
//!
//! Candidate A of the object store's write path keeps a stripe's bytes as a row of a generated
//! table, replicated by the tablet group that owns it ([S7](../../docs/src/object-storage/write-path.md#the-candidates)).
//! Nothing of the object store exists yet, so this crate declares that row as an ordinary
//! persistent unsorted table, a key and a byte vector, and measures what today's engine charges
//! for it at 64 KiB to 4 MiB:
//!
//! - the bytes a second it acknowledges, and their tail, for puts, gets, an even mixture and
//!   overwrites;
//! - the device bytes it writes for each byte stored, the WAL's and the archives' apart;
//! - the memory a node holds while it does;
//! - the tail of a small table beside it, driven lightly throughout.
//!
//! Two programs are built from it, as `tmdb-dataset`'s are: `x3-node`, the server the spike's
//! inventories name, and `x3`, the driver, which also starts europa's three local nodes and
//! carries every `shoaladm` command for this schema. Every arm runs through shoal-loadgen's own
//! driver, handed the kinds in [`kinds`]. Its records are merged by `x3 report` and its tables are
//! on `docs/src/object-storage/bytes-through-groups.md`. Like every spike's code it is thrown
//! away; nothing in its rows is a format.

pub mod bytes;
pub mod ceiling;
pub mod cluster;
pub mod kinds;
pub mod local;
pub mod measure;
pub mod record;
pub mod report;
pub mod stats;
pub mod table;

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{FileSystem, PersistentUnsortedTable, ShoalUnsortedTable};

/// A stripe's bytes as a row, which is candidate A of the write path
///
/// The key stands in for a stripe's identity and the bytes for its whole contents; a write of
/// the stripe is an insert of the whole row, as A has no way to patch part of one.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalUnsortedTable, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "StripesAsRows")]
pub struct StripeRow {
    /// The stripe's key, which the row is partitioned by
    #[shoal(partition)]
    pub key: u64,
    /// The stripe's bytes, made from the key and the write that made them
    pub bytes: Vec<u8>,
}

/// A small row of the table driven lightly beside the stripes, whose tail is the neighbour's
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalUnsortedTable, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "StripesAsRows")]
pub struct SmallRow {
    /// The row's key, which it is partitioned by
    #[shoal(partition)]
    pub key: u64,
    /// A value a write moves
    pub value: u64,
    /// A few dozen bytes of text, so the row is the size of a typical table's
    pub note: String,
}

/// The database
///
/// `#[shoal::db]` reads this struct and generates the client type, the query enum and the
/// response enum the driver sends with, and the schema the node serves.
#[shoal::db]
pub struct StripesAsRows {
    /// Stripes, a row each
    pub stripe_row: PersistentUnsortedTable<StripeRow, FileSystem>,
    /// The small table beside them
    pub small_row: PersistentUnsortedTable<SmallRow, FileSystem>,
}
