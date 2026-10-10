//! The schema the fixture's servers serve
//!
//! An ephemeral table and a persistent one, and since F68 an ephemeral sorted one: the fixture is mostly about processes, ports and
//! faults, and an ephemeral table starts without a storage directory to claim; the persistent
//! one is what a restart has to find again, and what gives the schema two distinct table ids
//! ([F39](../../../docs/src/features/membership.md)). `TestDb` is the name every table in the
//! workspace's tests uses for its schema.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::storage::FileSystem;
use shoal::tables::{EphemeralSortedTable, EphemeralUnsortedTable, PersistentUnsortedTable};
use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};

/// A row
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct Row {
    /// The partition key
    #[shoal(partition)]
    pub key: u64,
    /// The payload
    #[shoal(update)]
    pub data: String,
}

/// The schema
/// A persistent row, so a restart has something to find again
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct Note {
    /// The partition key
    #[shoal(partition)]
    pub key: u64,
    /// What the note says, which a conditional write can expect
    /// ([F68](../../../docs/src/features/conditional-writes.md))
    #[shoal(filter, update)]
    pub text: String,
}

/// An entry in a bucket, many to a partition, so a conditional write to a sorted table has a
/// table to be judged in ([F68](../../../docs/src/features/conditional-writes.md))
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct Entry {
    /// The bucket this entry is in
    #[shoal(partition)]
    pub bucket: u64,
    /// The entry's name within its bucket
    #[shoal(sort)]
    pub name: String,
    /// The version a conditional write expects to find
    #[shoal(filter, update)]
    pub version: u64,
}

#[db]
pub struct TestDb {
    /// The ephemeral table, which every test before F39 wrote to
    pub rows: EphemeralUnsortedTable<Row>,
    /// The persistent table, which survives a restart
    pub notes: PersistentUnsortedTable<Note, FileSystem>,
    /// An ephemeral sorted table, appended so the two above keep their table ids
    pub entries: EphemeralSortedTable<Entry>,
}
