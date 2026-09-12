//! The schema the fixture's servers serve
//!
//! An ephemeral table and a persistent one: the fixture is mostly about processes, ports and
//! faults, and an ephemeral table starts without a storage directory to claim; the persistent
//! one is what a restart has to find again, and what gives the schema two distinct table ids
//! ([F39](../../../docs/src/features/membership.md)). `TestDb` is the name every table in the
//! workspace's tests uses for its schema.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::storage::FileSystem;
use shoal::tables::{EphemeralUnsortedTable, PersistentUnsortedTable};
use shoal_derive::{db, ShoalUnsortedTable};

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
    /// What the note says
    #[shoal(update)]
    pub text: String,
}

#[db]
pub struct TestDb {
    /// The ephemeral table, which every test before F39 wrote to
    pub rows: EphemeralUnsortedTable<Row>,
    /// The persistent table, which survives a restart
    pub notes: PersistentUnsortedTable<Note, FileSystem>,
}
