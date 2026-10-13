//! The schema the fixture's servers serve
//!
//! An ephemeral table and a persistent one, and since F68 an ephemeral sorted one: the fixture is mostly about processes, ports and
//! faults, and an ephemeral table starts without a storage directory to claim; the persistent
//! one is what a restart has to find again, and what gives the schema two distinct table ids
//! ([F39](../../../docs/src/features/membership.md)). `TestDb` is the name every table in the
//! workspace's tests uses for its schema.
//!
//! Since [F86](../../../docs/src/features/replicated-pools.md) a second schema, `ObjectDb`, adds a
//! bucket beside a persistent table. A child serves it when its cluster's pools bind
//! [`OBJECT_BUCKET`], so every test before M15 keeps the tables, ids and groups it was written
//! against, and a bucket's generated tables appear only where a test asked for objects.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::storage::FileSystem;
use shoal::tables::{Bucket, EphemeralSortedTable, EphemeralUnsortedTable, PersistentUnsortedTable};
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

/// The bucket `ObjectDb` declares, which a cluster's pools bind to serve that schema
pub const OBJECT_BUCKET: &str = "Reels";

/// A document beside a bucket's objects, so a test can watch a table answer while objects move
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "ObjectDb")]
pub struct Doc {
    /// The partition key
    #[shoal(partition)]
    pub key: u64,
    /// What the document says
    #[shoal(update)]
    pub text: String,
}

/// Names the bucket the object tests write
pub struct Reels;

/// The schema of the object tests: a persistent table and a bucket
/// ([F86](../../../docs/src/features/replicated-pools.md))
#[db]
pub struct ObjectDb {
    /// The persistent table, which answers while objects move
    pub docs: PersistentUnsortedTable<Doc, FileSystem>,
    /// The bucket, bound to a pool by every cluster that serves this schema
    pub reels: Bucket<Reels>,
}
