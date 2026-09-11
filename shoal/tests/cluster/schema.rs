//! The schema the fixture's servers serve
//!
//! One ephemeral table: the fixture is about processes, ports and faults, not storage, and an
//! ephemeral table starts without a storage directory to claim. `TestDb` is the name every
//! table in the workspace's tests uses for its schema.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::shared::traits::RkyvSupport;
use shoal::tables::EphemeralUnsortedTable;
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
#[db]
pub struct TestDb {
    /// The only table
    pub rows: EphemeralUnsortedTable<Row>,
}
