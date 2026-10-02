//! A client half schema the unit tests drive, with a table of each kind and one that did not opt in

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{ShoalSortedTable, ShoalUnsortedTable};

/// An unsorted row keyed by a number, opted in
#[derive(
    Debug,
    Archive,
    Serialize,
    Deserialize,
    serde::Deserialize,
    Clone,
    ShoalUnsortedTable,
    PartialEq,
    DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Catalog", dataset)]
pub struct Item {
    /// The item's id
    #[shoal(partition)]
    pub id: u64,
    /// The item's name
    #[shoal(filter)]
    pub name: String,
}

/// A sorted row keyed by two strings, opted in
#[derive(
    Debug,
    Archive,
    Serialize,
    Deserialize,
    serde::Deserialize,
    Clone,
    ShoalSortedTable,
    PartialEq,
    DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Catalog", dataset)]
pub struct Review {
    /// The item reviewed
    #[shoal(partition)]
    pub item: String,
    /// Who reviewed it
    #[shoal(sort)]
    pub author: String,
    /// What they said
    pub body: String,
}

/// A row that did not opt in
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Catalog")]
pub struct Audit {
    /// The entry's id
    #[shoal(partition)]
    pub id: u64,
}

/// The schema, client half only, so the tests link no engine
#[shoal::db(client)]
pub struct Catalog {
    /// Items
    pub items: EphemeralUnsortedTable<Item>,
    /// Reviews of items
    pub reviews: EphemeralSortedTable<Review>,
    /// An audit log no benchmark may load
    pub audit: EphemeralUnsortedTable<Audit>,
}
