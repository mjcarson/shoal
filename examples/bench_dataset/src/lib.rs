//! A small catalog schema with a committed dataset, for testing `shoaladm bench` end to end ([F66](../../../docs/src/features/dataset-benchmarks.md))
//!
//! `Item` and `Review` opt in to being loaded from a dataset, so `dataset/Item.csv` and
//! `dataset/Review.jsonl` load them. `Audit` does not, and `dataset-bad/` names it, so the
//! refusal by name is exercised too. Nothing here is benchmark code: the benchmark of this
//! schema is the generic one, built from what the table derives emit.
//!
//! Since [F80](../../../docs/src/features/object-wire.md) the catalog keeps item images in a
//! bucket, `Images`, which `dataset-objects/Images.yaml` describes, so a bucket's object arms are
//! run the same generic way. A node of this schema binds the bucket to a pool, which
//! [`images_pool`] is for a test's.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::shared::pools::{PoolSpec, Redundancy};
use shoal::tables::Bucket;
use shoal::{
    EphemeralUnsortedTable, FileSystem, PersistentSortedTable, PersistentUnsortedTable,
    ShoalSortedTable, ShoalUnsortedTable,
};

/// An item in the catalog, keyed by its id
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
    /// What it is called
    #[shoal(filter)]
    pub name: String,
    /// What it costs, in cents
    #[shoal(update)]
    pub price: u64,
    /// A description, so a row has some width
    pub description: String,
}

/// A review of an item, sorted by who wrote it
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
    /// Who wrote the review
    #[shoal(sort)]
    pub author: String,
    /// The score, out of five
    #[shoal(filter)]
    pub stars: u64,
    /// What they said
    pub body: String,
}

/// An audit entry, which is never loaded from a dataset
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Catalog")]
pub struct Audit {
    /// The entry's id
    #[shoal(partition)]
    pub id: u64,
    /// What happened
    pub event: String,
}

/// The catalog
#[shoal::db]
pub struct Catalog {
    /// Items, by id
    pub items: PersistentUnsortedTable<Item, FileSystem>,
    /// Reviews of each item
    pub reviews: PersistentSortedTable<Review, FileSystem>,
    /// An audit log
    pub audit: EphemeralUnsortedTable<Audit>,
    /// Images of the items, as objects
    pub images: Bucket<Images>,
}

/// Names the bucket the catalog keeps item images in
pub struct Images;

/// The pool a test's node binds `Images` to: one copy, on the node's own storage, holding objects
/// up to 64 KiB in their rows
#[must_use]
pub fn images_pool() -> PoolSpec {
    PoolSpec::new("ssd", Redundancy::Replicas(1)).inline(64 << 10)
}
