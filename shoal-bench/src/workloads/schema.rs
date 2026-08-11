//! The tables the workloads drive
//!
//! One schema, shared by every workload, rather than one per workload. Two reasons, and the second
//! is the one that matters:
//!
//! - A `#[shoal::db]` struct mints a client type, a query enum and a response enum, so a schema per
//!   workload is a set of parallel type universes that no shared harness code can be written
//!   against.
//! - Holding the row shape fixed is what makes two workloads comparable to each other. If the
//!   sorted and unsorted insert workloads had different rows, the difference between them would be
//!   the row as much as the table, and the pair would stop being a control for one another.
//!
//! # What the rows are shaped like
//!
//! Deliberately narrow, and deliberately parameterized on one field. The `tmdb` example's row had
//! twenty four fields because that is what the dataset had, which meant every measurement over it
//! also measured the cost of a wide row without ever isolating it. These rows carry the keys they
//! need plus one payload string whose width the scale sets, so row width is a knob rather than an
//! accident.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{
    FileSystem, PersistentSortedTable, PersistentUnsortedTable, ShoalProjection, ShoalSortedTable,
    ShoalUnsortedTable,
};

/// A row in the unsorted table, one per partition
///
/// The write path's subject. One partition key, one filterable field so the filter path is
/// reachable, one updatable field so the update path is, and a payload whose width is set by the
/// scale.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Bench")]
pub struct Item {
    /// The key this row is partitioned by
    #[shoal(partition)]
    pub id: u64,
    /// A field a get can filter on
    #[shoal(filter)]
    pub bucket: u64,
    /// A field an update can change
    #[shoal(update)]
    pub label: String,
    /// The payload, whose width the scale sets
    pub payload: String,
}

/// A row in the sorted table, many per partition
///
/// The read path's subject. A sorted table is what makes a partition hold more than one row, which
/// is what a range scan, a sort key selection and a fanout curve all need.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Bench")]
pub struct Event {
    /// The key this row is partitioned by
    #[shoal(partition)]
    pub stream: u64,
    /// The key rows are sorted by within a partition
    ///
    /// A zero padded decimal rather than an integer, and not by choice: `RkyvSupport` is
    /// implemented for `String` and for nothing else, so a sorted table cannot currently have an
    /// integer sort key at all. Zero padding to a fixed width is what makes lexicographic order
    /// agree with numeric order, which a range scan over this key depends on. Filed in
    /// `docs/src/appendix/todos.md`.
    #[shoal(sort)]
    pub at: String,
    /// A field a get can filter on
    #[shoal(filter)]
    pub kind: u64,
    /// The payload, whose width the scale sets
    pub payload: String,
}

/// A projection over [`Item`] holding only its keys
///
/// Exists so the projection path is reachable from a workload. Reading this instead of a whole
/// `Item` is the comparison F2 shipped without a benchmark for.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalProjection, PartialEq, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "Item")]
pub struct ItemKeys {
    /// The key this row was partitioned by
    #[shoal(partition)]
    pub id: u64,
    /// The bucket this row is in
    pub bucket: u64,
}

/// The database every workload drives
#[shoal::db]
pub struct Bench {
    /// One row per partition, which is the write path's subject
    #[shoal(projections(ItemKeys))]
    pub item: PersistentUnsortedTable<Item, FileSystem>,
    /// Many rows per partition, which is the read path's subject
    pub event: PersistentSortedTable<Event, FileSystem>,
}

/// How many characters an [`Event`] sort key is padded to
///
/// Fixed, because lexicographic order only agrees with numeric order when every key is the same
/// width. `u64::MAX` is twenty digits, so this covers every value one can hold.
pub const SORT_KEY_WIDTH: usize = 20;

/// Builds an [`Event`] sort key that sorts in numeric order
///
/// # Arguments
///
/// * `at` - The position within the partition to build a key for
///
/// # Examples
///
/// ```
/// use shoal_bench::workloads::schema::sort_key;
///
/// // padded, so a range over these is a range over the numbers they stand for
/// assert!(sort_key(2) < sort_key(10));
/// ```
pub fn sort_key(at: u64) -> String {
    format!("{at:0width$}", width = SORT_KEY_WIDTH)
}

#[cfg(test)]
mod tests {
    use super::{sort_key, SORT_KEY_WIDTH};

    /// Sort keys are all one width, which is what makes them comparable as strings
    #[test]
    fn every_sort_key_is_the_same_width() {
        for at in [0u64, 1, 999, u64::MAX] {
            assert_eq!(sort_key(at).len(), SORT_KEY_WIDTH);
        }
    }

    /// Lexicographic order over sort keys is numeric order
    ///
    /// This is what a range scan over them depends on. Without the padding, `"10"` sorts before
    /// `"2"` and a bounded range silently returns the wrong rows.
    #[test]
    fn string_order_matches_numeric_order() {
        let mut keys: Vec<String> = [100u64, 2, 30, 4, 5_000].iter().map(|at| sort_key(*at)).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![sort_key(2), sort_key(4), sort_key(30), sort_key(100), sort_key(5_000)]
        );
    }
}
