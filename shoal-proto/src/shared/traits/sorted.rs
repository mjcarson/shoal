//! The traits for a sorted table where each partition contains many rows in a sorted order

use deepsize2::DeepSizeOf;

use super::{PartitionKeySupport, RkyvSupport, ShoalTableSupport, SortKey};
use crate::shared::queries::SortedUpdate;

pub trait ShoalSortedTable:
    std::fmt::Debug + Clone + ShoalTableSupport + RkyvSupport + PartitionKeySupport + Sized + DeepSizeOf
{
    ///// The user facing updates that can be applied to this table
    //type Update: RkyvSupport + std::fmt::Debug + Clone;

    /////// The server facing updates that can be applied to this table (just the updates no keys)
    ////type UpdateData: RkyvSupport + std::fmt::Debug + Clone;

    /// The sort type for this data
    ///
    /// Any [`SortKey`]: an integer, a `String`, a `Uuid`, an `Option` of a key, a tuple of keys
    /// (which several `#[shoal(sort)]` fields make), or a schema's own type that opts in. Until
    /// [Resolved #207](../../../../docs/src/appendix/resolved/sort-keys-of-any-type.md) this was
    /// bound by `RkyvSupport`, which only `String` implemented, and by `DeepSizeOf`, which no
    /// `Uuid` does; a partition now asks the key for its heap through [`SortKey::heap_bytes`].
    type Sort: SortKey + From<Self::Sort>;

    /// Build the sort tuple for this row
    fn get_sort(&self) -> Self::Sort;

    ///// Any filters to apply when listing/crawling rows
    //type Filters: rkyv::Archive + std::fmt::Debug + Clone;

    ///// Determine if a row should be filtered
    /////
    ///// # Arguments
    /////
    ///// * `filters` - The filters to apply
    ///// * `row` - The row to filter
    //fn is_filtered(filter: &Self::Filters, row: &Self) -> bool;

    ///// Determine if a row should be filtered
    /////
    ///// # Arguments
    /////
    ///// * `filters` - The filters to apply
    ///// * `row` - The row to filter
    //fn is_filtered_archived(
    //    filter: &Self::Filters,
    //    row: &<Self as rkyv::Archive>::Archived,
    //) -> bool;

    /// Apply an update to a single row
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply to a specific row
    fn update(&mut self, update: &SortedUpdate<Self>);
}
