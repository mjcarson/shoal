//! The traits for a sorted table where each partition contains many rows in a sorted order

use deepsize2::DeepSizeOf;

use super::{PartitionKeySupport, RkyvSupport};
use crate::shared::queries::SortedUpdate;

pub trait ShoalSortedTable:
    std::fmt::Debug + Clone + RkyvSupport + PartitionKeySupport + Sized + DeepSizeOf
{
    /// The updates that can be applied to this table
    type Update: RkyvSupport + std::fmt::Debug + Clone;

    /// The sort type for this data
    type Sort: Ord + RkyvSupport + std::fmt::Debug + From<Self::Sort> + Clone + DeepSizeOf;

    /// Build the sort tuple for this row
    fn get_sort(&self) -> &Self::Sort;

    /// Any filters to apply when listing/crawling rows
    type Filters: rkyv::Archive + std::fmt::Debug + Clone;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered(filter: &Self::Filters, row: &Self) -> bool;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered_archived(
        filter: &Self::Filters,
        row: &<Self as rkyv::Archive>::Archived,
    ) -> bool;

    /// Apply an update to a single row
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply to a specific row
    fn update(&mut self, update: &SortedUpdate<Self>);
}
