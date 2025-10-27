//! The traits for a basic unsorted table where each partition contains a
//! single row

use deepsize2::DeepSizeOf;

use super::{PartitionKeySupport, RkyvSupport};
use crate::shared::queries::UnsortedUpdate;

pub trait ShoalUnsortedTable:
    std::fmt::Debug + Clone + RkyvSupport + PartitionKeySupport + Sized + DeepSizeOf
{
    /// The updates that can be applied to this table
    type Update: RkyvSupport + std::fmt::Debug + Clone;

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
    fn update(&mut self, update: &UnsortedUpdate<Self>);
}
