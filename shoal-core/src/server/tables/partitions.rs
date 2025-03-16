//! A partition is a collection of data in shoal accesible by a partition key

use std::collections::BTreeMap;

use rkyv::{Archive, Deserialize, Serialize};

use crate::shared::queries::{SortedGet, SortedUpdate, UnsortedGet, UnsortedUpdate};
use crate::shared::responses::ResponseAction;
use crate::shared::traits::{RkyvSupport, ShoalSortedTable, ShoalUnsortedTable};

#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct UnsortedPartition<R: ShoalUnsortedTable> {
    /// This partitions key
    pub key: u64,
    /// The data in this partition
    pub row: R,
    /// The size of this partition
    pub size: usize,
}

impl<R: ShoalUnsortedTable> UnsortedPartition<R> {
    /// Create a new partition
    ///
    /// # Arguments
    ///
    /// * `key` - The key to this partition
    pub fn new(key: u64, row: R) -> Self {
        // get the size of this row
        let size = std::mem::size_of_val(&row);
        UnsortedPartition { key, row, size }
    }

    /// Get some rows from this partition
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters to use to get the rows
    /// * `found` - The vector to push the data to return
    pub fn get(&self, params: &UnsortedGet<R>, found: &mut Vec<R>) {
        // skip any rows that don't match our filter
        if let Some(filter) = &params.filters {
            // check if this row should be filtered out
            if !R::is_filtered(filter, &self.row) {
                // skip this row since it doesn't match our filteri
                return;
            }
        }
        // add this row to our response
        found.push(self.row.clone());
    }

    /// Update a row in this partition
    pub fn update(&mut self, update: &UnsortedUpdate<R>) {
        // update this rows data
        self.row.update(&update);
        // update the size of our row
        self.size = std::mem::size_of_val(&self.row);
    }
}

impl<T: ShoalUnsortedTable> RkyvSupport for UnsortedPartition<T> {}

#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct SortedPartition<T: ShoalSortedTable> {
    /// This partitions key
    key: u64,
    /// The data in this partition
    rows: BTreeMap<T::Sort, T>,
    /// The size of this partition
    size: usize,
}

impl<T: ShoalSortedTable> SortedPartition<T> {
    /// Create a new partition
    ///
    /// # Arguments
    ///
    /// * `key` - The key to this partition
    pub fn new(key: u64) -> Self {
        SortedPartition {
            key,
            rows: BTreeMap::default(),
            size: 0,
        }
    }

    /// add a new row to this partition
    ///
    /// # Arguments
    ///
    /// * `row` - The row to insert
    pub fn insert(&mut self, row: T) -> (isize, ResponseAction<T>) {
        // get this rows sort key
        let sort_key = row.get_sort().clone();
        // calculate the size of our new row
        let row_size = std::mem::size_of_val(&row);
        // add this row
        let diff = match self.rows.insert(sort_key, row) {
            // we replaced an existing row so find the delta in size
            Some(replaced) => {
                // calculate our old rows size
                let old_size = std::mem::size_of_val(&replaced);
                // calculate the diff in sizes
                row_size.cast_signed() - old_size.cast_signed()
            }
            None => row_size.cast_signed(),
        };
        // adjust this parititons size correctly
        self.size = self.size.saturating_add_signed(diff);
        // respond that we inserted a row
        (diff, ResponseAction::Insert(true))
    }

    /// Get some rows from this partition
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters to use to get the rows
    /// * `found` - The vector to push the data to return
    pub fn get(&self, params: &SortedGet<T>, found: &mut Vec<T>) {
        // get rows from this partition
        for row in self.rows.values() {
            // skip any rows that don't match our filter
            if let Some(filter) = &params.filters {
                // check if this row should be filtered out
                if !T::is_filtered(filter, row) {
                    // skip this row since it doesn't match our filter
                    continue;
                }
            }
            // add this row to our response
            found.push(row.clone());
            // get our limit if we have one set
            if let Some(limit) = params.limit {
                // check if we found enough data
                if found.len() >= limit {
                    break;
                }
            }
        }
    }

    /// Remove a row from this partition
    ///
    /// # Arguments
    ///
    /// * `sort` - The sort key of the row to delete
    pub fn remove(&mut self, sort: &T::Sort) -> Option<(usize, T)> {
        match self.rows.remove(sort) {
            Some(removed) => {
                // calculate the size of the row we removed
                let row_size = std::mem::size_of_val(&removed);
                // decrement our paritions size with this estimate
                self.size = self.size.saturating_sub(row_size);
                Some((row_size, removed))
            }
            None => None,
        }
    }

    /// Update a row in this partition
    pub fn update(&mut self, update: &SortedUpdate<T>) -> bool {
        // get the row to update
        match self.rows.get_mut(&update.sort_key) {
            // we foudn the target row so apply our update
            Some(row) => {
                row.update(&update);
                true
            }
            None => false,
        }
    }
}

impl<T: ShoalSortedTable> RkyvSupport for SortedPartition<T> where
    <<T as ShoalSortedTable>::Sort as Archive>::Archived: Ord
{
}
