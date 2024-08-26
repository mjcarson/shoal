//! A partition is a collection of data in shoal accesible by a partition key

use std::collections::BTreeMap;

use crate::shared::queries::{Get, Update};
use crate::shared::responses::ResponseAction;
use crate::shared::traits::ShoalTable;

#[derive(Debug)]
pub struct Partition<T: ShoalTable> {
    /// The data in this partition
    rows: BTreeMap<T::Sort, T>,
}

impl<T: ShoalTable> Default for Partition<T> {
    /// Create a default partition
    fn default() -> Self {
        Partition {
            rows: BTreeMap::default(),
        }
    }
}

impl<T: ShoalTable> Partition<T> {
    /// add a new row to this partition
    ///
    /// # Arguments
    ///
    /// * `row` - The row to insert
    pub fn insert(&mut self, row: T) -> ResponseAction<T> {
        // get this rows sort key
        let sort_key = row.get_sort().clone();
        // add this row
        self.rows.insert(sort_key, row);
        // respond that we inserted a row
        ResponseAction::Insert(true)
    }

    /// Get some rows from this partition
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters to use to get the rows
    /// * `found` - The vector to push the data to return
    pub fn get(&self, params: &Get<T>, found: &mut Vec<T>) {
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
    pub fn remove(&mut self, sort: &T::Sort) -> Option<T> {
        self.rows.remove(sort)
    }

    /// Update a row in this partition
    pub fn update(&mut self, update: &Update<T>) -> bool {
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
