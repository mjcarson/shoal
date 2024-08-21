//! A partition is a collection of data in shoal accesible by a partition key

use std::collections::BTreeMap;

use crate::shared::queries::Get;
use crate::shared::responses::ResponseAction;
use crate::shared::traits::ShoalTable;

#[derive(Debug)]
pub struct Partition<R: ShoalTable> {
    /// The data in this partition
    rows: BTreeMap<R::Sort, R>,
}

impl<R: ShoalTable> Default for Partition<R> {
    /// Create a default partition
    fn default() -> Self {
        Partition {
            rows: BTreeMap::default(),
        }
    }
}

impl<D: ShoalTable> Partition<D> {
    /// add a new row to this partition
    ///
    /// # Arguments
    ///
    /// * `row` - The row to insert
    pub fn insert(&mut self, row: D) -> ResponseAction<D> {
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
    pub fn get(&self, params: &Get<D>, found: &mut Vec<D>) {
        // get rows from this partition
        for row in self.rows.values() {
            // skip any rows that don't match our filter
            if let Some(filter) = &params.filters {
                // check if this row should be filtered out
                if !D::is_filtered(filter, row) {
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
}
