//! A partition is a collection of data in shoal accesible by a partition key

use deepsize2::DeepSizeOf;
use glommio::io::ReadResult;
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::shared::queries::{SortedGet, SortedUpdate, UnsortedGet, UnsortedUpdate};
use crate::shared::responses::ResponseAction;
use crate::shared::traits::{RkyvSupport, ShoalSortedTable, ShoalUnsortedTable};

pub trait PartitionSupport: DeepSizeOf {
    /// Get this partitions size
    fn size(&self) -> usize {
        self.deep_size_of()
    }
}

#[derive(Debug)]
pub enum MaybeLoaded<P: PartitionSupport> {
    /// A fully loaded partition
    Loaded { partition: P, generation: u64 },
    /// An accessible but not fully loaded partition
    Accessible(ReadResult),
}

impl<P: PartitionSupport> MaybeLoaded<P> {
    /// Get this partitions size
    pub fn size(&self) -> usize {
        match self {
            Self::Loaded { partition, .. } => partition.size(),
            Self::Accessible(read) => read.len(),
        }
    }

    ///  Check if this partition is evictable or not
    pub fn is_evictable(&self, flushed_generation: u64) -> bool {
        match self {
            Self::Loaded { generation, .. } => *generation <= flushed_generation,
            Self::Accessible(_) => true,
        }
    }
}

#[derive(Debug, Archive, Serialize, Deserialize, DeepSizeOf)]
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
        // get the size of this row + 17 bytes (8 for key, 8 for size, 1 for evictable)
        let size = row.deep_size_of() + 17;
        UnsortedPartition { key, row, size }
    }

    /// Get some rows from this partition
    ///
    /// Returns true if data was returned and false if it wasn't
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters to use to get the rows
    /// * `found` - The vector to push the data to return
    pub fn get(&self, params: &UnsortedGet<R>, found: &mut Vec<R>) -> bool {
        // skip any rows that don't match our filter
        if let Some(filter) = &params.filters {
            // check if this row should be filtered out
            if !R::is_filtered(filter, &self.row) {
                // skip this row since it doesn't match our filteri
                return false;
            }
        }
        // add this row to our response
        found.push(self.row.clone());
        true
    }

    /// Update a row in this partition
    pub fn update(&mut self, update: &UnsortedUpdate<R>) {
        // update this rows data
        self.row.update(&update);
        // update the size of our row
        self.size = self.deep_size_of();
    }
}

impl<R: ShoalUnsortedTable> MaybeLoaded<UnsortedPartition<R>>
where
    for<'a> <R as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// Get some rows from this partition
    ///
    /// Returns true if data was returned and false if it wasn't
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters to use to get the rows
    /// * `found` - The vector to push the data to return
    pub fn get(&self, params: &UnsortedGet<R>, found: &mut Vec<R>) -> bool {
        // if this row is loaded then use the get on the row
        match self {
            MaybeLoaded::Loaded { partition, .. } => partition.get(params, found),
            MaybeLoaded::Accessible(read) => {
                // access our data
                let access = match UnsortedPartition::<R>::access(read) {
                    Ok(access) => access,
                    Err(error) => {
                        // build a hasher to verify this map
                        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
                        // hash our map
                        hasher.update(&read[..]);
                        // get theh hash for our
                        let partition_hash = hasher.digest();
                        panic!(
                            "ML - get {} -> {:?} ? {} : {:?} .. {:?}",
                            params.partition_key,
                            error,
                            partition_hash,
                            &read[..16],
                            &read[read.len() - 16..]
                        )
                    }
                };
                // skip any rows that don't match our filter
                if let Some(filter) = &params.filters {
                    // check if this row should be filtered out
                    if !R::is_filtered_archived(filter, &access.row) {
                        // skip this row since it doesn't match our filteri
                        return false;
                    }
                }
                // deserialize our row
                let row = R::deserialize(&access.row).unwrap();
                // add this row to our response
                found.push(row);
                true
            }
        }
    }

    /// Update a row in this partition
    pub fn update(&mut self, update: &UnsortedUpdate<R>) -> Option<UnsortedPartition<R>> {
        // get our row or deserialize it
        match self {
            MaybeLoaded::Loaded { partition, .. } => {
                // this row is already loaded so just update it in place
                partition.update(update);
                // update the size of our row
                partition.size = partition.deep_size_of();
                // we don't need to replace our wrapped row so return none
                None
            }
            MaybeLoaded::Accessible(read) => {
                // access our data
                let access = UnsortedPartition::<R>::access(read).unwrap();
                // deserialize our row
                let mut loaded = UnsortedPartition::<R>::deserialize(&access).unwrap();
                // update this rows data
                loaded.update(&update);
                // update the size of our row
                loaded.size = loaded.deep_size_of();
                // we loaded an updated our row so return it
                Some(loaded)
            }
        }
    }

    /// Get the data from a loaded partition or deserialize it from an accesible one
    pub fn deserialize(self) -> Result<UnsortedPartition<R>, rkyv::rancor::Error> {
        match self {
            MaybeLoaded::Loaded { partition, .. } => Ok(partition),
            MaybeLoaded::Accessible(read) => {
                // access our data
                let access = UnsortedPartition::<R>::access(&read)?;
                // deserialize our row
                UnsortedPartition::<R>::deserialize(&access)
            }
        }
    }
}

impl<T: ShoalUnsortedTable> RkyvSupport for UnsortedPartition<T> {}

impl<R: ShoalUnsortedTable> PartitionSupport for UnsortedPartition<R> {
    fn size(&self) -> usize {
        self.size
    }
}

/// A partition that can contain multiple sorted rows
#[derive(Debug, Archive, Serialize, Deserialize, DeepSizeOf)]
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
        let row_size = row.deep_size_of();
        // add this row
        let diff = match self.rows.insert(sort_key, row) {
            // we replaced an existing row so find the delta in size
            Some(replaced) => {
                // calculate our old rows size
                let old_size = replaced.deep_size_of();
                // calculate the diff in sizes
                row_size.cast_signed() - old_size.cast_signed()
            }
            None => row_size.cast_signed(),
        };
        // adjust this partitions size correctly
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
                let row_size = removed.deep_size_of();
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
                // TODO update row size
                // update our row
                row.update(&update);
                true
            }
            None => false,
        }
    }

    /// Check if this partition is empty
    pub fn is_empty(&self) -> bool {
        // check if our rows is empty
        self.rows.is_empty()
    }
}

impl<T: ShoalSortedTable> RkyvSupport for SortedPartition<T> where
    <<T as ShoalSortedTable>::Sort as Archive>::Archived: Ord
{
}

impl<T: ShoalSortedTable> PartitionSupport for SortedPartition<T>
where
    <<T as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
{
    fn size(&self) -> usize {
        self.size
    }
}
