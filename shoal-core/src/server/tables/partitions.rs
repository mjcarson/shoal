//! A partition is a collection of data in shoal accesible by a partition key

use deepsize2::DeepSizeOf;
use glommio::io::ReadResult;
use gxhash::GxHashSet;
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::with::Skip;
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

/// A partition that may be fully loaded into memory or accesible as an archive
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

/// A row that may exist or may be a tombstone of a deleted row
#[derive(Debug, Archive, Serialize, Deserialize, DeepSizeOf)]
pub enum MaybeRow<R> {
    /// A row that still exists
    Row(R),
    /// The tombstone of a deleted row
    Tombstone,
}

#[derive(Debug, Archive, Serialize, Deserialize, DeepSizeOf)]
pub struct UnsortedPartition<R: ShoalUnsortedTable> {
    /// This partitions key
    pub key: u64,
    /// The data in this partition or the tombstone of its deleted row
    pub row: MaybeRow<R>,
    /// The size of this partition
    pub size: usize,
}

impl<R: ShoalUnsortedTable> UnsortedPartition<R> {
    /// Create a new partition
    ///
    /// # Arguments
    ///
    /// * `key` - The key to this partition
    /// * `row` - The row this partition contains
    pub fn new(key: u64, row: R) -> Self {
        // get the size of this row + 17 bytes (8 for key, 8 for size, 1 for evictable)
        let size = row.deep_size_of() + 17;
        UnsortedPartition {
            key,
            row: MaybeRow::Row(row),
            size,
        }
    }

    /// Create the tombstone of a deleted partition
    ///
    /// An unsorted partition holds exactly one row, so deleting that row cannot
    /// simply drop the partition from memory: the pre-delete copy may still be
    /// sitting in an archive, and any later read would load it back. The
    /// tombstone shadows that copy until compaction prunes it for real.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to this partition
    pub fn tombstone(key: u64) -> Self {
        // a tombstone carries no row data so it only costs its fixed overhead
        UnsortedPartition {
            key,
            row: MaybeRow::Tombstone,
            size: 17,
        }
    }

    /// Check if this partitions row has been deleted
    pub fn is_tombstoned(&self) -> bool {
        matches!(self.row, MaybeRow::Tombstone)
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
        // a deleted row has no data to return
        let MaybeRow::Row(row) = &self.row else {
            return false;
        };
        // skip any rows that don't match our filter
        if let Some(filter) = &params.filters {
            // check if this row should be filtered out
            if !R::is_filtered(filter, row) {
                // skip this row since it doesn't match our filteri
                return false;
            }
        }
        // add this row to our response
        found.push(row.clone());
        true
    }

    /// Update a row in this partition
    ///
    /// Returns true if a row was updated and false if this partition has been
    /// deleted and so has nothing to update.
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply
    pub fn update(&mut self, update: &UnsortedUpdate<R>) -> bool {
        // a deleted row cannot be updated
        let MaybeRow::Row(row) = &mut self.row else {
            return false;
        };
        // update this rows data
        row.update(&update);
        // update the size of our row
        self.size = self.deep_size_of();
        true
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
                let access = UnsortedPartition::<R>::access(read).unwrap();
                // a deleted row has no data to return
                let ArchivedMaybeRow::Row(archived) = &access.row else {
                    return false;
                };
                // skip any rows that don't match our filter
                if let Some(filter) = &params.filters {
                    // check if this row should be filtered out
                    if !R::is_filtered_archived(filter, archived) {
                        // skip this row since it doesn't match our filteri
                        return false;
                    }
                }
                // deserialize our row
                let row = R::deserialize(archived).unwrap();
                // add this row to our response
                found.push(row);
                true
            }
        }
    }

    /// Check if this partitions row has been deleted
    pub fn is_tombstoned(&self) -> bool {
        match self {
            MaybeLoaded::Loaded { partition, .. } => partition.is_tombstoned(),
            MaybeLoaded::Accessible(read) => {
                // access our data
                let access = UnsortedPartition::<R>::access(read).unwrap();
                // check if this archived row is a tombstone
                matches!(access.row, ArchivedMaybeRow::Tombstone)
            }
        }
    }

    /// Update a row in this partition
    ///
    /// Returns the deserialized partition if this update forced us to deserialize
    /// it, so our caller can replace their accessible partition with it.
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply
    pub fn update(&mut self, update: &UnsortedUpdate<R>) -> Option<UnsortedPartition<R>> {
        // get our row or deserialize it
        match self {
            MaybeLoaded::Loaded { partition, .. } => {
                // this row is already loaded so just update it in place
                partition.update(update);
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
    pub rows: BTreeMap<T::Sort, MaybeRow<T>>,
    /// The size of this partition
    size: usize,
    /// Whether this partition might have data on disk
    pub check_disk: bool,
    /// How many of our rows are tombstones
    ///
    /// This is never written to an archive because compaction removes deleted rows
    /// outright instead of tombstoning them, so a partition read back from disk has
    /// none. It exists only so that sweeping dead tombstones does not have to walk
    /// every row of every partition to find out there are none.
    #[rkyv(with = Skip)]
    tombstones: usize,
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
            check_disk: true,
            tombstones: 0,
        }
    }

    /// add a new row to this partition
    ///
    /// # Arguments
    ///
    /// * `row` - The row to insert
    pub fn insert(&mut self, row: T) -> (isize, ResponseAction<T>) {
        // get this rows sort key
        let sort_key = row.get_sort();
        // calculate the size of our new row
        let row_size = row.deep_size_of();
        // add this row wrapped in MaybeRow::Row
        let diff = match self.rows.insert(sort_key, MaybeRow::Row(row)) {
            // we replaced an existing row so find the delta in size
            Some(MaybeRow::Row(replaced)) => {
                // calculate our old rows size
                let old_size = replaced.deep_size_of();
                // calculate the diff in sizes
                row_size.cast_signed() - old_size.cast_signed()
            }
            // this row was deleted and is now back, so it is no longer a tombstone
            Some(MaybeRow::Tombstone) => {
                self.tombstones -= 1;
                row_size.cast_signed()
            }
            // this is a brand new row
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
        // get live rows from this partition (tombstones are skipped)
        for row in self.live_row_values() {
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
    /// Remove a row from this partition by replacing it with a tombstone
    ///
    /// Only inserts a tombstone if the row currently exists as a Row variant.
    /// Returns None if the row doesn't exist or is already a tombstone.
    ///
    /// # Arguments
    ///
    /// * `sort` - The sort key of the row to delete
    pub fn remove(&mut self, sort: &T::Sort) -> Option<(usize, T)> {
        // only tombstone if the row actually exists
        if !matches!(self.rows.get(sort), Some(MaybeRow::Row(_))) {
            return None;
        }
        // replace the row with a tombstone
        match self.rows.insert(sort.clone(), MaybeRow::Tombstone) {
            Some(MaybeRow::Row(removed)) => {
                // calculate the size of the row we removed
                let row_size = removed.deep_size_of();
                // decrement our partitions size with this estimate
                self.size = self.size.saturating_sub(row_size);
                // track that we are now shadowing a row with a tombstone
                self.tombstones += 1;
                Some((row_size, removed))
            }
            // SAFETY: we checked above that the row exists as Row
            _ => unreachable!(),
        }
    }

    /// Insert a tombstone for a sort key unconditionally
    ///
    /// Used during intent replay where we need tombstones to overlay
    /// partition data that may be loaded from disk later.
    ///
    /// # Arguments
    ///
    /// * `sort` - The sort key to tombstone
    pub fn tombstone(&mut self, sort: &T::Sort) -> isize {
        match self.rows.insert(sort.clone(), MaybeRow::Tombstone) {
            Some(MaybeRow::Row(removed)) => {
                let row_size = removed.deep_size_of();
                self.size = self.size.saturating_sub(row_size);
                // track that we are now shadowing a row with a tombstone
                self.tombstones += 1;
                -(row_size as isize)
            }
            // this key had nothing in memory, so our tombstone is shadowing disk
            None => {
                self.tombstones += 1;
                0
            }
            // this key was already tombstoned
            Some(MaybeRow::Tombstone) => 0,
        }
    }

    /// Merge a copy of this partition that was read from an archive into this one
    ///
    /// The disk copy becomes the base and our rows are replayed on top of it, since
    /// memory is always the newer of the two. `BTreeMap::extend` overwrites on a key
    /// collision, so an in memory tombstone still shadows the archived row it hides.
    ///
    /// # Arguments
    ///
    /// * `disk` - The copy of this partition that was read from an archive
    pub fn merge_from_disk(&mut self, disk: Self) {
        // keep our in memory rows to the side and make the disk copy our base
        let memory = std::mem::replace(self, disk);
        // replay our in memory rows ontop of the disk copy
        self.rows.extend(memory.rows.into_iter());
        // archives never contain tombstones so only our in memory ones survived
        self.tombstones = memory.tombstones;
        // we just merged in the full disk copy so there is nothing left to load
        self.check_disk = false;
    }

    /// Drop every tombstone in this partition and return how many were dropped
    ///
    /// A tombstone shadows a row that may still be in an archive, so this may only
    /// be called once every delete this partition has issued has been compacted -
    /// at that point the archive no longer holds those rows and there is nothing
    /// left to shadow. Dropping one early resurrects the row it was hiding.
    pub fn drop_tombstones(&mut self) -> usize {
        // bail out early if we have nothing to sweep
        if self.tombstones == 0 {
            return 0;
        }
        // only keep rows that still hold data
        self.rows.retain(|_, row| matches!(row, MaybeRow::Row(_)));
        // our tombstones are all gone now
        std::mem::take(&mut self.tombstones)
    }

    /// Update a row in this partition
    ///
    /// Returns `Some(diff)` with the change in memory usage if the row was
    /// found and updated, or `None` if the row does not exist.
    pub fn update(&mut self, update: &SortedUpdate<T>) -> Option<isize> {
        // get the row to update
        match self.rows.get_mut(&update.sort_key) {
            // we found the target row so apply our update
            Some(MaybeRow::Row(row)) => {
                // measure old size
                let old_size = row.deep_size_of();
                // update our row
                row.update(&update);
                // measure new size and compute diff
                let new_size = row.deep_size_of();
                let diff = new_size.cast_signed() - old_size.cast_signed();
                // adjust this partitions size correctly
                self.size = self.size.saturating_add_signed(diff);
                Some(diff)
            }
            // tombstones and missing rows can't be updated
            Some(MaybeRow::Tombstone) | None => None,
        }
    }

    /// Iterate over only the live rows in this partition, skipping tombstones
    pub fn live_rows(&self) -> impl Iterator<Item = (&T::Sort, &T)> {
        self.rows.iter().filter_map(|(k, v)| match v {
            MaybeRow::Row(row) => Some((k, row)),
            MaybeRow::Tombstone => None,
        })
    }

    /// Iterate over only the live row values in this partition, skipping tombstones
    pub fn live_row_values(&self) -> impl Iterator<Item = &T> {
        self.rows.values().filter_map(|v| match v {
            MaybeRow::Row(row) => Some(row),
            MaybeRow::Tombstone => None,
        })
    }

    /// Check if this partition is empty
    pub fn is_empty(&self) -> bool {
        // check if our rows is empty
        self.rows.is_empty()
    }
}

impl<T: ShoalSortedTable> ArchivedSortedPartition<T>
where
    <<T as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
{
    /// Iterate over only the live rows in this archived partition, skipping tombstones
    pub fn live_rows(
        &self,
    ) -> impl Iterator<
        Item = (
            &<<T as ShoalSortedTable>::Sort as Archive>::Archived,
            &<T as Archive>::Archived,
        ),
    > {
        self.rows.iter().filter_map(|(k, v)| match v {
            ArchivedMaybeRow::Row(row) => Some((k, row)),
            ArchivedMaybeRow::Tombstone => None,
        })
    }

    /// Iterate over only the live row values in this archived partition, skipping tombstones
    pub fn live_row_values(&self) -> impl Iterator<Item = &<T as Archive>::Archived> {
        self.rows.values().filter_map(|v| match v {
            ArchivedMaybeRow::Row(row) => Some(row),
            ArchivedMaybeRow::Tombstone => None,
        })
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

#[cfg(test)]
mod tests {
    use super::{MaybeRow, SortedPartition};
    use crate::shared::queries::parser::{FieldRole, TypeValidator};
    use crate::shared::queries::SortedUpdate;
    use crate::shared::traits::{
        PartitionKeySupport, RkyvSupport, ShoalSortedTable, ShoalTableSupport, TableSchemaSupport,
    };
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};

    /// The smallest sorted row that satisfies the table traits
    ///
    /// Only the pieces `SortedPartition` actually calls are meaningful here; the
    /// schema and filter hooks exist to satisfy the trait bounds.
    #[derive(Debug, Clone, Archive, Serialize, Deserialize, DeepSizeOf)]
    struct TestRow {
        /// The partition this row belongs to
        partition_key: String,
        /// The key this row is sorted by within its partition
        sort_key: String,
        /// This rows payload
        data: String,
    }

    impl TestRow {
        /// Create a test row
        ///
        /// # Arguments
        ///
        /// * `sort_key` - The sort key for this row
        fn new(sort_key: &str) -> Self {
            TestRow {
                partition_key: "partition".to_owned(),
                sort_key: sort_key.to_owned(),
                data: "data".to_owned(),
            }
        }
    }

    impl RkyvSupport for TestRow {}

    impl PartitionKeySupport for TestRow {
        type PartitionKey = String;

        fn name() -> &'static str {
            "TestRow"
        }

        fn get_partition_key(&self) -> u64 {
            0
        }

        fn get_partition_key_from_values(_sort: &Self::PartitionKey) -> u64 {
            0
        }

        fn get_partition_key_from_archived_insert(_intent: &<Self as Archive>::Archived) -> u64 {
            0
        }
    }

    impl TableSchemaSupport for TestRow {
        fn get_field_validator(_field_name: &str) -> Option<TypeValidator> {
            None
        }

        fn get_field_role(_field_name: &str) -> Option<FieldRole> {
            None
        }

        fn field_names() -> Vec<&'static str> {
            vec!["partition_key", "sort_key", "data"]
        }
    }

    impl ShoalTableSupport for TestRow {
        type Update = String;
        type UpdateData = String;
        type Filters = String;

        fn is_filtered(_filter: &Self::Filters, _row: &Self) -> bool {
            true
        }

        fn is_filtered_archived(
            _filter: &Self::Filters,
            _row: &<Self as Archive>::Archived,
        ) -> bool {
            true
        }
    }

    impl ShoalSortedTable for TestRow {
        type Sort = String;

        fn get_sort(&self) -> Self::Sort {
            self.sort_key.clone()
        }

        fn update(&mut self, update: &SortedUpdate<Self>) {
            self.data = update.update.clone();
        }
    }

    #[test]
    /// A delete leaves a tombstone behind and reinserting the row takes it away
    fn tombstones_are_counted() {
        // build a partition with two rows in it
        let mut partition = SortedPartition::<TestRow>::new(0);
        partition.insert(TestRow::new("a"));
        partition.insert(TestRow::new("b"));
        assert_eq!(partition.tombstones, 0);
        // delete one of them
        assert!(partition.remove(&"a".to_owned()).is_some());
        assert_eq!(partition.tombstones, 1);
        // deleting a row that is already a tombstone changes nothing
        assert!(partition.remove(&"a".to_owned()).is_none());
        assert_eq!(partition.tombstones, 1);
        // an unconditional tombstone for a key we have never seen still counts, since
        // it is shadowing a row that may be sitting in an archive
        partition.tombstone(&"c".to_owned());
        assert_eq!(partition.tombstones, 2);
        // reinserting a deleted row takes its tombstone away
        partition.insert(TestRow::new("a"));
        assert_eq!(partition.tombstones, 1);
    }

    #[test]
    /// Sweeping drops every tombstone and leaves live rows alone
    fn dropping_tombstones_keeps_live_rows() {
        // build a partition with two rows in it and delete one
        let mut partition = SortedPartition::<TestRow>::new(0);
        partition.insert(TestRow::new("a"));
        partition.insert(TestRow::new("b"));
        partition.remove(&"a".to_owned());
        // sweeping reports how many tombstones it dropped
        assert_eq!(partition.drop_tombstones(), 1);
        assert_eq!(partition.tombstones, 0);
        // our deleted row is gone entirely and our live row is untouched
        assert!(!partition.rows.contains_key(&"a".to_owned()));
        assert!(matches!(
            partition.rows.get(&"b".to_owned()),
            Some(MaybeRow::Row(_))
        ));
        // sweeping a partition with no tombstones does nothing
        assert_eq!(partition.drop_tombstones(), 0);
    }

    #[test]
    /// A merged in disk copy keeps our tombstones and the count that goes with them
    fn merging_from_disk_keeps_tombstones() {
        // build the copy of this partition that an archive would hold
        let mut disk = SortedPartition::<TestRow>::new(0);
        disk.insert(TestRow::new("a"));
        disk.insert(TestRow::new("b"));
        // build the in memory copy, which has deleted one of those rows
        let mut memory = SortedPartition::<TestRow>::new(0);
        memory.insert(TestRow::new("a"));
        memory.remove(&"a".to_owned());
        // merge the disk copy in
        memory.merge_from_disk(disk);
        // our tombstone has to win over the archived row it is shadowing
        assert_eq!(memory.tombstones, 1);
        assert!(matches!(
            memory.rows.get(&"a".to_owned()),
            Some(MaybeRow::Tombstone)
        ));
        // and the row we never touched has to come back
        assert!(matches!(
            memory.rows.get(&"b".to_owned()),
            Some(MaybeRow::Row(_))
        ));
        // we just loaded the whole partition so there is nothing left to check for
        assert!(!memory.check_disk);
    }
}
