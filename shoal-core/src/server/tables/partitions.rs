//! A partition is a collection of data in shoal accesible by a partition key

use deepsize2::DeepSizeOf;
use glommio::io::ReadResult;
use gxhash::GxHashSet;
use crate::server::tables::persistent::RowSink;
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use rkyv::util::AlignedVec;
use rkyv::with::Skip;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::{Bound, Deref};

use crate::shared::queries::{
    SortRange, SortSelect, SortedExists, SortedGet, SortedUpdate, UnsortedGet, UnsortedUpdate,
};
use crate::shared::responses::ResponseAction;
use crate::shared::traits::{RkyvSupport, ShoalProjection, ShoalSortedTable, ShoalUnsortedTable};

pub trait PartitionSupport: DeepSizeOf {
    /// Get this partitions size
    fn size(&self) -> usize {
        self.deep_size_of()
    }
}

/// A buffer of archived bytes that never move and never change
///
/// An archive is read where it lies rather than copied out, so whatever holds those bytes
/// has to keep handing back the same ones. A partition read off disk is always a glommio
/// [`ReadResult`]; an [`AlignedVec`] is what a sort key archived for a seek lives in, and
/// what a test or a benchmark builds a partition archive in, since a `ReadResult` can only
/// come from a live reactor.
///
/// # Safety
///
/// `deref` must return the same base pointer and the same length on every call for the
/// life of the value, and the bytes behind it must never change.
pub unsafe trait StableBytes: Deref<Target = [u8]> {}

// SAFETY: a `ReadResult` holds its pointer and length in fields that are only ever read
// through `&self`, and the DMA buffer behind them is never written to after the read
// completes, so every deref yields the same bytes.
unsafe impl StableBytes for ReadResult {}

// SAFETY: an `AlignedVec` only moves its buffer through `&mut self`, and a
// `ValidatedArchive` never hands one out, so every deref yields the same bytes.
//
// This is what a sort key archived for a seek is held in - see `SeekBytes` - and it is also
// how a test or a benchmark builds a partition archive, since a `ReadResult` needs a reactor.
unsafe impl StableBytes for AlignedVec {}

/// An archive whose bytes were validated when they were read, and are not validated again
///
/// `rkyv::access` is the checked entry point: it runs `bytecheck` over the whole buffer
/// before it will hand back a reference into it, which is O(bytes) and has to happen before
/// anything can be sought. Holding the *reference* it returns is not possible - it borrows
/// from the buffer beside it, which is self referential - so the archive of a partition used
/// to be re-validated by every query that touched it, and a get naming one row paid for the
/// size of the partition it landed in.
///
/// This holds the bytes and the *fact* that they were validated instead. [`Self::new`] is the
/// only way to build one and it validates; [`Self::archived`] then reads without validating.
/// Nothing is validated less often in total - a corrupt archive is still caught, at the read
/// that produced it rather than at every query afterwards.
///
/// See [F4](../../../../docs/src/features/validated-archives.md).
pub struct ValidatedArchive<P, B = ReadResult> {
    /// The archived bytes, validated exactly once by [`Self::new`]
    ///
    /// Private, and never handed out by reference or by `&mut`. That is what makes the
    /// unchecked read in [`Self::archived`] sound, so it must stay that way.
    raw: B,
    /// The number of bytes validation saw, and what memory accounting charges for
    len: usize,
    /// The offset of the archives root, computed from the length validation saw
    ///
    /// Pinned here rather than recomputed per read so that the length can play no part in
    /// the safety argument: a root position derived once from validated bytes cannot drift.
    root_pos: usize,
    /// The type these bytes are an archive of
    kind: PhantomData<fn() -> P>,
}

impl<P, B: StableBytes> ValidatedArchive<P, B> {
    /// Validate a buffer of archived bytes, once
    ///
    /// This is the only constructor, which is the invariant everything else here rests on.
    /// It is also where a misaligned buffer is caught - the unchecked read below only
    /// `debug_assert!`s alignment, so in a release build nothing else would.
    ///
    /// # Arguments
    ///
    /// * `raw` - The archived bytes to validate
    pub fn new(raw: B) -> Result<Self, rkyv::rancor::Error>
    where
        P: RkyvSupport,
        for<'a> <P as Archive>::Archived:
            CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    {
        // validate every byte of this archive, which is the only time it is validated
        P::access(&raw)?;
        // record what validation saw so no later read has to derive it again
        let len = raw.len();
        let root_pos = rkyv::api::root_position::<<P as Archive>::Archived>(len);
        Ok(ValidatedArchive {
            raw,
            len,
            root_pos,
            kind: PhantomData,
        })
    }

    /// Read this archive without validating it again
    ///
    /// # Safety
    ///
    /// This is safe because [`Self::new`] already validated these exact bytes and nothing
    /// can have changed them since. `rkyv::access` is `check_pos_with_context` followed by
    /// `access_pos_unchecked` on the same buffer and the same position, so this is the second
    /// half of a call that already succeeded. Three things keep that true, and all three are
    /// things to preserve rather than facts to rely on blindly:
    ///
    /// * `new` is the only constructor, so a `ValidatedArchive` that exists was validated.
    /// * `raw` is private and is never exposed by reference or by `&mut`, so the bytes behind
    ///   it are the bytes validation saw.
    /// * [`StableBytes`] requires every deref to yield that same pointer and length.
    pub fn archived(&self) -> &<P as Archive>::Archived
    where
        P: Archive,
    {
        // SAFETY: see the note above - these bytes passed `bytecheck` in `new`, they are
        // immutable for the life of this value, and `root_pos` came from the length that
        // validation saw
        unsafe {
            rkyv::api::access_pos_unchecked::<<P as Archive>::Archived>(&self.raw, self.root_pos)
        }
    }
}

impl<P, B> ValidatedArchive<P, B> {
    /// Get the number of archived bytes this holds
    ///
    /// Deliberately answered from the length recorded at construction rather than from the
    /// buffer, so that this needs none of the bounds reading the archive does.
    pub fn len(&self) -> usize {
        self.len
    }
}

/// Print what an archive is rather than what is in it
///
/// Derived `Debug` would bound `P` and `B` on `Debug` and would print every byte of the
/// buffer, which is not useful for a partition sized archive.
impl<P, B> std::fmt::Debug for ValidatedArchive<P, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidatedArchive")
            .field("len", &self.len)
            .finish()
    }
}

/// A partition that may be fully loaded into memory or accesible as an archive
///
/// The buffer an archive is held in is a type parameter rather than a [`ReadResult`]
/// because a `ReadResult` can only come from a real DMA read - its constructors are
/// private to glommio - which left this whole arm unreachable from a test or a benchmark.
/// Production never names `B`, since it defaults to the type the loader produces.
#[derive(Debug)]
pub enum MaybeLoaded<P: PartitionSupport, B = ReadResult> {
    /// A fully loaded partition
    Loaded { partition: P, generation: u64 },
    /// An accessible but not fully loaded partition
    Accessible(ValidatedArchive<P, B>),
}

impl<P: PartitionSupport, B> MaybeLoaded<P, B> {
    /// Get this partitions size
    pub fn size(&self) -> usize {
        match self {
            Self::Loaded { partition, .. } => partition.size(),
            Self::Accessible(archive) => archive.len(),
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
    /// * `found` - Where to record the row this partition answers with
    pub fn get<'a, P: ShoalProjection<Row = R>>(
        &'a self,
        params: &UnsortedGet<R>,
        found: &mut RowSink<'a, P>,
    ) -> bool {
        // a get that already holds every row it asked for has nothing to take from us
        if params.limit_reached(found.len()) {
            return false;
        }
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
        // answer with the row this partition is holding, if that is what this get asked for
        //
        // an unprojected get asks for the whole row, and a resident whole row is already the
        // answer - so it is pointed at rather than cloned
        // ([O2](../../../docs/src/appendix/optimizations.md)). A projection is a strict subset
        // of its row and still has to be built
        match P::IDENTITY {
            Some(identity) => found.push_resident(identity(row)),
            None => found.push_built(P::from_row(row)),
        }
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

impl<R: ShoalUnsortedTable, B: StableBytes> MaybeLoaded<UnsortedPartition<R>, B>
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
    /// * `found` - Where to record the row this partition answers with
    pub fn get<'a, P: ShoalProjection<Row = R>>(
        &'a self,
        params: &UnsortedGet<R>,
        found: &mut RowSink<'a, P>,
    ) -> bool {
        // a get that already holds every row it asked for has nothing to take from us
        if params.limit_reached(found.len()) {
            return false;
        }
        // if this row is loaded then use the get on the row
        match self {
            MaybeLoaded::Loaded { partition, .. } => partition.get(params, found),
            MaybeLoaded::Accessible(read) => {
                // access our data
                let access = read.archived();
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
                // answer with the fields this get asked for, straight out of the archive
                //
                // an unprojected get asks for the whole row, and the whole row is already
                // sitting here in the layout the wire wants, so point at it and let the
                // response serialize it where it lies. A projection names a strict subset of
                // the rows fields, so it has no archive of its own to point at and is built
                // the way it always was
                match P::ARCHIVED_IDENTITY {
                    Some(_) => found.push_archived(archived),
                    None => found.push_built(P::from_archived(archived)),
                }
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
                let access = read.archived();
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
                let access = read.archived();
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
                let access = read.archived();
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
        hotpath::measure_block!("SortedPartition::insert", {
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
        })
    }

    /// Seek a live row in this partition by its sort key
    ///
    /// A tombstone is a row that was deleted, so it is a miss here rather than something
    /// the caller has to remember to check for.
    ///
    /// # Arguments
    ///
    /// * `sort_key` - The sort key of the row to seek
    fn live_row(&self, sort_key: &T::Sort) -> Option<&T> {
        // only a live row is a row we hold
        match self.rows.get(sort_key) {
            Some(MaybeRow::Row(row)) => Some(row),
            Some(MaybeRow::Tombstone) | None => None,
        }
    }

    /// Iterate over the live rows of this partition whose sort key falls inside a range
    ///
    /// The caller has already checked that the range can contain a key, because
    /// `BTreeMap::range` panics on one that cannot.
    ///
    /// # Arguments
    ///
    /// * `range` - The range of sort keys to walk
    fn live_rows_in_range<'a>(
        &'a self,
        range: &'a SortRange<T::Sort>,
    ) -> impl Iterator<Item = &'a T> {
        // seek to this ranges lower bound and walk in sort order until its upper one
        self.rows
            .range(range.bounds())
            .filter_map(|(_, row)| match row {
                MaybeRow::Row(row) => Some(row),
                MaybeRow::Tombstone => None,
            })
    }

    /// Collect the rows a scan visited into a gets response
    ///
    /// The three ways a get can select rows differ only in which rows they visit, so the
    /// filtering, the limit check, and the push live here once instead of once per arm.
    ///
    /// The limit is checked against `found` before a row is pushed rather than after,
    /// because `found` is shared by every partition a get touches. Checking after the
    /// push hands back one extra row for every partition past the one that filled the
    /// limit, and hands back a row at all for a get whose limit was already full when it
    /// got here.
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters of the get these rows were visited for
    /// * `rows` - The rows this gets selection visited, in sort order
    /// * `found` - Where to record the rows this partition answers with
    fn collect_rows<'a, P: ShoalProjection<Row = T>, I: Iterator<Item = &'a T>>(
        params: &SortedGet<T>,
        rows: I,
        found: &mut RowSink<'a, P>,
    ) where
        T: 'a,
    {
        hotpath::measure_block!("SortedPartition::collect_rows", {
            // whether this get can answer with the rows themselves, decided once for the scan
            // rather than once per row so the branch is hoisted out of the loop
            let identity = P::IDENTITY;
            // visit the rows this get selected until we hold as many as it asked for
            for row in rows {
                // stop scanning once we hold every row this get asked for
                if params.limit_reached(found.len()) {
                    break;
                }
                // skip any rows that don't match our filter
                if let Some(filter) = &params.filters {
                    // check if this row should be filtered out
                    if !T::is_filtered(filter, row) {
                        // skip this row since it doesn't match our filter
                        continue;
                    }
                }
                // answer with the row this partition is holding, if that is what this get asked
                // for
                //
                // an unprojected get asks for the whole row, and a resident whole row is already
                // the answer - so it is pointed at rather than cloned
                // ([O2](../../../docs/src/appendix/optimizations.md))
                match identity {
                    Some(identity) => found.push_resident(identity(row)),
                    None => found.push_built(P::from_row(row)),
                }
            }
        })
    }

    /// Check whether any of the rows a scan visited survives an exists filters
    ///
    /// This is the twin of [`Self::collect_rows`] and is shared by the same three arms for
    /// the same reason.
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters of the exists these rows were visited for
    /// * `rows` - The rows this exists selection visited, in sort order
    fn any_row<'a, I: Iterator<Item = &'a T>>(params: &SortedExists<T>, rows: I) -> bool
    where
        T: 'a,
    {
        // visit the rows this exists selected until one of them survives our filters
        for row in rows {
            // skip any rows that don't match our filter
            if let Some(filter) = &params.filters {
                // check if this row should be filtered out
                if !T::is_filtered(filter, row) {
                    // skip this row since it doesn't match our filter
                    continue;
                }
            }
            // we hold one of the rows this exists asked about
            return true;
        }
        // we hold none of the rows this exists asked about
        false
    }

    /// Get some rows from this partition
    ///
    /// A get naming sort keys is asking for those rows and no others, so each of them is
    /// sought in our tree rather than walked to. The keys are sought in the order they
    /// were given, which is sort order because `normalize_sort_keys` put them in it when
    /// this query entered the server. A get bounding them by a range seeks to that ranges
    /// lower bound and walks until its upper one, which is why paging costs a page rather
    /// than a partition. A get selecting every row walks all of them.
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters to use to get the rows
    /// * `found` - Where to record the rows this partition answers with
    pub fn get<'a, P: ShoalProjection<Row = T>>(
        &'a self,
        params: &'a SortedGet<T>,
        found: &mut RowSink<'a, P>,
    ) {
        hotpath::measure_block!("SortedPartition::get", {
            // visit the rows this get selected, however it chose to select them
            match &params.sort_select {
                // this get asked for the whole partition, so walk it (tombstones are skipped)
                SortSelect::All => Self::collect_rows(params, self.live_row_values(), found),
                // this get named its rows, so seek each of them instead of walking to it
                SortSelect::Keys(keys) => {
                    let rows = keys.iter().filter_map(|sort_key| self.live_row(sort_key));
                    Self::collect_rows(params, rows, found);
                }
                // this get bounded its rows, so seek to the lower bound and walk to the upper one
                SortSelect::Range(range) => {
                    // a range that cannot contain a key holds no rows, and would panic the seek
                    if range.is_empty() {
                        return;
                    }
                    Self::collect_rows(params, self.live_rows_in_range(range), found);
                }
            }
        })
    }

    /// Check if any of the rows this exists selected are in this partition
    ///
    /// This is the twin of [`Self::get`] and selects its rows exactly the same three ways.
    /// An exists selecting every row is asking whether this partition holds any row at all,
    /// which is what it has always answered.
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters to use to check for rows
    pub fn exists(&self, params: &SortedExists<T>) -> bool {
        // visit the rows this exists selected, however it chose to select them
        match &params.sort_select {
            // this exists asks about this partition and not about any row in particular
            SortSelect::All => Self::any_row(params, self.live_row_values()),
            // this exists named its rows, so seek each of them instead of walking to it
            SortSelect::Keys(keys) => {
                let rows = keys.iter().filter_map(|sort_key| self.live_row(sort_key));
                Self::any_row(params, rows)
            }
            // this exists bounded its rows, so seek to the lower bound and walk to the upper
            SortSelect::Range(range) => {
                // a range that cannot contain a key holds no rows, and would panic the seek
                if range.is_empty() {
                    return false;
                }
                Self::any_row(params, self.live_rows_in_range(range))
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
    /// This is the one place a partitions size is recomputed instead of maintained by
    /// delta. Neither input describes what we end up holding: the disk copy's size
    /// counts none of our in memory rows and ours counts none of the archived ones,
    /// and only the rows themselves say which of the two won each key.
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
        // the union of both copies is what we hold now, so recompute our size from
        // the rows we ended up with, counting only the live ones like our other paths
        self.size = self
            .rows
            .values()
            .filter_map(|row| match row {
                MaybeRow::Row(row) => Some(row.deep_size_of()),
                MaybeRow::Tombstone => None,
            })
            .sum();
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

/// The archived forms of the sort keys a get or exists selected
///
/// A partition being read in place holds its keys in their archived form, so a key being
/// sought there has to be put in that form to be compared against them. Those bytes are the
/// same for every partition of one query, so they are built at most once per execution and
/// only when a partition of it is actually being read in place - a resident partition is
/// sought with the key exactly as it stands, and pays nothing for this.
///
/// They are held as [`ValidatedArchive`]s rather than as raw bytes for the same reason a
/// partition is: a seek used to validate the key it was looking for once per key per
/// partition, and it is the same bytes every time.
#[derive(Debug)]
pub struct SeekBytes<S> {
    /// The archived form of each sort key that was named, in the order they were named
    keys: Vec<ValidatedArchive<S, AlignedVec>>,
    /// The archived form of the value a ranges lower bound holds, if it holds one
    start: Option<ValidatedArchive<S, AlignedVec>>,
    /// The archived form of the value a ranges upper bound holds, if it holds one
    end: Option<ValidatedArchive<S, AlignedVec>>,
}

/// A selection of every row names nothing to seek with
///
/// Written out rather than derived because a derive would require the sort key itself to
/// be `Default`, which nothing else here asks of it.
impl<S> Default for SeekBytes<S> {
    fn default() -> Self {
        SeekBytes {
            keys: Vec::new(),
            start: None,
            end: None,
        }
    }
}

impl<S: RkyvSupport> SeekBytes<S>
where
    for<'a> <S as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    /// Archive the keys and bounds a selection named, and validate them once
    ///
    /// A selection of every row names neither, so this is empty for it.
    ///
    /// # Arguments
    ///
    /// * `select` - The selection whose keys and bounds to archive
    pub fn new(select: &SortSelect<S>) -> Self {
        hotpath::measure_block!("SeekBytes::new", {
            // archive whichever of a set of keys or a pair of bounds this selection named
            match select {
                // a selection of every row names no key to seek with
                SortSelect::All => SeekBytes::default(),
                // a set of keys is archived one key at a time, in the order they were named
                SortSelect::Keys(keys) => SeekBytes {
                    keys: keys.iter().map(|key| Self::archive_key(key)).collect(),
                    start: None,
                    end: None,
                },
                // a range only has a value to archive at an end that bounds something
                SortSelect::Range(range) => SeekBytes {
                    keys: Vec::new(),
                    start: Self::bound_bytes(&range.start),
                    end: Self::bound_bytes(&range.end),
                },
            }
        })
    }

    /// Archive one sort key and validate it, once for the whole query
    ///
    /// The unwrap cannot fire on bytes this process serialized a line earlier - it is the
    /// price of holding the validated form rather than re-validating at every seek.
    ///
    /// # Arguments
    ///
    /// * `key` - The sort key to archive
    fn archive_key(key: &S) -> ValidatedArchive<S, AlignedVec> {
        ValidatedArchive::new(<S as RkyvSupport>::serialize(key))
            .expect("a sort key we just archived failed validation")
    }

    /// Archive the value one end of a range holds, if it holds one
    ///
    /// # Arguments
    ///
    /// * `bound` - The end of the range to archive
    fn bound_bytes(bound: &Bound<S>) -> Option<ValidatedArchive<S, AlignedVec>> {
        // an unbounded end holds no value to compare an archives keys against
        match bound {
            Bound::Included(key) | Bound::Excluded(key) => Some(Self::archive_key(key)),
            Bound::Unbounded => None,
        }
    }
}

impl<S> SeekBytes<S> {
    /// Iterate over the archived form of each sort key that was named
    fn keys(&self) -> impl Iterator<Item = &ValidatedArchive<S, AlignedVec>> {
        self.keys.iter()
    }

    /// Get the archived form of the value a ranges lower bound holds
    fn start(&self) -> Option<&ValidatedArchive<S, AlignedVec>> {
        self.start.as_ref()
    }

    /// Get the archived form of the value a ranges upper bound holds
    fn end(&self) -> Option<&ValidatedArchive<S, AlignedVec>> {
        self.end.as_ref()
    }
}

impl<R: ShoalSortedTable, B: StableBytes> MaybeLoaded<SortedPartition<R>, B>
where
    <<R as ShoalSortedTable>::Sort as Archive>::Archived: Ord,
    <R as Archive>::Archived: rkyv::Deserialize<R, Strategy<Pool, rkyv::rancor::Error>>,
    for<'a> <<R as ShoalSortedTable>::Sort as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    for<'a> <R as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    /// Seek a live row in an archived partition by the archived form of its sort key
    ///
    /// An archives keys are the archived form of a sort key, so the key being looked for
    /// has to be put in that form to be compared against them, and the seek compares two
    /// archived keys. That leans on `Archived<Sort>` ordering the way `Sort` does, which is
    /// the same thing the archive already leans on: it was written out in `Sort` order and
    /// is searched in `Archived<Sort>` order.
    ///
    /// The bytes are handed in rather than built here because they are the same for every
    /// partition of one query - see [`SeekBytes`].
    ///
    /// # Arguments
    ///
    /// * `access` - The archived partition to seek in
    /// * `raw` - The archived form of the sort key of the row to seek
    fn seek_archived<'a>(
        access: &'a ArchivedSortedPartition<R>,
        raw: &ValidatedArchive<R::Sort, AlignedVec>,
    ) -> Option<&'a <R as Archive>::Archived> {
        hotpath::measure_block!("MaybeLoaded::seek_archived", {
            // read the archived form of our key, which was validated when it was built
            let wanted = raw.archived();
            // seek this key in the archive, where a tombstone is a row that was deleted
            match access.rows.get(wanted) {
                // this archive holds the row we were looking for
                Some(ArchivedMaybeRow::Row(row)) => Some(row),
                // this archive either never held this row or holds a tombstone of it
                Some(ArchivedMaybeRow::Tombstone) | None => None,
            }
        })
    }

    /// Put one end of a range in the form an archives keys are in
    ///
    /// Only the value an end holds needs archiving; whether that end includes its value is
    /// a property of the query and is carried over as it stands. An end holding no value
    /// bounds nothing and stays unbounded.
    ///
    /// # Arguments
    ///
    /// * `bound` - The end of the range being archived
    /// * `raw` - The archived form of the value that end holds, if it holds one
    fn archived_bound<'a>(
        bound: &Bound<R::Sort>,
        raw: Option<&'a ValidatedArchive<R::Sort, AlignedVec>>,
    ) -> Bound<&'a <<R as ShoalSortedTable>::Sort as Archive>::Archived> {
        // an end with no value cannot bound anything
        let Some(raw) = raw else {
            return Bound::Unbounded;
        };
        // read the archived form of this ends value, validated when it was built
        let wanted = raw.archived();
        match bound {
            Bound::Included(_) => Bound::Included(wanted),
            Bound::Excluded(_) => Bound::Excluded(wanted),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    /// Collect the archived rows a scan visited into a gets response
    ///
    /// This is the archived twin of `SortedPartition::collect_rows` and is shared by the
    /// same three arms for the same reason. A row is deserialized only once it has survived
    /// the filters, so a scan pays for the rows it returns rather than the rows it visits.
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters of the get these rows were visited for
    /// * `rows` - The archived rows this gets selection visited, in sort order
    /// * `found` - Where to record the rows this partition answers with
    fn collect_archived<'a, P, I>(params: &SortedGet<R>, rows: I, found: &mut RowSink<'a, P>)
    where
        P: ShoalProjection<Row = R>,
        I: Iterator<Item = &'a <R as Archive>::Archived>,
        <R as Archive>::Archived: 'a,
    {
        hotpath::measure_block!("MaybeLoaded::collect_archived", {
            // ask once whether this projection is the row itself, instead of once per row
            let identity = P::ARCHIVED_IDENTITY;
            // visit the rows this get selected until we hold as many as it asked for
            for row in rows {
                // stop scanning once we hold every row this get asked for
                if params.limit_reached(found.len()) {
                    break;
                }
                // skip any rows that don't match our filter
                if let Some(filter) = &params.filters {
                    // check if this row should be filtered out
                    if !R::is_filtered_archived(filter, row) {
                        // skip this row since it doesn't match our filter
                        continue;
                    }
                }
                // answer with the fields this get asked for, straight out of the archive
                //
                // an unprojected get asks for the whole row, which is already in the layout the
                // wire wants, so point at it rather than materializing a copy to serialize back
                // into the same bytes. A projection is a strict subset of the rows fields, so
                // there is no archive of it to point at and it is built the way it always was
                match identity {
                    Some(_) => found.push_archived(row),
                    None => found.push_built(P::from_archived(row)),
                }
            }
        })
    }

    /// Check whether any of the archived rows a scan visited survives an exists filters
    ///
    /// This is the twin of [`Self::collect_archived`]. Nothing is deserialized, because an
    /// exists answers with a boolean and never returns a row.
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters of the exists these rows were visited for
    /// * `rows` - The archived rows this exists selection visited, in sort order
    fn any_archived<'a, I>(params: &SortedExists<R>, rows: I) -> bool
    where
        I: Iterator<Item = &'a <R as Archive>::Archived>,
        <R as Archive>::Archived: 'a,
    {
        // visit the rows this exists selected until one of them survives our filters
        for row in rows {
            // skip any rows that don't match our filter
            if let Some(filter) = &params.filters {
                // check if this row should be filtered out
                if !R::is_filtered_archived(filter, row) {
                    // skip this row since it doesn't match our filter
                    continue;
                }
            }
            // this archive holds one of the rows this exists asked about
            return true;
        }
        // this archive holds nothing that survived our filters
        false
    }

    /// Get some rows from this partition whether it is loaded or still an archive
    ///
    /// Rows are appended to `found`, which already holds everything the earlier
    /// partitions of this get contributed, so a limit spans the whole get instead of
    /// resetting at each partition.
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters to use to get the rows
    /// * `seek` - The archived keys of this get, built the first time one is needed
    /// * `found` - Where to record the rows this partition answers with
    pub fn get<'a, P: ShoalProjection<Row = R>>(
        &'a self,
        params: &'a SortedGet<R>,
        seek: &mut Option<SeekBytes<R::Sort>>,
        found: &mut RowSink<'a, P>,
    ) {
        // scan our rows however this partition happens to be held
        match self {
            // this partition is already in memory so scan it directly
            MaybeLoaded::Loaded { partition, .. } => partition.get(params, found),
            // this partition is only on disk, so read it where it lies
            //
            // only this arm is measured. the loaded arm above is already counted by
            // SortedPartition::get, and measuring both here would double count it.
            MaybeLoaded::Accessible(read) => hotpath::measure_block!("MaybeLoaded::get_archived", {
                // this partition came from disk so access it in place
                let access = read.archived();
                // put this gets keys in the form this archives keys are in, once per query
                let seek = seek.get_or_insert_with(|| SeekBytes::new(&params.sort_select));
                // visit the rows this get selected, however it chose to select them
                match &params.sort_select {
                    // this get asked for the whole archive, so walk it (tombstones skipped)
                    SortSelect::All => {
                        Self::collect_archived(params, access.live_row_values(), found);
                    }
                    // this get named its rows, so seek each of them instead of walking to it
                    SortSelect::Keys(_) => {
                        let rows = seek
                            .keys()
                            .filter_map(|raw| Self::seek_archived(access, raw));
                        Self::collect_archived(params, rows, found);
                    }
                    // this get bounded its rows, so seek to the lower bound and walk up
                    SortSelect::Range(range) => {
                        // a range that cannot contain a key holds no rows, and panics a seek
                        if range.is_empty() {
                            return;
                        }
                        let rows = Self::archived_rows_in_range(access, range, seek);
                        Self::collect_archived(params, rows, found);
                    }
                }
            }),
        }
    }

    /// Check if any of the rows this exists selected are in this partition
    ///
    /// This is the twin of [`Self::get`] and exists for the same reason: an archived
    /// partition has to be checked in place, so both of the ways a partition can be held
    /// need covering, and a table should not have to know which of the two it has.
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters to use to check for rows
    /// * `seek` - The archived keys of this exists, built the first time one is needed
    pub fn exists(&self, params: &SortedExists<R>, seek: &mut Option<SeekBytes<R::Sort>>) -> bool {
        // check our rows however this partition happens to be held
        match self {
            // this partition is already in memory so check it directly
            MaybeLoaded::Loaded { partition, .. } => partition.exists(params),
            MaybeLoaded::Accessible(read) => {
                // this partition came from disk so access it in place
                let access = read.archived();
                // put this exists keys in the form this archives keys are in, once per query
                let seek = seek.get_or_insert_with(|| SeekBytes::new(&params.sort_select));
                // visit the rows this exists selected, however it chose to select them
                match &params.sort_select {
                    // this exists asks about this archive and not about any row in particular
                    SortSelect::All => Self::any_archived(params, access.live_row_values()),
                    // this exists named its rows, so seek each of them instead of walking
                    SortSelect::Keys(_) => {
                        let rows = seek
                            .keys()
                            .filter_map(|raw| Self::seek_archived(access, raw));
                        Self::any_archived(params, rows)
                    }
                    // this exists bounded its rows, so seek to the lower bound and walk up
                    SortSelect::Range(range) => {
                        // a range that cannot contain a key holds no rows, and panics a seek
                        if range.is_empty() {
                            return false;
                        }
                        let rows = Self::archived_rows_in_range(access, range, seek);
                        Self::any_archived(params, rows)
                    }
                }
            }
        }
    }

    /// Iterate over the live rows of an archive whose sort key falls inside a range
    ///
    /// `ArchivedBTreeMap::range` descends to the lower bound and stops at the first key
    /// past the upper one, so this seeks in `log n` exactly the way the in memory scan
    /// does rather than walking the archive from the start.
    ///
    /// The caller has already checked that the range can contain a key.
    ///
    /// # Arguments
    ///
    /// * `access` - The archived partition to walk
    /// * `range` - The range of sort keys to walk
    /// * `seek` - The archived forms of this ranges bounds
    fn archived_rows_in_range<'a>(
        access: &'a ArchivedSortedPartition<R>,
        range: &SortRange<R::Sort>,
        seek: &SeekBytes<R::Sort>,
    ) -> impl Iterator<Item = &'a <R as Archive>::Archived> {
        // put both ends of this range in the form this archives keys are in
        let bounds = (
            Self::archived_bound(&range.start, seek.start()),
            Self::archived_bound(&range.end, seek.end()),
        );
        // seek to this ranges lower bound and walk in sort order until its upper one
        access
            .rows
            .range(bounds)
            .filter_map(|(_, row)| match row {
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
    use super::{MaybeLoaded, MaybeRow, SortedPartition, UnsortedPartition, ValidatedArchive};
    use crate::server::tables::persistent::sorted::replay_update;
    use crate::server::tables::persistent::RowSink;
    use crate::server::tables::persistent::unsorted::UnsortedIntents;
    use crate::shared::queries::parser::{FieldRole, TypeValidator};
    use crate::shared::rearchive::Rearchive;
    use crate::shared::row_ref::RowRef;
    use crate::shared::queries::{SortRange, SortSelect, SortedExists, SortedGet, SortedUpdate};
    use crate::shared::queries::{UnsortedGet, UnsortedUpdate};
    use crate::shared::traits::{
        PartitionKeySupport, RkyvSupport, ShoalProjection, ShoalSortedTable, ShoalTableSupport,
        ShoalUnsortedTable, TableSchemaSupport,
    };
    use crate::storage::{IntentReadSupport, RecoveryStats, ShouldPrune};
    use deepsize2::DeepSizeOf;
    use rkyv::util::AlignedVec;
    use rkyv::{Archive, Deserialize, Serialize};
    use std::collections::HashMap;
    use std::ops::Bound;

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
        // this row is only ever used in process, so its fingerprint only has to be distinct
        const SCHEMA_FINGERPRINT: u64 = 0x7e57_0001;

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
        type Projection = TestProjectionKind;

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

    /// The subsets of a test rows fields a get can be answered with
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Archive, Serialize, Deserialize)]
    enum TestProjectionKind {
        /// The whole row, which is what a get that names no projection asks for
        #[default]
        Full,
        /// The sort key alone, which is what `SortKeyOnly` returns
        SortKeyOnly,
    }

    impl RkyvSupport for TestProjectionKind {}


    /// What each field of a [`TestRow`] produced on its way back out of an archive
    ///
    /// This is written by hand here because the table derives cannot run inside `shoal-core` —
    /// they emit `::shoal::` paths, and this crate is underneath that facade. It is field for
    /// field what `shoal-derive` emits for a row of three `String` fields, so a mirror that
    /// stopped matching rkyv would fail these tests the same way it would fail a schema's.
    struct TestRowArchivedResolver {
        /// What `TestRow::partition_key` produced on its way out of the archive
        partition_key: <String as Rearchive>::ArchivedResolver,
        /// What `TestRow::sort_key` produced on its way out of the archive
        sort_key: <String as Rearchive>::ArchivedResolver,
        /// What `TestRow::data` produced on its way out of the archive
        data: <String as Rearchive>::ArchivedResolver,
    }

    /// A test row can be written back out of the archive it was read from
    impl Rearchive for TestRow {
        /// What this row's fields produced, which is not rkyv's resolver for this row
        type ArchivedResolver = TestRowArchivedResolver;

        /// Write every field's out of line data, straight out of the archive holding it
        ///
        /// # Arguments
        ///
        /// * `archived` - The archived row to write back out
        /// * `serializer` - The serializer to write the out of line data into
        fn serialize_archived<S>(
            archived: &<Self as Archive>::Archived,
            serializer: &mut S,
        ) -> Result<Self::ArchivedResolver, <S as rkyv::rancor::Fallible>::Error>
        where
            S: rkyv::rancor::Fallible + rkyv::ser::Writer + rkyv::ser::Allocator + ?Sized,
            <S as rkyv::rancor::Fallible>::Error: rkyv::rancor::Source,
        {
            Ok(TestRowArchivedResolver {
                partition_key: <String as Rearchive>::serialize_archived(&archived.partition_key, serializer)?,
                sort_key: <String as Rearchive>::serialize_archived(&archived.sort_key, serializer)?,
                data: <String as Rearchive>::serialize_archived(&archived.data, serializer)?,
            })
        }

        /// Write the fixed size row on top of what was serialized for its fields
        ///
        /// # Arguments
        ///
        /// * `archived` - The archived row being written back out
        /// * `resolver` - What each of its fields produced
        /// * `out` - Where the archived row belongs
        fn resolve_archived(
            archived: &<Self as Archive>::Archived,
            resolver: Self::ArchivedResolver,
            out: rkyv::Place<<Self as Archive>::Archived>,
        ) {
            // split the place the row belongs in into one place per field
            rkyv::munge::munge!(let ArchivedTestRow { partition_key, sort_key, data } = out);
            <String as Rearchive>::resolve_archived(&archived.partition_key, resolver.partition_key, partition_key);
            <String as Rearchive>::resolve_archived(&archived.sort_key, resolver.sort_key, sort_key);
            <String as Rearchive>::resolve_archived(&archived.data, resolver.data, data);
        }
    }


    /// Read the keys naming a row out of whichever form a sink pointed at
    ///
    /// A sink answers with a [`RowRef`] rather than a row, because a row still in the archive its
    /// partition was read from is not a row — it is `Archived<Row>`, and the two only become
    /// interchangeable once they are being written to the wire. A test that wants to name the
    /// rows a scan returned has to read them from either form, so it asks for the key rather
    /// than reaching for the field.
    trait Keys<'a> {
        /// The partition this row belongs to
        fn partition_key(self) -> &'a str;

        /// The key this row is sorted by within its partition
        fn sort_key(self) -> &'a str;
    }

    impl<'a> Keys<'a> for RowRef<'a, TestRow> {
        /// The partition this row belongs to, wherever the row is living
        fn partition_key(self) -> &'a str {
            match self {
                RowRef::Resident(row) => row.partition_key.as_str(),
                RowRef::InArchive(archived) => archived.partition_key.as_str(),
            }
        }

        /// The key this row is sorted by, wherever the row is living
        fn sort_key(self) -> &'a str {
            match self {
                RowRef::Resident(row) => row.sort_key.as_str(),
                RowRef::InArchive(archived) => archived.sort_key.as_str(),
            }
        }
    }

    impl<'a> Keys<'a> for RowRef<'a, SortKeyOnly> {
        /// The partition the row this was projected from belonged to
        fn partition_key(self) -> &'a str {
            match self {
                RowRef::Resident(row) => row.partition_key.as_str(),
                RowRef::InArchive(archived) => archived.partition_key.as_str(),
            }
        }

        /// The key the row this was projected from was sorted by
        fn sort_key(self) -> &'a str {
            match self {
                RowRef::Resident(row) => row.sort_key.as_str(),
                RowRef::InArchive(archived) => archived.sort_key.as_str(),
            }
        }
    }

    /// A whole row is the identity projection of itself
    impl ShoalProjection for TestRow {
        type Row = TestRow;

        // this projection is only ever used in process, so its fingerprint only has to be distinct
        const SCHEMA_FINGERPRINT: u64 = 0x7e57_0002;

        const PROJECTION: TestProjectionKind = TestProjectionKind::Full;

        // a row is its own identity projection, the same thing the table derive emits - without
        // this the default takes over and every row is copied, which is safe and is exactly what
        // this constant exists to avoid
        const IDENTITY: Option<fn(&TestRow) -> &Self> = Some(|row| row);

        // and an archived row is its own identity projection too, which is what lets a get off
        // disk answer out of the archive it read rather than materializing a row per row
        const ARCHIVED_IDENTITY: Option<
            fn(&<TestRow as Archive>::Archived) -> &<Self as Archive>::Archived,
        > = Some(|row| row);

        fn from_row(row: &TestRow) -> Self {
            row.clone()
        }

        fn from_archived(row: &<TestRow as Archive>::Archived) -> Self {
            <TestRow as RkyvSupport>::deserialize(row).unwrap()
        }
    }

    /// A projection that drops everything but the keys naming a row
    ///
    /// This carries the partition key because every projection has to, and the sort key so a
    /// test can tell which rows a scan returned and in what order. What it leaves out is the
    /// payload, which is what a projection is for.
    #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, DeepSizeOf)]
    struct SortKeyOnly {
        /// The partition this row belonged to
        partition_key: String,
        /// The key this row was sorted by within its partition
        sort_key: String,
    }

    impl RkyvSupport for SortKeyOnly {}

    impl PartitionKeySupport for SortKeyOnly {
        type PartitionKey = String;

        fn name() -> &'static str {
            "SortKeyOnly"
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


    /// What each field of a [`SortKeyOnly`] produced on its way back out of an archive
    ///
    /// This is written by hand here because the table derives cannot run inside `shoal-core` —
    /// they emit `::shoal::` paths, and this crate is underneath that facade. It is field for
    /// field what `shoal-derive` emits for a row of two `String` fields, so a mirror that
    /// stopped matching rkyv would fail these tests the same way it would fail a schema's.
    struct SortKeyOnlyArchivedResolver {
        /// What `SortKeyOnly::partition_key` produced on its way out of the archive
        partition_key: <String as Rearchive>::ArchivedResolver,
        /// What `SortKeyOnly::sort_key` produced on its way out of the archive
        sort_key: <String as Rearchive>::ArchivedResolver,
    }

    /// A test row can be written back out of the archive it was read from
    impl Rearchive for SortKeyOnly {
        /// What this row's fields produced, which is not rkyv's resolver for this row
        type ArchivedResolver = SortKeyOnlyArchivedResolver;

        /// Write every field's out of line data, straight out of the archive holding it
        ///
        /// # Arguments
        ///
        /// * `archived` - The archived row to write back out
        /// * `serializer` - The serializer to write the out of line data into
        fn serialize_archived<S>(
            archived: &<Self as Archive>::Archived,
            serializer: &mut S,
        ) -> Result<Self::ArchivedResolver, <S as rkyv::rancor::Fallible>::Error>
        where
            S: rkyv::rancor::Fallible + rkyv::ser::Writer + rkyv::ser::Allocator + ?Sized,
            <S as rkyv::rancor::Fallible>::Error: rkyv::rancor::Source,
        {
            Ok(SortKeyOnlyArchivedResolver {
                partition_key: <String as Rearchive>::serialize_archived(&archived.partition_key, serializer)?,
                sort_key: <String as Rearchive>::serialize_archived(&archived.sort_key, serializer)?,
            })
        }

        /// Write the fixed size row on top of what was serialized for its fields
        ///
        /// # Arguments
        ///
        /// * `archived` - The archived row being written back out
        /// * `resolver` - What each of its fields produced
        /// * `out` - Where the archived row belongs
        fn resolve_archived(
            archived: &<Self as Archive>::Archived,
            resolver: Self::ArchivedResolver,
            out: rkyv::Place<<Self as Archive>::Archived>,
        ) {
            // split the place the row belongs in into one place per field
            rkyv::munge::munge!(let ArchivedSortKeyOnly { partition_key, sort_key } = out);
            <String as Rearchive>::resolve_archived(&archived.partition_key, resolver.partition_key, partition_key);
            <String as Rearchive>::resolve_archived(&archived.sort_key, resolver.sort_key, sort_key);
        }
    }

    impl ShoalProjection for SortKeyOnly {
        type Row = TestRow;

        // this projection is only ever used in process, so its fingerprint only has to be distinct
        const SCHEMA_FINGERPRINT: u64 = 0x7e57_0003;

        const PROJECTION: TestProjectionKind = TestProjectionKind::SortKeyOnly;

        fn from_row(row: &TestRow) -> Self {
            SortKeyOnly {
                partition_key: row.partition_key.clone(),
                sort_key: row.sort_key.clone(),
            }
        }

        fn from_archived(row: &<TestRow as Archive>::Archived) -> Self {
            // read only the two keys out of the archive, leaving the payload where it is
            let mut pool = rkyv::de::Pool::new();
            SortKeyOnly {
                partition_key: rkyv::api::deserialize_using::<String, _, rkyv::rancor::Error>(
                    &row.partition_key,
                    &mut pool,
                )
                .unwrap(),
                sort_key: rkyv::api::deserialize_using::<String, _, rkyv::rancor::Error>(
                    &row.sort_key,
                    &mut pool,
                )
                .unwrap(),
            }
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

    /// A row is either sorted or unsorted in a real schema, but implementing both
    /// here lets the same fixture cover an unsorted partition without a second copy
    /// of every supporting trait.
    impl ShoalUnsortedTable for TestRow {
        fn update(&mut self, update: &UnsortedUpdate<Self>) {
            self.data = update.update.clone();
        }
    }

    /// Apply a batch of intents to an unsorted partition map and report the counts
    ///
    /// # Arguments
    ///
    /// * `loaded` - The partitions to apply these intents over
    /// * `intents` - The intents to apply
    fn apply_unsorted(
        loaded: &mut HashMap<u64, UnsortedPartition<TestRow>>,
        intents: Vec<UnsortedIntents<TestRow>>,
    ) -> (ShouldPrune, RecoveryStats) {
        // start this batch with nothing discarded
        let mut stats = RecoveryStats::default();
        // apply every intent to the one partition these tests use
        let prune = UnsortedPartition::<TestRow>::apply_intents(loaded, 0, intents, &mut stats);
        (prune, stats)
    }

    #[test]
    /// Compacting an update whose partition is gone counts it as data we lost
    fn apply_intents_counts_an_orphaned_update() {
        // no archive copy and no insert, so this update has nothing to land on
        let mut loaded = HashMap::new();
        let (prune, stats) = apply_unsorted(
            &mut loaded,
            vec![UnsortedIntents::Update(UnsortedUpdate {
                partition_key: 0,
                update: "updated".to_owned(),
            })],
        );
        // we ended with no row data, so this partition is pruned
        assert!(matches!(prune, ShouldPrune::Yes));
        // and the update we could not apply is counted as loss
        assert_eq!(stats.orphaned_updates, 1);
        assert!(!stats.is_clean());
    }

    #[test]
    /// Compacting an update onto a partition this batch deleted is not data loss
    ///
    /// A delete here drops the partition outright rather than tombstoning it, so
    /// without tracking the delete this looks identical to an orphaned update.
    fn apply_intents_separates_deleted_partitions_from_lost_ones() {
        // insert a partition, delete it, then update it - all in one batch
        let mut loaded = HashMap::new();
        let (prune, stats) = apply_unsorted(
            &mut loaded,
            vec![
                UnsortedIntents::Insert(TestRow::new("a")),
                UnsortedIntents::Delete { partition_key: 0 },
                UnsortedIntents::Update(UnsortedUpdate {
                    partition_key: 0,
                    update: "updated".to_owned(),
                }),
            ],
        );
        // the delete still wins, so this partition is pruned
        assert!(matches!(prune, ShouldPrune::Yes));
        // but the update it swallowed was meant to be dropped
        assert_eq!(stats.updates_after_delete, 1);
        assert_eq!(stats.orphaned_updates, 0);
        assert!(stats.is_clean());
    }

    #[test]
    /// An update applied over an existing partition counts as nothing
    fn apply_intents_counts_nothing_when_it_applies() {
        // start from a partition that is already on disk
        let mut loaded = HashMap::new();
        loaded.insert(0, UnsortedPartition::new(0, TestRow::new("a")));
        let (prune, stats) = apply_unsorted(
            &mut loaded,
            vec![UnsortedIntents::Update(UnsortedUpdate {
                partition_key: 0,
                update: "updated".to_owned(),
            })],
        );
        // we still hold live row data so nothing is pruned
        assert!(matches!(prune, ShouldPrune::No));
        // and the update landed, so this recovery discarded nothing
        assert_eq!(stats, RecoveryStats::default());
        let MaybeRow::Row(row) = &loaded.get(&0).unwrap().row else {
            panic!("our row was replaced by a tombstone");
        };
        assert_eq!(row.data, "updated");
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

    /// Build an update for one row of our test partition
    ///
    /// # Arguments
    ///
    /// * `sort_key` - The sort key of the row to update
    fn update_for(sort_key: &str) -> SortedUpdate<TestRow> {
        SortedUpdate {
            partition_key: 0,
            sort_key: sort_key.to_owned(),
            update: "updated".to_owned(),
        }
    }

    #[test]
    /// A replayed update that lands on a live row is applied and counted as nothing
    fn replay_update_applies_to_a_live_row() {
        // build a partition holding the row our update names
        let mut partition = SortedPartition::<TestRow>::new(0);
        partition.insert(TestRow::new("a"));
        // replay an update over it
        let mut stats = RecoveryStats::default();
        replay_update(&mut partition, &update_for("a"), &mut stats);
        // the update landed, so nothing was discarded
        assert_eq!(stats, RecoveryStats::default());
        // and the row carries it
        let MaybeRow::Row(row) = partition.rows.get(&"a".to_owned()).unwrap() else {
            panic!("our row was replaced by a tombstone");
        };
        assert_eq!(row.data, "updated");
    }

    #[test]
    /// A replayed update onto a deleted row is not counted as data loss
    ///
    /// `SortedPartition::update` answers `None` for a tombstoned row and for a row
    /// that was never there, and only one of those means anything went missing.
    fn replay_update_separates_deleted_rows_from_lost_ones() {
        // build a partition with one live row and one we then delete
        let mut partition = SortedPartition::<TestRow>::new(0);
        partition.insert(TestRow::new("a"));
        partition.remove(&"a".to_owned());
        // an update onto the row a delete already took is the delete working
        let mut stats = RecoveryStats::default();
        replay_update(&mut partition, &update_for("a"), &mut stats);
        assert_eq!(stats.updates_after_delete, 1);
        assert_eq!(stats.orphaned_updates, 0);
        // so this recovery has still not lost anything
        assert!(stats.is_clean());
        // an update onto a row that is simply not there is data we no longer have
        replay_update(&mut partition, &update_for("never-inserted"), &mut stats);
        assert_eq!(stats.updates_after_delete, 1);
        assert_eq!(stats.orphaned_updates, 1);
        // and that does make this recovery unclean
        assert!(!stats.is_clean());
    }

    /// Build a get for a partition with an optional limit
    ///
    /// # Arguments
    ///
    /// * `limit` - The limit to place on this get if it has one
    fn get_with_limit(limit: Option<usize>) -> SortedGet<TestRow> {
        SortedGet {
            partition_keys: vec![0],
            sort_select: SortSelect::All,
            filters: None,
            limit,
            projection: TestProjectionKind::Full,
        }
    }

    /// Build a partition holding a row for each of the given sort keys
    ///
    /// # Arguments
    ///
    /// * `sort_keys` - The sort keys to build rows for
    fn partition_of(sort_keys: &[&str]) -> SortedPartition<TestRow> {
        // build an empty partition to fill
        let mut partition = SortedPartition::<TestRow>::new(0);
        // add a row for each sort key we were given
        for sort_key in sort_keys {
            partition.insert(TestRow::new(sort_key));
        }
        partition
    }

    #[test]
    /// A limit stops a scan, and takes the first rows in sort order
    fn a_limit_stops_a_partition_scan() {
        // build a partition with five rows in it
        let partition = partition_of(&["a", "b", "c", "d", "e"]);
        // scan it with a limit of two
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_limit(Some(2));
        partition.get(&params, &mut found);
        // a limit takes the first rows in sort order, not an arbitrary two
        let sort_keys = found.iter().map(|row| row.sort_key()).collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["a", "b"]);
    }

    #[test]
    /// A limit spans every partition a get touches rather than resetting at each one
    ///
    /// The response vec is shared by every partition of a get, so a limit checked after
    /// the push instead of before hands back one extra row for every partition past the
    /// one that filled it. This fails with four rows against that version.
    fn a_limit_is_shared_across_partitions() {
        // build two partitions of three rows each
        let first = partition_of(&["a", "b", "c"]);
        let second = partition_of(&["d", "e", "f"]);
        // scan both of them into the same response vec with a limit of three, which is
        // exactly what the first partition holds
        let get = get_with_limit(Some(3));
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        first.get(&get, &mut found);
        second.get(&get, &mut found);
        // the second partition must not have added anything
        assert_eq!(found.len(), 3);
    }

    #[test]
    /// A scan handed an already full response vec adds nothing
    ///
    /// This is the shape a get takes when it resumes after a partition was read from
    /// disk: the rows it accumulated before it blocked are handed back to it, and its
    /// limit has to be counted against those and not just against this scan.
    fn a_full_found_vec_is_left_alone() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // pretend an earlier execution of this get already found its two rows
        let mut found = RowSink::default();
        found.push_built(TestRow::new("x"));
        found.push_built(TestRow::new("y"));
        let params = get_with_limit(Some(2));
        partition.get(&params, &mut found);
        // our already full response is untouched
        assert_eq!(found.len(), 2);
        let sort_keys = found.iter().map(|row| row.sort_key()).collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["x", "y"]);
    }

    #[test]
    /// A limit of zero scans nothing at all
    fn a_zero_limit_finds_no_rows() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // scan it with a limit of zero
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_limit(Some(0));
        partition.get(&params, &mut found);
        // a limit of zero is reached before a single row is read
        assert!(found.is_empty());
    }

    #[test]
    /// A get with no limit still returns every row
    fn a_get_with_no_limit_returns_every_row() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // scan it with no limit at all
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_limit(None);
        partition.get(&params, &mut found);
        // an unlimited get is never short circuited
        assert_eq!(found.len(), 3);
    }

    /// Build a get naming some sort keys, with an optional limit
    ///
    /// The keys are handed over in the order they are given, since a scan trusts that
    /// they were normalized upstream by `split_by_shard`.
    ///
    /// # Arguments
    ///
    /// * `sort_keys` - The sort keys this get should select
    /// * `limit` - The limit to place on this get if it has one
    fn get_with_sort_keys(sort_keys: &[&str], limit: Option<usize>) -> SortedGet<TestRow> {
        SortedGet {
            partition_keys: vec![0],
            sort_select: SortSelect::Keys(
                sort_keys.iter().map(|key| (*key).to_owned()).collect(),
            ),
            filters: None,
            limit,
            projection: TestProjectionKind::Full,
        }
    }

    /// Turn a get into one asking to be answered with the sort key projection
    ///
    /// A projection changes what a get comes back as and nothing about which rows it selects,
    /// so every test below builds its get the usual way and then projects it.
    ///
    /// # Arguments
    ///
    /// * `get` - The get to project
    fn projected(mut get: SortedGet<TestRow>) -> SortedGet<TestRow> {
        get.projection = TestProjectionKind::SortKeyOnly;
        get
    }

    /// Build a get bounding its rows by a range of sort keys
    ///
    /// # Arguments
    ///
    /// * `range` - The range of sort keys this get should select
    /// * `limit` - The limit to place on this get if it has one
    fn get_with_range(range: SortRange<String>, limit: Option<usize>) -> SortedGet<TestRow> {
        SortedGet {
            partition_keys: vec![0],
            sort_select: SortSelect::Range(range),
            filters: None,
            limit,
            projection: TestProjectionKind::Full,
        }
    }

    /// Build an exists bounding its rows by a range of sort keys
    ///
    /// # Arguments
    ///
    /// * `range` - The range of sort keys this exists should check
    fn exists_with_range(range: SortRange<String>) -> SortedExists<TestRow> {
        SortedExists {
            partition_keys: vec![0],
            sort_select: SortSelect::Range(range),
            filters: None,
        }
    }

    /// Build a range over a pair of borrowed sort keys
    ///
    /// # Arguments
    ///
    /// * `start` - The lower bound of the range to build
    /// * `end` - The upper bound of the range to build
    fn range_of(start: Bound<&str>, end: Bound<&str>) -> SortRange<String> {
        // owning the bound values is what a real query hands the table
        let owned = |bound: Bound<&str>| match bound {
            Bound::Included(key) => Bound::Included(key.to_owned()),
            Bound::Excluded(key) => Bound::Excluded(key.to_owned()),
            Bound::Unbounded => Bound::Unbounded,
        };
        SortRange::new(owned(start), owned(end))
    }

    /// Build an exists naming some sort keys
    ///
    /// # Arguments
    ///
    /// * `sort_keys` - The sort keys this exists should check for
    fn exists_with_sort_keys(sort_keys: &[&str]) -> SortedExists<TestRow> {
        SortedExists {
            partition_keys: vec![0],
            sort_select: SortSelect::Keys(
                sort_keys.iter().map(|key| (*key).to_owned()).collect(),
            ),
            filters: None,
        }
    }

    #[test]
    /// A named sort key selects its own row and nothing else
    ///
    /// This is the whole of the defect: the sort keys were carried to the table and
    /// never read, so this came back with all four rows.
    fn a_named_sort_key_selects_one_row() {
        // build a partition with four rows in it
        let partition = partition_of(&["a", "b", "c", "d"]);
        // ask for one of them by sort key
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_sort_keys(&["c"], None);
        partition.get(&params, &mut found);
        // only the row we named comes back
        let sort_keys = found
            .iter()
            .map(|row| row.sort_key())
            .collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["c"]);
    }

    #[test]
    /// Several named sort keys select each of their rows, in the order they were named
    fn named_sort_keys_select_their_rows() {
        // build a partition with five rows in it
        let partition = partition_of(&["a", "b", "c", "d", "e"]);
        // ask for two of them by sort key
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_sort_keys(&["b", "d"], None);
        partition.get(&params, &mut found);
        // both of the rows we named come back and nothing else does
        let sort_keys = found
            .iter()
            .map(|row| row.sort_key())
            .collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["b", "d"]);
    }

    #[test]
    /// A sort key naming no row in this partition finds nothing
    fn a_missing_sort_key_finds_nothing() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for a row this partition does not hold
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_sort_keys(&["z"], None);
        partition.get(&params, &mut found);
        // a miss is a miss, not the whole partition
        assert!(found.is_empty());
    }

    #[test]
    /// A get naming no sort keys still returns every row
    ///
    /// This pins the case that was already right, so a future selection cannot narrow a
    /// get that never asked to be narrowed.
    fn selecting_every_row_returns_every_row() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for the partition without narrowing it at all
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_limit(None);
        partition.get(&params, &mut found);
        // every row this partition holds comes back
        assert_eq!(found.len(), 3);
    }

    #[test]
    /// A get naming an empty set of sort keys names no rows
    ///
    /// This is the one behaviour `SortSelect` changed. An empty `sort_keys` list used to
    /// mean the whole partition, because it was the only way a get could say it had not
    /// narrowed itself. `SortSelect::All` says that now, so an empty set is a set with
    /// nothing in it and selects nothing - which is what "these rows" has always meant for
    /// every other set.
    fn an_empty_sort_key_selection_returns_no_rows() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for a set of rows without putting a single one in it
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_sort_keys(&[], None);
        partition.get(&params, &mut found);
        // a set naming no rows selects none of them
        assert!(found.is_empty());
    }

    #[test]
    /// A tombstoned sort key is a miss and not a row
    ///
    /// A tombstone shadows a row that may still be in an archive, so a lookup that
    /// lands on one has found a deleted row rather than a live one.
    fn a_tombstoned_sort_key_is_not_found() {
        // build a partition with rows in it and delete one of them
        let mut partition = partition_of(&["a", "b", "c"]);
        partition.remove(&"b".to_owned());
        // ask for the row we just deleted
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_sort_keys(&["b"], None);
        partition.get(&params, &mut found);
        // a deleted row is not returned by naming it
        assert!(found.is_empty());
    }

    #[test]
    /// A limit bounds a sort key selection like it bounds a scan
    fn a_limit_bounds_a_sort_key_selection() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c", "d"]);
        // name three of them but only allow two back
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_sort_keys(&["a", "b", "c"], Some(2));
        partition.get(&params, &mut found);
        // the limit takes the first rows we named
        let sort_keys = found
            .iter()
            .map(|row| row.sort_key())
            .collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["a", "b"]);
    }

    #[test]
    /// An exists naming a sort key answers for that row alone
    ///
    /// This is the sharper half of the defect: the old exists returned true on the
    /// first live row it walked, so it answered "does this partition hold anything"
    /// for a partition that does not hold the named row at all.
    fn exists_answers_for_a_named_sort_key() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // a row we hold exists
        assert!(partition.exists(&exists_with_sort_keys(&["b"])));
        // a row we do not hold does not, even though this partition is not empty
        assert!(!partition.exists(&exists_with_sort_keys(&["z"])));
    }

    #[test]
    /// An exists naming several sort keys is true if any of them exists
    fn exists_is_true_for_any_named_sort_key() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // one of these two rows is held and one is not
        assert!(partition.exists(&exists_with_sort_keys(&["z", "c"])));
        // neither of these are
        assert!(!partition.exists(&exists_with_sort_keys(&["y", "z"])));
    }

    #[test]
    /// An exists naming a deleted row is false
    fn exists_is_false_for_a_tombstoned_sort_key() {
        // build a partition with rows in it and delete one of them
        let mut partition = partition_of(&["a", "b", "c"]);
        partition.remove(&"b".to_owned());
        // the row we deleted no longer exists
        assert!(!partition.exists(&exists_with_sort_keys(&["b"])));
    }

    #[test]
    /// An exists selecting every row asks whether this partition holds any live row
    fn exists_selecting_every_row_asks_about_the_partition() {
        // build an exists that has not narrowed itself at all
        let whole_partition = SortedExists {
            partition_keys: vec![0],
            sort_select: SortSelect::All,
            filters: None,
        };
        // a partition with a live row in it holds something
        let mut partition = partition_of(&["a"]);
        assert!(partition.exists(&whole_partition));
        // a partition holding only a tombstone does not
        partition.remove(&"a".to_owned());
        assert!(!partition.exists(&whole_partition));
    }

    #[test]
    /// A range selects the rows between its bounds and no others
    ///
    /// This is the whole of the feature on the in memory side: a partition of n rows asked
    /// for a span of k of them seeks to the lower bound instead of walking to it.
    fn a_range_selects_its_rows() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c", "d", "e"]);
        // ask for the rows between two of them
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let range = range_of(Bound::Included("b"), Bound::Included("d"));
        let params = get_with_range(range, None);
        partition.get(&params, &mut found);
        // the rows inside the range come back, in sort order, and nothing else
        let keys: Vec<&str> = found.iter().map(|row| row.sort_key()).collect();
        assert_eq!(keys, vec!["b", "c", "d"]);
    }

    #[test]
    /// An excluded lower bound leaves out the row it names
    ///
    /// This is the bound paging is built on: the last row of a page is excluded so the next
    /// page starts after it rather than repeating it.
    fn an_excluded_lower_bound_skips_its_key() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for everything after the first row without including it
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let range = range_of(Bound::Excluded("a"), Bound::Unbounded);
        let params = get_with_range(range, None);
        partition.get(&params, &mut found);
        // the row the bound named is left out and the rest come back
        let keys: Vec<&str> = found.iter().map(|row| row.sort_key()).collect();
        assert_eq!(keys, vec!["b", "c"]);
    }

    #[test]
    /// An excluded upper bound leaves out the row it names, and an included one keeps it
    fn an_upper_bound_decides_whether_its_key_is_kept() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for everything up to but not including the last row
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let range = range_of(Bound::Unbounded, Bound::Excluded("c"));
        let params = get_with_range(range, None);
        partition.get(&params, &mut found);
        let keys: Vec<&str> = found.iter().map(|row| row.sort_key()).collect();
        assert_eq!(keys, vec!["a", "b"]);
        // asking for everything up to and including it keeps it
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let range = range_of(Bound::Unbounded, Bound::Included("c"));
        let params = get_with_range(range, None);
        partition.get(&params, &mut found);
        let keys: Vec<&str> = found.iter().map(|row| row.sort_key()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    /// A range bounded at neither end selects every row
    fn an_unbounded_range_returns_every_row() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for a range that bounds nothing
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_range(SortRange::default(), None);
        partition.get(&params, &mut found);
        // every row this partition holds comes back
        assert_eq!(found.len(), 3);
    }

    #[test]
    /// A range whose start is past its end selects nothing instead of panicking
    ///
    /// `BTreeMap::range` panics on this shape, so the guard in front of the seek is what
    /// this pins. Without it the scan takes the shard down rather than answering.
    fn an_inverted_range_returns_nothing() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for a range that runs backwards
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let range = range_of(Bound::Included("c"), Bound::Included("a"));
        let params = get_with_range(range, None);
        partition.get(&params, &mut found);
        // a range that cannot contain a key holds no rows
        assert!(found.is_empty());
    }

    #[test]
    /// A range over one key with an excluded end selects nothing instead of panicking
    ///
    /// The other shape `BTreeMap::range` panics on, and the easier of the two to write by
    /// accident - `title > 'm' AND title < 'm'` is one typo away from a valid query.
    fn an_empty_exclusive_range_returns_nothing() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for a range whose ends meet on a key neither of them includes
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let range = range_of(Bound::Excluded("b"), Bound::Excluded("b"));
        let params = get_with_range(range, None);
        partition.get(&params, &mut found);
        // there is nothing between a key and itself
        assert!(found.is_empty());
    }

    #[test]
    /// A tombstone inside a range is skipped rather than returned
    ///
    /// A range walks rows rather than seeking each of them, so it meets tombstones the way
    /// an unnarrowed scan does. Returning one resurrects a deleted row.
    fn a_range_skips_tombstones() {
        // build a partition with rows in it and delete one in the middle
        let mut partition = partition_of(&["a", "b", "c"]);
        partition.remove(&"b".to_owned());
        // ask for a range covering all three of them
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_range(SortRange::default(), None);
        partition.get(&params, &mut found);
        // the deleted row is not in the answer
        let keys: Vec<&str> = found.iter().map(|row| row.sort_key()).collect();
        assert_eq!(keys, vec!["a", "c"]);
    }

    #[test]
    /// A range stops as soon as its get holds every row it asked for
    ///
    /// This is what makes a page cost a page: the walk ends at the limit rather than at the
    /// upper bound, so a `LIMIT 20` over a range spanning a whole partition reads 20 rows.
    fn a_range_stops_at_its_limit() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c", "d", "e"]);
        // ask for an unbounded range with a limit well inside it
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_range(SortRange::default(), Some(2));
        partition.get(&params, &mut found);
        // the walk stopped at the limit rather than at the end of the range
        let keys: Vec<&str> = found.iter().map(|row| row.sort_key()).collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    /// A range that a get has already filled its limit from contributes nothing
    ///
    /// `found` is shared by every partition a get touches, so the limit check has to happen
    /// before the first push of a range and not only between its rows.
    fn a_full_get_takes_no_rows_from_a_range() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // hand the scan a response that already holds every row its get asked for
        let mut found = RowSink::default();
        found.push_built(TestRow::new("z"));
        let params = get_with_range(SortRange::default(), Some(1));
        partition.get(&params, &mut found);
        // nothing was added to an answer that was already complete
        assert_eq!(found.len(), 1);
    }

    #[test]
    /// An exists answers for a range the same way a get selects one
    fn exists_answers_for_a_range() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // a range holding one of our rows exists
        assert!(partition.exists(&exists_with_range(range_of(
            Bound::Included("b"),
            Bound::Included("b")
        ))));
        // a range past every row we hold does not
        assert!(!partition.exists(&exists_with_range(range_of(
            Bound::Excluded("c"),
            Bound::Unbounded
        ))));
    }

    #[test]
    /// An exists over a range of tombstones is false
    ///
    /// The rows are there in the tree, so an exists that did not skip them would answer for
    /// rows that were deleted.
    fn exists_is_false_for_a_range_of_tombstones() {
        // build a partition and delete every row in it
        let mut partition = partition_of(&["a", "b"]);
        partition.remove(&"a".to_owned());
        partition.remove(&"b".to_owned());
        // a range covering both of them holds nothing live
        assert!(!partition.exists(&exists_with_range(SortRange::default())));
    }

    #[test]
    /// An exists over an empty range is false rather than a panic
    fn exists_over_an_empty_range_is_false() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask about a range that cannot contain a key
        assert!(!partition.exists(&exists_with_range(range_of(
            Bound::Included("c"),
            Bound::Included("a")
        ))));
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

    #[test]
    /// A merged in disk copy is sized from the rows the merge ended up with
    ///
    /// Neither input's size describes the union, so leaving either one in place makes a
    /// partition that just grew report that it shrank - which is what floored shard
    /// memory usage at zero in the load path.
    fn merging_from_disk_recomputes_size() {
        // build the copy of this partition that an archive would hold
        let mut disk = SortedPartition::<TestRow>::new(0);
        disk.insert(TestRow::new("a"));
        // build a larger in memory copy holding rows the archive has never seen
        let mut memory = SortedPartition::<TestRow>::new(0);
        memory.insert(TestRow::new("b"));
        memory.insert(TestRow::new("c"));
        memory.insert(TestRow::new("d"));
        // the disk copy has to be the smaller of the two for this to mean anything
        let memory_size = memory.size;
        assert!(disk.size < memory_size);
        // merge the disk copy in
        memory.merge_from_disk(disk);
        // our size has to be the sum of every live row we ended up holding
        let expected = memory
            .rows
            .values()
            .filter_map(|row| match row {
                MaybeRow::Row(row) => Some(row.deep_size_of()),
                MaybeRow::Tombstone => None,
            })
            .sum::<usize>();
        assert_eq!(memory.size, expected);
        // a merge is a union so it can never leave us smaller than we already were
        assert!(memory.size > memory_size);
    }

    #[test]
    /// A tombstone that survives a merge contributes nothing to the merged size
    fn merging_from_disk_sizes_only_live_rows() {
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
        // only the archived row our tombstone is not shadowing counts towards our size
        let live = match memory.rows.get(&"b".to_owned()) {
            Some(MaybeRow::Row(row)) => row.deep_size_of(),
            _ => panic!("the archived row we never touched should have survived"),
        };
        assert_eq!(memory.size, live);
    }

    #[test]
    /// A projected scan of the whole partition answers with the projection
    ///
    /// The scan is generic in what it builds, so this is the test that a projection reaches
    /// it at all: the rows come back as `SortKeyOnly` and never as `TestRow`.
    fn a_projected_scan_returns_the_projection() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for the whole partition as the projection
        let mut found: RowSink<'_, SortKeyOnly> = RowSink::default();
        let params = projected(get_with_limit(None));
        partition.get(&params, &mut found);
        // every row comes back, projected, in sort order
        let sort_keys = found
            .iter()
            .map(|row| row.sort_key())
            .collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["a", "b", "c"]);
    }

    #[test]
    /// A resident scan that named no projection copies no rows at all
    ///
    /// This is [O2](../../../docs/src/appendix/optimizations.md)'s claim stated as a count, and
    /// so checked as one. The entry was that every returned row is copied at least twice: once
    /// out of the partition and once into the reply. A get that named no projection and read a
    /// resident partition now copies it **zero** times on the way out - the reply is serialized
    /// straight from the rows the partition is holding - so the only copy left is the one into
    /// the buffer the client is sent, which no amount of work removes.
    ///
    /// A projection is the control. It is a strict subset of its row and has to be built, so it
    /// builds every row it returns, and seeing that number move to three is what says this test
    /// is counting the thing it means to.
    fn a_resident_unprojected_scan_copies_no_rows() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c"]);
        // ask for the whole partition, as whole rows
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_limit(None);
        partition.get(&params, &mut found);
        // every row came back
        assert_eq!(found.len(), 3);
        // and not one of them was copied to do it
        assert_eq!(
            found.built(),
            0,
            "a resident get that named no projection copied rows it could have pointed at"
        );
        // the same scan asking for a projection has to build what it answers with
        let mut projected_rows: RowSink<'_, SortKeyOnly> = RowSink::default();
        let params = projected(get_with_limit(None));
        partition.get(&params, &mut projected_rows);
        assert_eq!(projected_rows.len(), 3);
        assert_eq!(
            projected_rows.built(),
            3,
            "a projection is a subset of its row and cannot be answered where the row lies"
        );
    }

    #[test]
    /// An archived scan points at every row it returns, which is the other half of O2
    ///
    /// What an accessible partition holds is `Archived<T>` rather than `T`, so these rows cannot
    /// be pointed at the way resident ones are — they are written back out of the archive by the
    /// mirror [F28](../../../docs/src/features/rearchived-rows.md) generates. This is the count
    /// that says the mirror is actually *reached*: both paths answer with the same rows, so
    /// nothing else in this file would notice if a get quietly went back to materializing them.
    fn an_archived_scan_points_at_every_row_it_returns() {
        // hold a partition as the archive an evicted one is
        let partition = accessible(&["a", "b", "c"]);
        // ask for all of it, as whole rows, with no projection to blame
        let mut seek = None;
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        let params = get_with_limit(None);
        partition.get(&params, &mut seek, &mut found);
        // every row came back, and not one of them was materialized to do it
        assert_eq!(found.len(), 3);
        assert_eq!(
            found.built(),
            0,
            "an archived get materialized rows it could have written out of the archive"
        );
        // and the rows it points at are the rows the archive holds, in sort order
        let sort_keys = found.iter().map(Keys::sort_key).collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["a", "b", "c"]);
    }

    #[test]
    /// A projected archived scan still builds every row it returns
    ///
    /// A projection is a strict subset of its row's fields, so the archive holds no value of it
    /// to point at however the partition is being held. This is the guard on the constant that
    /// decides which of the two an archived scan does.
    fn a_projected_archived_scan_builds_every_row() {
        // hold a partition as the archive an evicted one is
        let partition = accessible(&["a", "b", "c"]);
        // ask for it as the projection instead of as whole rows
        let mut seek = None;
        let mut found: RowSink<'_, SortKeyOnly> = RowSink::default();
        let params = projected(get_with_limit(None));
        partition.get(&params, &mut seek, &mut found);
        // every row came back, and every one of them was built to do it
        assert_eq!(found.len(), 3);
        assert_eq!(
            found.built(),
            3,
            "a projection was answered out of an archive that holds no value of it"
        );
    }

    #[test]
    /// An unsorted archived get points at the row it answers with
    ///
    /// The unsorted twin of [`an_archived_scan_points_at_every_row_it_returns`]. An unsorted
    /// partition holds a single row, so the same claim is one row rather than a scan of them.
    fn an_unsorted_archived_get_points_at_its_row() {
        // hold a one row partition as the archive an evicted one is
        let partition = unsorted_accessible(&UnsortedPartition::new(0, TestRow::new("a")));
        // ask for it as a whole row, with no projection to blame
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        assert!(partition.get(&unsorted_get(None), &mut found));
        // the row came back, and it was not materialized to do it
        assert_eq!(found.len(), 1);
        assert_eq!(
            found.built(),
            0,
            "an archived unsorted get materialized the row it could have pointed at"
        );
        assert_eq!(found.iter().next().unwrap().sort_key(), "a");
    }

    #[test]
    /// A projected get still carries the partition its rows came from
    ///
    /// The shard collecting the shares of a split get asks each row which partition it came
    /// from, so a projection that dropped that key would come back shuffled.
    fn a_projected_row_still_names_its_partition() {
        // build a partition with a row in it
        let partition = partition_of(&["a"]);
        // ask for it as the projection
        let mut found: RowSink<'_, SortKeyOnly> = RowSink::default();
        let params = projected(get_with_limit(None));
        partition.get(&params, &mut found);
        // the projected row names the partition its row was in
        assert_eq!(found.iter().next().unwrap().partition_key(), "partition");
    }

    #[test]
    /// A projected get naming sort keys seeks the rows it named, like an unprojected one
    fn a_projected_sort_key_selection_seeks_its_rows() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c", "d"]);
        // name two of them and ask for the projection
        let mut found: RowSink<'_, SortKeyOnly> = RowSink::default();
        let params = projected(get_with_sort_keys(&["b", "d"], None));
        partition.get(&params, &mut found);
        // the rows we named come back and no others
        let sort_keys = found
            .iter()
            .map(|row| row.sort_key())
            .collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["b", "d"]);
    }

    #[test]
    /// A projected get bounded by a range walks the same span an unprojected one does
    fn a_projected_range_bounds_its_rows() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c", "d", "e"]);
        // bound it at both ends and ask for the projection
        let range = SortRange::new(
            Bound::Excluded("b".to_owned()),
            Bound::Included("d".to_owned()),
        );
        let mut found: RowSink<'_, SortKeyOnly> = RowSink::default();
        let params = projected(get_with_range(range, None));
        partition.get(&params, &mut found);
        // only the rows inside the range come back
        let sort_keys = found
            .iter()
            .map(|row| row.sort_key())
            .collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["c", "d"]);
    }

    #[test]
    /// A limit stops a projected scan where it stops an unprojected one
    ///
    /// The limit is counted against what a get has found rather than against what it read, so
    /// a projected get fills its limit with the same number of rows a whole row get does.
    fn a_limit_bounds_a_projected_scan() {
        // build a partition with rows in it
        let partition = partition_of(&["a", "b", "c", "d"]);
        // ask for the projection but only allow two rows back
        let mut found: RowSink<'_, SortKeyOnly> = RowSink::default();
        let params = projected(get_with_limit(Some(2)));
        partition.get(&params, &mut found);
        // the limit takes the first rows in sort order
        let sort_keys = found
            .iter()
            .map(|row| row.sort_key())
            .collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["a", "b"]);
    }

    #[test]
    /// A projected scan skips a tombstone the same way an unprojected one does
    fn a_projected_scan_skips_tombstones() {
        // build a partition with rows in it and delete one of them
        let mut partition = partition_of(&["a", "b", "c"]);
        partition.remove(&"b".to_owned());
        // ask for the whole partition as the projection
        let mut found: RowSink<'_, SortKeyOnly> = RowSink::default();
        let params = projected(get_with_limit(None));
        partition.get(&params, &mut found);
        // the deleted row is not projected into the answer
        let sort_keys = found
            .iter()
            .map(|row| row.sort_key())
            .collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["a", "c"]);
    }

    #[test]
    /// A projection built from an archived row reads only the fields it named
    ///
    /// This is the conversion the archived scan uses, and the reason a projection is cheaper
    /// than the get it replaces: the payload is left in the archive rather than deserialized.
    fn a_projection_reads_an_archived_row() {
        // archive a row the way a partition on disk holds one
        let row = TestRow::new("a");
        let archived_bytes = <TestRow as RkyvSupport>::serialize(&row);
        let archived = <TestRow as RkyvSupport>::access(&archived_bytes).unwrap();
        // project it straight out of the archive
        let projection = <SortKeyOnly as ShoalProjection>::from_archived(archived);
        // both of the fields it named came across
        assert_eq!(projection.sort_key, "a");
        assert_eq!(projection.partition_key, "partition");
    }

    /// Hold a partition the way an evicted one is held after it is read back
    ///
    /// The buffer is an [`AlignedVec`] rather than the glommio `ReadResult` a real read
    /// produces, because a `ReadResult` can only come from a live reactor. That is why
    /// [`MaybeLoaded`] carries its buffer as a type parameter: without it none of the tests
    /// below could exist, and the archived arm of every scan was unreachable from here.
    ///
    /// # Arguments
    ///
    /// * `sort_keys` - The sort keys to build rows for
    fn accessible(sort_keys: &[&str]) -> MaybeLoaded<SortedPartition<TestRow>, AlignedVec> {
        // archive the partition the way a compaction would have written it
        let raw = <SortedPartition<TestRow> as RkyvSupport>::serialize(&partition_of(sort_keys));
        // hold it as an archive rather than as rows, validated the way a load validates it
        MaybeLoaded::Accessible(ValidatedArchive::new(raw).unwrap())
    }

    /// Archive a partition the way a compaction would have written it
    ///
    /// # Arguments
    ///
    /// * `sort_keys` - The sort keys to build rows for
    fn archived_of(sort_keys: &[&str]) -> AlignedVec {
        <SortedPartition<TestRow> as RkyvSupport>::serialize(&partition_of(sort_keys))
    }

    /// Copy an archive, keeping its alignment, so a test can damage it
    ///
    /// A plain `Vec<u8>` would not do - the unchecked read alignment checks in a debug build
    /// and the validator rejects a misaligned buffer, so a corruption test built on one would
    /// pass for the wrong reason.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to copy
    fn aligned_copy(bytes: &[u8]) -> AlignedVec {
        let mut copy = AlignedVec::new();
        copy.extend_from_slice(bytes);
        copy
    }

    #[test]
    /// A truncated archive is rejected rather than held as a validated one
    ///
    /// Truncation rather than a flipped byte, because a flip usually lands in a payload the
    /// validator has no opinion about and would pass - which would make this test green for
    /// a reason that has nothing to do with validation running.
    fn new_rejects_a_truncated_archive() {
        // archive a partition and take a byte off the end of it
        let raw = archived_of(&["a", "b", "c"]);
        let truncated = aligned_copy(&raw[..raw.len() - 1]);
        // the bytes no longer describe a partition, so there is no validated form of them
        assert!(ValidatedArchive::<SortedPartition<TestRow>, _>::new(truncated).is_err());
        // and the archive it came from is still fine
        assert!(ValidatedArchive::<SortedPartition<TestRow>, _>::new(raw).is_ok());
    }

    #[test]
    /// An archive whose root has been overwritten is rejected
    ///
    /// This is the corruption that matters most, since the root is what an unchecked read
    /// would follow straight into a bad pointer.
    fn new_rejects_a_corrupt_root_pointer() {
        // archive a partition and zero the region its root sits in
        let raw = archived_of(&["a", "b", "c"]);
        let mut corrupt = aligned_copy(&raw);
        let len = corrupt.len();
        for byte in &mut corrupt[len - 8..] {
            *byte = 0xff;
        }
        // a root that points nowhere is caught before anything can follow it
        assert!(ValidatedArchive::<SortedPartition<TestRow>, _>::new(corrupt).is_err());
    }

    #[test]
    /// A validated archive reads back exactly what a checked access returns
    ///
    /// This is the test for the whole unchecked read: `archived` claims to be the second
    /// half of the `access` it replaced, and this is what says so. If rkyv ever changes
    /// where a root sits, this breaks rather than the read quietly returning nonsense.
    fn archived_is_the_same_reference_access_returns() {
        // archive a partition and hold it as a validated one
        let raw = archived_of(&["a", "b", "c"]);
        let checked = <SortedPartition<TestRow> as RkyvSupport>::access(&raw).unwrap() as *const _;
        let validated = ValidatedArchive::<SortedPartition<TestRow>, _>::new(raw).unwrap();
        // both routes land on the same place in the same bytes
        assert!(std::ptr::eq(validated.archived() as *const _, checked));
        // and the archive reports the bytes it was built from
        assert_eq!(validated.archived().rows.len(), 3);
    }

    /// Hold the same rows in memory, which is the other way a partition can be held
    ///
    /// # Arguments
    ///
    /// * `sort_keys` - The sort keys to build rows for
    fn resident(sort_keys: &[&str]) -> MaybeLoaded<SortedPartition<TestRow>, AlignedVec> {
        MaybeLoaded::Loaded {
            partition: partition_of(sort_keys),
            generation: 0,
        }
    }

    /// Name the rows a scan returned, in the order it returned them
    ///
    /// # Arguments
    ///
    /// * `found` - The rows a scan returned
    fn sort_keys_of(found: &[TestRow]) -> Vec<&str> {
        found.iter().map(|row| row.sort_key.as_str()).collect()
    }

    /// Run a get against a partition however it happens to be held
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition to scan
    /// * `get` - The get to run against it
    fn archived_get(
        partition: &MaybeLoaded<SortedPartition<TestRow>, AlignedVec>,
        get: &SortedGet<TestRow>,
    ) -> Vec<TestRow> {
        // a seek is built once per execution rather than once per partition
        let mut seek = None;
        let mut found = RowSink::default();
        partition.get(get, &mut seek, &mut found);
        found.into_owned()
    }

    #[test]
    /// A get selecting every row walks a whole archive
    fn an_accessible_get_returns_every_row() {
        // hold a partition as the archive an evicted one is
        let partition = accessible(&["a", "b", "c"]);
        // ask for all of it
        let found = archived_get(&partition, &get_with_limit(None));
        // every row came back, in sort order
        assert_eq!(sort_keys_of(&found), vec!["a", "b", "c"]);
    }

    #[test]
    /// A named sort key is sought in an archive rather than walked to
    fn an_accessible_get_seeks_a_named_sort_key() {
        let partition = accessible(&["a", "b", "c"]);
        // a key this archive holds comes back on its own
        let found = archived_get(&partition, &get_with_sort_keys(&["b"], None));
        assert_eq!(sort_keys_of(&found), vec!["b"]);
        // a key it does not hold finds nothing rather than the nearest row
        let missing = archived_get(&partition, &get_with_sort_keys(&["z"], None));
        assert!(missing.is_empty());
    }

    #[test]
    /// A range bounds the rows an archived scan visits
    fn an_accessible_get_bounds_a_range() {
        let partition = accessible(&["a", "b", "c", "d", "e"]);
        // an exclusive lower bound skips its own key and an inclusive upper keeps its
        let range = range_of(Bound::Excluded("b"), Bound::Included("d"));
        let found = archived_get(&partition, &get_with_range(range, None));
        assert_eq!(sort_keys_of(&found), vec!["c", "d"]);
        // a range that cannot hold a key returns nothing rather than panicking the seek
        let inverted = range_of(Bound::Included("d"), Bound::Excluded("b"));
        let none = archived_get(&partition, &get_with_range(inverted, None));
        assert!(none.is_empty());
    }

    #[test]
    /// A limit is shared across the archived partitions of one get
    fn an_accessible_get_shares_its_limit() {
        // two archived partitions, scanned into the same answer
        let first = accessible(&["a", "b"]);
        let second = accessible(&["c", "d"]);
        let get = get_with_limit(Some(3));
        let mut seek = None;
        let mut found = RowSink::default();
        first.get(&get, &mut seek, &mut found);
        second.get(&get, &mut seek, &mut found);
        // the second partition contributed only what was left of the limit
        assert_eq!(sort_keys_of(&found.into_owned()), vec!["a", "b", "c"]);
    }

    #[test]
    /// An archived partition answers every selection the way a resident one does
    ///
    /// This is the contract the archived path is actually held to: which of the two ways a
    /// partition happens to be held is an implementation detail of eviction, and a query
    /// cannot be allowed to notice it. Every other test here checks one arm; this one checks
    /// that the arms agree.
    fn an_accessible_and_a_loaded_partition_agree() {
        let sort_keys = ["a", "b", "c", "d", "e"];
        let archived = accessible(&sort_keys);
        let loaded = resident(&sort_keys);
        // every way a get can select rows, run against both
        let gets = [
            get_with_limit(None),
            get_with_limit(Some(2)),
            get_with_sort_keys(&["b", "d"], None),
            get_with_sort_keys(&["z"], None),
            get_with_range(range_of(Bound::Included("b"), Bound::Excluded("d")), None),
            get_with_range(range_of(Bound::Unbounded, Bound::Unbounded), None),
        ];
        for get in &gets {
            assert_eq!(
                sort_keys_of(&archived_get(&archived, get)),
                sort_keys_of(&archived_get(&loaded, get)),
            );
        }
        // and every way an exists can ask about them
        let checks = [
            exists_with_sort_keys(&["c"]),
            exists_with_sort_keys(&["z"]),
            exists_with_range(range_of(Bound::Included("b"), Bound::Excluded("d"))),
            exists_with_range(range_of(Bound::Included("y"), Bound::Excluded("z"))),
        ];
        for check in &checks {
            let mut archived_seek = None;
            let mut loaded_seek = None;
            assert_eq!(
                archived.exists(check, &mut archived_seek),
                loaded.exists(check, &mut loaded_seek),
            );
        }
    }

    #[test]
    /// An exists answers from an archive without deserializing anything
    fn an_accessible_exists_answers_for_a_named_key() {
        let partition = accessible(&["a", "b", "c"]);
        // a key this archive holds is there
        let mut seek = None;
        assert!(partition.exists(&exists_with_sort_keys(&["b"]), &mut seek));
        // one it does not hold is not
        let mut seek = None;
        assert!(!partition.exists(&exists_with_sort_keys(&["z"]), &mut seek));
        // and a range past the end of it holds nothing
        let mut seek = None;
        let past_the_end = exists_with_range(range_of(Bound::Included("y"), Bound::Unbounded));
        assert!(!partition.exists(&past_the_end, &mut seek));
    }

    #[test]
    /// A projected get reads its fields straight out of an archive
    fn a_projected_accessible_get_returns_the_projection() {
        let partition = accessible(&["a", "b", "c"]);
        // ask to be answered with the keys alone rather than with whole rows
        let mut seek = None;
        let mut found: RowSink<'_, SortKeyOnly> = RowSink::default();
        let params = projected(get_with_limit(None));
        partition.get(&params, &mut seek, &mut found);
        // every row was projected, and the projection kept its keys
        let sort_keys = found
            .iter()
            .map(|row| row.sort_key())
            .collect::<Vec<_>>();
        assert_eq!(sort_keys, vec!["a", "b", "c"]);
        assert_eq!(found.iter().next().unwrap().partition_key(), "partition");
    }

    #[test]
    /// An archived partition is charged for the bytes it holds
    ///
    /// This is what the memory counter is incremented by when a partition is read off disk,
    /// so it has to be the length of the buffer rather than the size of the rows in it.
    fn an_accessible_partition_reports_its_byte_size() {
        // archive a partition and hold it both ways
        let raw = <SortedPartition<TestRow> as RkyvSupport>::serialize(&partition_of(&["a", "b"]));
        let len = raw.len();
        let partition: MaybeLoaded<SortedPartition<TestRow>, AlignedVec> =
            MaybeLoaded::Accessible(ValidatedArchive::new(raw).unwrap());
        // an archive costs what it takes up, not what it would take up as rows
        assert_eq!(partition.size(), len);
        // and it can always be evicted, since dropping it loses nothing
        assert!(partition.is_evictable(0));
    }

    /// Hold an unsorted partition as the archive an evicted one is
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition to archive
    fn unsorted_accessible(
        partition: &UnsortedPartition<TestRow>,
    ) -> MaybeLoaded<UnsortedPartition<TestRow>, AlignedVec> {
        let raw = <UnsortedPartition<TestRow> as RkyvSupport>::serialize(partition);
        MaybeLoaded::Accessible(ValidatedArchive::new(raw).unwrap())
    }

    /// Build a get for an unsorted partition
    ///
    /// # Arguments
    ///
    /// * `limit` - The limit to place on this get if it has one
    fn unsorted_get(limit: Option<usize>) -> UnsortedGet<TestRow> {
        UnsortedGet {
            partition_keys: vec![0],
            filters: None,
            limit,
            projection: TestProjectionKind::Full,
        }
    }

    #[test]
    /// An archived unsorted partition returns the one row it holds
    fn an_accessible_unsorted_get_returns_its_row() {
        // hold a one row partition as an archive
        let partition = unsorted_accessible(&UnsortedPartition::new(0, TestRow::new("a")));
        // ask for it
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        assert!(partition.get(&unsorted_get(None), &mut found));
        assert_eq!(sort_keys_of(&found.into_owned()), vec!["a"]);
    }

    #[test]
    /// An archived tombstone is still a tombstone, and answers no get
    fn an_accessible_unsorted_tombstone_is_tombstoned() {
        // hold the tombstone of a deleted partition as an archive
        let partition = unsorted_accessible(&UnsortedPartition::tombstone(0));
        assert!(partition.is_tombstoned());
        // a deleted row has nothing to return
        let mut found: RowSink<'_, TestRow> = RowSink::default();
        assert!(!partition.get(&unsorted_get(None), &mut found));
        assert!(found.is_empty());
        // and a live one is not a tombstone
        let live = unsorted_accessible(&UnsortedPartition::new(0, TestRow::new("a")));
        assert!(!live.is_tombstoned());
    }

    #[test]
    /// Updating an archived unsorted partition deserializes it for the caller to swap in
    fn an_accessible_unsorted_update_deserializes() {
        // hold a one row partition as an archive
        let mut partition = unsorted_accessible(&UnsortedPartition::new(0, TestRow::new("a")));
        // update it, which an archive cannot be done in place
        let updated = partition
            .update(&UnsortedUpdate {
                partition_key: 0,
                update: "updated".to_owned(),
            })
            .expect("an archived partition has to be deserialized to be updated");
        // the row that came back carries the update
        let MaybeRow::Row(row) = &updated.row else {
            panic!("our row was replaced by a tombstone");
        };
        assert_eq!(row.data, "updated");
        // and an already loaded partition is updated in place instead
        let mut loaded = MaybeLoaded::<UnsortedPartition<TestRow>, AlignedVec>::Loaded {
            partition: UnsortedPartition::new(0, TestRow::new("a")),
            generation: 0,
        };
        assert!(loaded
            .update(&UnsortedUpdate {
                partition_key: 0,
                update: "updated".to_owned(),
            })
            .is_none());
    }
}
