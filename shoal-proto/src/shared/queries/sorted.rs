//! The query types for a sorted table in Shoal. This is a table where the
//! partitions contain multiple rows in a sorted structure.

use rkyv::{Archive, Deserialize, Serialize};
use std::ops::Bound;
use uuid::Uuid;

use crate::shared::queries::normalize_sort_keys;
use crate::shared::traits::{RkyvSupport, ShoalSortedTable};

/// Which rows of a partition a sorted get or exists is asking for
///
/// A sorted partition is a tree keyed by sort key, so there are exactly three useful
/// questions to ask it: all of it, some named rows, or a span of it. Each of those is an arm
/// here, which is what keeps "these keys *and* this range" from being a state the server has
/// to have an opinion about — a query arriving over the wire is deserialized straight into
/// its struct and never passes through a constructor that could have rejected it.
///
/// [`SortSelect::All`] is the only arm that means every row. `Keys(vec![])` names no rows and
/// so matches nothing, which is a change from the empty `sort_keys` list this replaced.
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub enum SortSelect<S> {
    /// Every row in the partition
    All,
    /// The rows named by these sort keys, in sort order and without repeats
    Keys(Vec<S>),
    /// The rows whose sort key falls inside this range
    Range(SortRange<S>),
}

impl<S> SortSelect<S> {
    /// Get the sort keys this selection named, if it named a set of them
    pub fn keys(&self) -> Option<&[S]> {
        // only a set of keys has keys to hand back
        match self {
            SortSelect::Keys(keys) => Some(keys),
            SortSelect::All | SortSelect::Range(_) => None,
        }
    }

    /// Get the range this selection bounded its rows by, if it bounded them
    pub fn range(&self) -> Option<&SortRange<S>> {
        // only a range has bounds to hand back
        match self {
            SortSelect::Range(range) => Some(range),
            SortSelect::All | SortSelect::Keys(_) => None,
        }
    }
}

impl<S: Ord + Clone> SortSelect<S> {
    /// Put this selection in the form the scans below expect
    ///
    /// A set of keys is sorted and deduplicated by [`normalize_sort_keys`], because the scans
    /// seek in the order they are given and do not check for repeats. The other two arms have
    /// nothing to normalize: a range is already an ordered pair, and every row is every row.
    ///
    /// This is done as a query enters the server rather than as it is built, for the same
    /// reason `group_by_shard` deduplicates partition keys there — it is the one place every
    /// query passes through, whoever built it.
    pub fn normalized(&self) -> Self {
        // put a set of keys in the order the rows they name come back in
        match self {
            SortSelect::Keys(keys) => SortSelect::Keys(normalize_sort_keys(keys)),
            SortSelect::All => SortSelect::All,
            SortSelect::Range(range) => SortSelect::Range(range.clone()),
        }
    }
}

/// `Default` for a selection, which is every row
///
/// This is written out rather than derived because `#[derive(Default)]` on a generic enum bounds
/// every one of its type parameters on `Default`, and a sort key has no reason to have one. The
/// arm this returns holds no `S` at all, so the bound would be asking for nothing.
#[allow(clippy::derivable_impls)]
impl<S> Default for SortSelect<S> {
    fn default() -> Self {
        SortSelect::All
    }
}

/// `Debug` for the archived form of a selection
///
/// This is written out rather than derived because rkyv's `derive(Debug)` emits an impl with
/// no bounds on it, and an archived selection is only printable when the archived form of
/// its sort key is. The generated per table query structs archive with `derive(Debug)`, so
/// something has to carry that obligation.
impl<S: Archive> std::fmt::Debug for ArchivedSortSelect<S>
where
    <Vec<S> as Archive>::Archived: std::fmt::Debug,
    <SortRange<S> as Archive>::Archived: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // print whichever way this selection chose its rows
        match self {
            ArchivedSortSelect::All => formatter.write_str("All"),
            ArchivedSortSelect::Keys(keys) => formatter.debug_tuple("Keys").field(keys).finish(),
            ArchivedSortSelect::Range(range) => {
                formatter.debug_tuple("Range").field(range).finish()
            }
        }
    }
}

/// A range of sort keys, bounding the rows a get or exists asks for
///
/// This is what makes paging over a large partition possible: an exclusive lower bound of the
/// last row of a page is a cursor onto the next one, so page *n* costs a seek plus its own
/// rows rather than every row before it.
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct SortRange<S> {
    /// The lower bound of this range
    pub start: Bound<S>,
    /// The upper bound of this range
    pub end: Bound<S>,
}

impl<S> SortRange<S> {
    /// Create a range from a pair of bounds
    ///
    /// # Arguments
    ///
    /// * `start` - The lower bound of this range
    /// * `end` - The upper bound of this range
    pub fn new(start: Bound<S>, end: Bound<S>) -> Self {
        SortRange { start, end }
    }

    /// Create a range covering every row after a key, not including it
    ///
    /// This is the cursor: hand it the sort key of the last row of a page and it names the
    /// next page.
    ///
    /// # Arguments
    ///
    /// * `key` - The sort key to start after
    pub fn after(key: S) -> Self {
        SortRange::new(Bound::Excluded(key), Bound::Unbounded)
    }

    /// Create a range covering every row from a key onwards, including it
    ///
    /// # Arguments
    ///
    /// * `key` - The sort key to start at
    pub fn starting_at(key: S) -> Self {
        SortRange::new(Bound::Included(key), Bound::Unbounded)
    }

    /// Create a range covering every row before a key, not including it
    ///
    /// # Arguments
    ///
    /// * `key` - The sort key to stop before
    pub fn before(key: S) -> Self {
        SortRange::new(Bound::Unbounded, Bound::Excluded(key))
    }

    /// Create a range covering every row up to a key, including it
    ///
    /// # Arguments
    ///
    /// * `key` - The sort key to stop at
    pub fn ending_at(key: S) -> Self {
        SortRange::new(Bound::Unbounded, Bound::Included(key))
    }

    /// Set the lower bound of this range
    ///
    /// # Arguments
    ///
    /// * `start` - The lower bound to set
    #[must_use]
    pub fn with_start(mut self, start: Bound<S>) -> Self {
        self.start = start;
        self
    }

    /// Set the upper bound of this range
    ///
    /// # Arguments
    ///
    /// * `end` - The upper bound to set
    #[must_use]
    pub fn with_end(mut self, end: Bound<S>) -> Self {
        self.end = end;
        self
    }

    /// Borrow this ranges bounds in the form a tree seek takes
    pub fn bounds(&self) -> (Bound<&S>, Bound<&S>) {
        (self.start.as_ref(), self.end.as_ref())
    }
}

impl<S: Ord> SortRange<S> {
    /// Check whether this range can contain a key at all
    ///
    /// **This is not an optimization.** `BTreeMap::range` panics when it is handed a range
    /// whose start is past its end, or one whose ends are equal and either of them excludes,
    /// so every scan asks this before it seeks. A range that cannot contain a key also holds
    /// no rows, so answering with none of them is both safe and right.
    pub fn is_empty(&self) -> bool {
        // an unbounded end can never cross the other one
        let (start, end) = match (&self.start, &self.end) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => return false,
            (Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => (start, end),
        };
        // a start past its end names nothing, and a single key needs both ends to include it
        match start.cmp(end) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => !matches!(
                (&self.start, &self.end),
                (Bound::Included(_), Bound::Included(_))
            ),
            std::cmp::Ordering::Less => false,
        }
    }

    /// Check whether a sort key falls inside this range
    ///
    /// # Arguments
    ///
    /// * `key` - The sort key to check
    pub fn contains(&self, key: &S) -> bool {
        // check this key against our lower bound
        let above_start = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(start) => key >= start,
            Bound::Excluded(start) => key > start,
        };
        // a key below our lower bound is outside this range whatever the upper one is
        if !above_start {
            return false;
        }
        // check this key against our upper bound
        match &self.end {
            Bound::Unbounded => true,
            Bound::Included(end) => key <= end,
            Bound::Excluded(end) => key < end,
        }
    }
}

impl<S> Default for SortRange<S> {
    fn default() -> Self {
        SortRange::new(Bound::Unbounded, Bound::Unbounded)
    }
}

/// `Debug` for the archived form of a range
///
/// Written out for the same reason [`ArchivedSortSelect`]'s is.
impl<S: Archive> std::fmt::Debug for ArchivedSortRange<S>
where
    <Bound<S> as Archive>::Archived: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArchivedSortRange")
            .field("start", &self.start)
            .field("end", &self.end)
            .finish()
    }
}

/// The different types of queries for a single datatype
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub enum SortedQuery<T: ShoalSortedTable + std::fmt::Debug + RkyvSupport> {
    /// Insert a row into shoal
    Insert { key: u64, row: T },
    /// Get some data from shoal
    Get(SortedGet<T>),
    /// Delete a row from shoal
    Delete { key: u64, sort_key: T::Sort },
    /// Update a row in a shoal
    Update(SortedUpdate<T>),
    /// Check if data exists in shoal
    Exists(SortedExists<T>),
}

impl<T: ShoalSortedTable + std::fmt::Debug> SortedQuery<T> {
    /// Get the subset of each rows fields this query asked to be answered with
    ///
    /// Only a get returns rows, so every other query answers with the whole row it would have
    /// carried. That is the same answer a get that named no projection gives, which is what
    /// lets the database dispatch on this one value rather than on the query kind as well.
    pub fn projection(&self) -> T::Projection {
        // only a get returns rows there is a subset of
        match self {
            SortedQuery::Get(get) => get.projection,
            _ => T::Projection::default(),
        }
    }

    /// Get the most rows this query asked for, if it set a limit
    pub fn limit(&self) -> Option<usize> {
        // only a get returns rows that a limit could apply to
        match self {
            SortedQuery::Get(get) => get.limit,
            _ => None,
        }
    }

    /// Get the partitions this query named, in the order it named them
    ///
    /// This is the order the rows come back in, so the shard collecting the shares of a
    /// split query uses it to put them back together. Only a get returns rows there is an
    /// order to, so every other query names none.
    pub fn partition_keys(&self) -> &[u64] {
        // only a get returns rows whose order this could describe
        match self {
            SortedQuery::Get(get) => &get.partition_keys,
            _ => &[],
        }
    }
}

/// A single query tagged with client info
pub struct TaggedSortedQuery<R: ShoalSortedTable> {
    /// The id for this query
    pub id: Uuid,
    /// This queries index in the queries vec
    pub index: usize,
    /// The query to execute
    pub query: SortedQuery<R>,
}

impl<R: ShoalSortedTable> TaggedSortedQuery<R> {
    /// Create a new tagged query
    ///
    /// # Arguments
    ///
    /// * `id` - The id for this query's bundle
    /// * `index` - The index for this query in its parent bundle
    /// * `query` - The query to execute
    pub fn new(id: Uuid, index: usize, query: SortedQuery<R>) -> Self {
        Self { id, index, query }
    }
}

/// A get query
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct SortedGet<R: ShoalSortedTable> {
    /// The partition keys to get data from
    pub partition_keys: Vec<u64>,
    /// Which rows of each partition this get is asking for
    ///
    /// Rows come back in sort order whichever arm this is, rather than in the order a set
    /// named its keys. [`SortSelect::All`] is the only arm that means every row.
    pub sort_select: SortSelect<R::Sort>,
    /// Any filters to apply to rows
    pub filters: Option<R::Filters>,
    /// The number of rows to get at most
    pub limit: Option<usize>,
    /// The subset of each rows fields this get is asking to be answered with
    pub projection: R::Projection,
}

impl<R: ShoalSortedTable> SortedGet<R> {
    /// Create a get for just some of this gets partitions
    ///
    /// Everything but the partition keys and the row selection is copied as is, including
    /// the limit. The limit is deliberately not divided up, because a get counts its limit
    /// against the rows it has accumulated so far rather than against any one scan, so a
    /// narrowed get that is handed a partly filled response vec still stops in the
    /// right place.
    ///
    /// The selection is passed in rather than copied because the caller has already normalized
    /// it. That used to happen once, on the coordinator, where the query entered the server;
    /// since F26 the coordinator does not deserialize a query and so has no selection to
    /// normalize, and `ArchivedShardRouting::narrow_to` does it on the shard that executes the
    /// query instead - which is once per shard a get was split to rather than once per get.
    ///
    /// # Arguments
    ///
    /// * `partition_keys` - The keys of the partitions this get should cover
    /// * `sort_select` - The normalized selection of the rows this get should return
    pub fn for_partitions(
        &self,
        partition_keys: Vec<u64>,
        sort_select: SortSelect<R::Sort>,
    ) -> Self {
        SortedGet {
            partition_keys,
            sort_select,
            filters: self.filters.clone(),
            limit: self.limit,
            // a narrowed get answers with the same rows the get it came from asked for
            projection: self.projection,
        }
    }

    /// Create a single partition get from another get
    ///
    /// The selection is carried over as it is, since a get being parked on a disk read has
    /// already been through `split_by_shard` and had it normalized.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key of the partition that needs to be loaded from disk
    pub fn to_blocked(&self, partition_key: u64) -> Self {
        self.for_partitions(vec![partition_key], self.sort_select.clone())
    }

    /// Check if we have already found every row this get asked for
    ///
    /// A get accumulates rows across every partition it names, and a get whose
    /// partitions had to be read from disk accumulates them across several executions,
    /// so this is asked about the rows found so far and not about the rows any one scan
    /// produced. A limit of zero is reached before a single row is read, so a `LIMIT 0`
    /// get scans nothing and loads nothing.
    ///
    /// The rows are counted rather than inspected, so this says nothing about what a get is
    /// being answered with: a projected get fills its limit with the same number of rows an
    /// unprojected one does, and a get answering with rows it borrowed fills it with the same
    /// number as one answering with copies.
    ///
    /// # Arguments
    ///
    /// * `found` - How many rows this get has found so far
    pub fn limit_reached(&self, found: usize) -> bool {
        // check whether this get was given a limit at all
        match self.limit {
            // we are done once we hold as many rows as we were asked for
            Some(limit) => found >= limit,
            // a get with no limit can never fill
            None => false,
        }
    }
}

/// An exists query to check if data exists
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct SortedExists<R: ShoalSortedTable> {
    /// The partition keys to check for data in
    pub partition_keys: Vec<u64>,
    /// Which rows of each partition this exists is asking about
    ///
    /// An exists naming keys or a range is asking whether any of those rows is here. One
    /// selecting [`SortSelect::All`] is asking whether its partitions hold any row at all.
    pub sort_select: SortSelect<R::Sort>,
    /// Any filters to apply to rows
    pub filters: Option<R::Filters>,
}

impl<R: ShoalSortedTable> SortedExists<R> {
    /// Create an exists for just some of this exists partitions
    ///
    /// # Arguments
    ///
    /// * `partition_keys` - The keys of the partitions this exists should cover
    /// * `sort_select` - The normalized selection of the rows this exists asks about
    pub fn for_partitions(
        &self,
        partition_keys: Vec<u64>,
        sort_select: SortSelect<R::Sort>,
    ) -> Self {
        SortedExists {
            partition_keys,
            sort_select,
            filters: self.filters.clone(),
        }
    }

    /// Create a single partition exists from another exists
    ///
    /// The selection is carried over as it is, since an exists being parked on a disk read
    /// has already been through `split_by_shard` and had it normalized.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The key of the partition that needs to be loaded from disk
    pub fn to_blocked(&self, partition_key: u64) -> Self {
        self.for_partitions(vec![partition_key], self.sort_select.clone())
    }
}

/// An update query for a single row in Shoal
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct SortedUpdate<T: ShoalSortedTable> {
    /// The key to the partition to update data in
    pub partition_key: u64,
    /// The sort key to apply updates too
    pub sort_key: T::Sort,
    /// The updates to apply
    pub update: T::UpdateData,
}
