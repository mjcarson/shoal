//! Persistent tables cache hot data in memory while also storing data on disk.
//!
//! This means that data is retained through restarts at the cost of speed.

pub(crate) mod sorted;
pub(crate) mod unsorted;

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use uuid::Uuid;

pub use sorted::PersistentSortedTable;
pub use unsorted::PersistentUnsortedTable;

use crate::server::messages::{Answer, QueryMetadata};
use crate::server::stage_profile::StageStamps;
use crate::shared::protocol::error::ErrorCode;
use crate::shared::responses::{GetRows, Response, ResponseAction, ResponseError, RowGroup};
use crate::shared::row_ref::RowRef;
use crate::shared::traits::ShoalProjection;
use rkyv::Archive;

/// Replace what a query answered with the failure it was released with, if it was released by one
///
/// A query parked on a read that gave up is replayed rather than answered on the spot, because it
/// may still be parked on *another* partition — answering it here would put a second response at
/// an index that already has one. Applying the failure where the query finally produces a response
/// is what keeps it to exactly one, and keeps the `end` flag and the index the ones this query
/// would have answered with.
///
/// A query that produced nothing here is still parked, so it carries its failure onward untouched.
///
/// # Arguments
///
/// * `answered` - What executing the query produced, if anything
/// * `failed` - The failure this query was released with, if it was released by one
/// Wrap an answer that was always a value in the shape every query now answers in
///
/// Only a get can answer with rows, so only a get can answer with rows it did not copy. Every
/// other query already held its whole answer in a `bool`, and goes through here rather than
/// through a match at each of its call sites.
///
/// # Arguments
///
/// * `answered` - What the query produced, if it produced anything
pub(crate) fn open<P>(
    answered: Option<(Uuid, Uuid, StageStamps, Response<P>)>,
) -> Option<(Uuid, Uuid, StageStamps, Answer<Response<P>>)> {
    answered.map(|(client, id, stamps, response)| (client, id, stamps, Answer::Open(response)))
}

pub(crate) fn apply_failure<P>(
    answered: Option<(Uuid, Uuid, StageStamps, Answer<Response<P>>)>,
    failed: Option<ResponseError>,
) -> Option<(Uuid, Uuid, StageStamps, Answer<Response<P>>)> {
    // only a query that produced a response has anything to swap
    match answered {
        Some((client, id, stamps, Answer::Open(mut response))) => {
            // a read that gave up is a failure whatever kind of query was waiting on it - a
            // get that could not read its partition and a delete that could not read the row
            // it was deleting are both failures, not "found nothing" and "deleted nothing"
            if let Some(error) = failed {
                response.data = ResponseAction::Error(error);
            }
            Some((client, id, stamps, Answer::Open(response)))
        }
        // a sealed answer cannot be carrying a failure, and the two conditions are exclusive by
        // construction rather than by luck: a failure reaches a query only when a read it was
        // parked on gave up, and a query that has parked is never answered in place
        sealed @ Some((_, _, _, Answer::Sealed(_))) => {
            debug_assert!(
                failed.is_none(),
                "a query answered in place was released with a failure, which means it parked"
            );
            sealed
        }
        // this query is still parked somewhere else, so its failure travels on with it
        None => None,
    }
}

/// Build the failure a client is told when an archive could not be read back
///
/// What the client is told names the table and the partition and nothing else. The path, the
/// archive id and the validation error stay in the `ERROR` event the caller emits: with no
/// authentication yet, the server's filesystem layout is not something to hand to whoever opened
/// a socket.
///
/// # Arguments
///
/// * `table` - The table the unreadable partition belongs to
/// * `partition_id` - The partition that could not be read
pub(crate) fn corrupt_archive<T: std::fmt::Display>(
    table: T,
    partition_id: u64,
) -> ResponseError {
    ResponseError::new(
        ErrorCode::CorruptArchive,
        format!("partition {partition_id} of {table} could not be read"),
    )
}

/// What a partition read left behind for the queries that were parked on it
///
/// A read has three outcomes and only two of them used to be expressible. Before this, a load
/// that failed part way through returned its error out of the function, which meant the queries
/// parked on that partition were never drained out of `blocked` and their clients waited on
/// responses that nothing would ever produce.
#[derive(Debug)]
pub enum PartitionLoad<Q> {
    /// Nothing was waiting on this partition
    Idle,
    /// The archive is resident now, and these queries can be replayed against it
    ///
    /// The generation is the newest compacted one, which is what the caller marks this
    /// partition evictable at.
    Loaded(Vec<(QueryMetadata, Q)>, u64),
    /// The archive could not be made usable, and these queries answer with the failure instead
    ///
    /// There is no generation because nothing entered the table and nothing came out of the lru
    /// that has to be put back, which is the same reason a reported failure sends no
    /// `MarkEvictable` message either.
    Failed(Vec<(QueryMetadata, Q)>),
}

/// The rows one get has found so far, kept in the order its query named its partitions
///
/// A get names its partitions in the order it wants their rows back in, but a partition that
/// has to be read from disk is replayed long after the ones that were already resident. Each
/// partition is given its own slot here rather than appending to one shared vec, so a replayed
/// partition lands where the query asked for it instead of wherever it happened to finish.
#[derive(Debug)]
pub(crate) struct PendingGet<R> {
    /// The partition keys this get named, in the order it named them
    keys: Vec<u64>,
    /// The rows found for each key above, or None for a partition we have not read yet
    slots: Vec<Option<Vec<R>>>,
    /// The most rows this get asked for, if it set a limit
    limit: Option<usize>,
}

impl<R> PendingGet<R> {
    /// Start tracking the partitions a get named
    ///
    /// # Arguments
    ///
    /// * `keys` - The partition keys this get named, in the order it named them
    /// * `limit` - The most rows this get asked for, if it set a limit
    pub fn new(keys: &[u64], limit: Option<usize>) -> Self {
        PendingGet {
            keys: keys.to_vec(),
            slots: (0..keys.len()).map(|_| None).collect(),
            limit,
        }
    }

    /// Find the slot a partitions rows belong in, if it still needs reading
    ///
    /// Partition keys are deduplicated before a get reaches a table, so a key names at most
    /// one slot. A key we have already read gives back None, since a partition is only ever
    /// worth reading once per get.
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key to find the slot for
    pub fn rank(&self, key: u64) -> Option<usize> {
        // find where this get named this partition
        let rank = self.keys.iter().position(|found| *found == key)?;
        // a slot we have already filled has nothing left to read
        match self.slots[rank] {
            Some(_) => None,
            None => Some(rank),
        }
    }

    /// Record the rows a partition gave us
    ///
    /// # Arguments
    ///
    /// * `rank` - The slot these rows belong in
    /// * `rows` - The rows this partition gave us
    pub fn fill(&mut self, rank: usize, rows: Vec<R>) {
        self.slots[rank] = Some(rows);
    }

    /// Whether the partitions named before this one already hold every row this get asked for
    ///
    /// A partition can only be passed over once every partition named before it has been read,
    /// because an unread one could still supply rows that come before any of these and push
    /// this partitions rows out of the answer. A get with no limit can never fill, so nothing
    /// is ever passed over for one.
    ///
    /// # Arguments
    ///
    /// * `rank` - The slot to check the partitions before
    pub fn filled_before(&self, rank: usize) -> bool {
        // a get with no limit wants every row every one of its partitions holds
        let Some(limit) = self.limit else {
            return false;
        };
        // count the rows held by the partitions named before this one
        let mut found = 0;
        for slot in &self.slots[..rank] {
            // an unread partition could still hold rows that come before these
            let Some(rows) = slot else {
                return false;
            };
            found += rows.len();
        }
        found >= limit
    }

    /// Whether any partition this get named is still waiting to be read
    pub fn is_pending(&self) -> bool {
        self.slots.iter().any(Option::is_none)
    }

    /// Fold our slots into the rows this get answers with, and the index naming their partitions
    ///
    /// The slots are in the order the query named its partitions, so this is where that order
    /// becomes the order of the rows. Each partition stopped at the limit on its own, so their
    /// total can still be over it and is trimmed here.
    ///
    /// **The grouping is kept rather than flattened away.** This used to collect every row into
    /// one fresh `Vec` and throw away which partition each came from, after which the shard
    /// collecting a split get had to hash every row's partition key to work it back out
    /// ([O18](../../../docs/src/appendix/optimizations.md)). The slots already hold exactly that
    /// index, in exactly the right order.
    pub fn finish(self) -> GetRows<R> {
        // pair each slot with the partition it was read from, in the order they were named
        let slots = self
            .keys
            .iter()
            .copied()
            .zip(self.slots)
            .map(|(key, rows)| (key, rows.unwrap_or_default()));
        let mut data = GetRows::from_slots(slots);
        // drop anything past the limit this get asked for
        if let Some(limit) = self.limit {
            data.truncate(limit);
        }
        data
    }
}

/// Where a row a get found currently lives
///
/// A get that named no projection can answer with the row wherever the table is keeping it: with
/// the row itself if its partition is resident, and with the archived row if its partition is
/// still the archive it was read from. Only a projection has to build what it answers with, and
/// that built row lives in the sink's own scratch space until the reply is serialized.
enum Found<'a, P: ShoalProjection> {
    /// Still in the partition that holds it
    Resident(&'a P),
    /// Still in the archive its partition was read from
    ///
    /// Held as the *row's* archived type rather than the projection's, because that is the type
    /// the archive holds and the type [`ShoalProjection::from_archived`] takes if this row ever
    /// has to be materialized after all. Only the identity projection can put a row here, and
    /// [`ShoalProjection::ARCHIVED_IDENTITY`] is what says so.
    InArchive(&'a <<P as ShoalProjection>::Row as Archive>::Archived),
    /// Built by this get, and held at this index of the scratch space
    ///
    /// An index rather than a reference, because the scratch space is a `Vec` that grows as the
    /// scan runs and a reference into it would dangle the moment it reallocated.
    Built(usize),
}

impl<P: ShoalProjection> std::fmt::Debug for Found<'_, P> {
    /// Say where a row lives, without requiring an archived row to be printable
    ///
    /// Deriving this would put a `Debug` bound on the *archived* row type, which a schema is not
    /// otherwise required to ask rkyv for, so a table whose rows did not would stop compiling.
    ///
    /// # Arguments
    ///
    /// * `formatter` - The formatter to write where this row lives into
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Found::Resident(_) => formatter.write_str("Found::Resident(..)"),
            Found::InArchive(_) => formatter.write_str("Found::InArchive(..)"),
            Found::Built(at) => write!(formatter, "Found::Built({at})"),
        }
    }
}

/// The rows one execution of a get found, borrowed wherever they could be
///
/// This is what makes [O2](../../../docs/src/appendix/optimizations.md)'s resident half work for
/// a get that names several partitions of which only some are resident. The alternative was a
/// homogeneity rule — borrow only when *every* named partition is resident — which is simpler and
/// gives up the common case, because a partition read from disk stays an archive and a long lived
/// table is a mixture.
#[derive(Debug)]
pub struct RowSink<'a, P: ShoalProjection> {
    /// Where each row this get found currently lives, in the order it was found
    ///
    /// **Empty until this get finds a row it can point at.** A get every one of whose rows had to
    /// be built — which is every projected get — keeps its rows in `scratch` alone and this stays
    /// empty, because an index that says "the *i*th row is the *i*th built row" for every row
    /// carries nothing. See [`Self::mixed`].
    found: Vec<Found<'a, P>>,
    /// The rows this get had to build, in the order it built them
    scratch: Vec<P>,
    /// Whether this get has found rows in both places, and so needs `found` to tell them apart
    ///
    /// This exists because of a measurement. `push_built` writing to both vectors cost the
    /// archived scans **5–14%** against the shape they had before
    /// ([F27](../../../docs/src/features/grouped-responses.md)) — a second push per row, on what
    /// was then the one path that gained nothing from being able to point at rows. An archived
    /// row is pointed at now ([F28](../../../docs/src/features/rearchived-rows.md)) and the
    /// remaining path that builds every row is a projection, but the shape is kept because the
    /// measurement that produced it applies to that path unchanged. While this is false the
    /// second push does not happen, and `found` is backfilled if a row that can be pointed at
    /// ever arrives.
    mixed: bool,
    /// Which partition each run of those rows came from, in the order they were scanned
    groups: Vec<RowGroup>,
    /// How many rows had been found when the run being scanned started
    run_started_at: usize,
}

impl<P: ShoalProjection> Default for RowSink<'_, P> {
    fn default() -> Self {
        RowSink {
            found: Vec::new(),
            scratch: Vec::new(),
            mixed: false,
            groups: Vec::new(),
            run_started_at: 0,
        }
    }
}

impl<'a, P: ShoalProjection> RowSink<'a, P> {
    /// Answer with a row the partition is already holding
    ///
    /// # Arguments
    ///
    /// * `row` - The row to answer with, where it lies
    pub fn push_resident(&mut self, row: &'a P) {
        // the moment a row can be pointed at, where each row lives stops being implied by its
        // position and has to be recorded - so catch `found` up with what `scratch` already holds
        if !self.mixed {
            self.found
                .extend((0..self.scratch.len()).map(Found::Built));
            self.mixed = true;
        }
        self.found.push(Found::Resident(row));
    }

    /// Answer with a row that is still in the archive its partition was read from
    ///
    /// The archived twin of [`RowSink::push_resident`], and it keeps the same bookkeeping: a row
    /// pointed at here is a row that was not built, so `found` has to start saying where each row
    /// lives from this point on.
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived row to answer with, where it lies
    pub fn push_archived(&mut self, archived: &'a <<P as ShoalProjection>::Row as Archive>::Archived) {
        // the moment a row can be pointed at, where each row lives stops being implied by its
        // position and has to be recorded - so catch `found` up with what `scratch` already holds
        if !self.mixed {
            self.found
                .extend((0..self.scratch.len()).map(Found::Built));
            self.mixed = true;
        }
        self.found.push(Found::InArchive(archived));
    }

    /// Answer with a row this get had to build
    ///
    /// # Arguments
    ///
    /// * `row` - The row this get built
    pub fn push_built(&mut self, row: P) {
        // while every row is a built one, its position in `scratch` is its position in the
        // answer, and saying so per row is a second write for nothing
        if self.mixed {
            self.found.push(Found::Built(self.scratch.len()));
        }
        self.scratch.push(row);
    }

    /// How many rows this get has found so far
    #[must_use]
    pub fn len(&self) -> usize {
        // until a row is pointed at, every row this get found is a built one
        if self.mixed {
            self.found.len()
        } else {
            self.scratch.len()
        }
    }

    /// Whether this get has found no rows at all
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Record that everything found since the last call came from this partition
    ///
    /// Called once per partition the get visited, whether or not it gave anything, so the caller
    /// does not have to remember where each run started. A partition that gave nothing closes no
    /// group, which is the same rule [`GetRows::from_slots`] follows.
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition the rows found since the last close came from
    pub fn close_group(&mut self, partition: u64) {
        // a partition that gave nothing is an absence rather than an empty group
        let len = self.len() - self.run_started_at;
        if len > 0 {
            self.groups.push(RowGroup {
                partition,
                len: len as u64,
            });
        }
        self.run_started_at = self.len();
    }

    /// How many of the rows this get found it had to build rather than point at
    ///
    /// This is the number [O2](../../../docs/src/appendix/optimizations.md) is about. A get that
    /// named no projection and read only resident partitions builds **none** of its rows, and a
    /// test asserts exactly that — the entry's claim is a count, so it is checked as one rather
    /// than inferred from a benchmark.
    #[must_use]
    pub fn built(&self) -> usize {
        self.scratch.len()
    }

    /// Point at every row this get found, wherever it lives
    ///
    /// This yields a [`RowRef`] rather than a `&P` because a row still in an archive is not a
    /// row: what the partition holds is `Archived<P::Row>`, and the two are only interchangeable
    /// once they are being written to the wire, which is what `RowRef` is for.
    ///
    /// # Arguments
    ///
    /// * `self` - The sink to walk
    pub fn iter(&self) -> impl Iterator<Item = RowRef<'_, P>> {
        // only the identity projection can have pointed at an archived row, so it set this
        let identity = P::ARCHIVED_IDENTITY;
        // a get that pointed at nothing is its scratch space, in order
        let built = (!self.mixed).then(|| self.scratch.iter().map(RowRef::new));
        let placed = self.mixed.then(move || {
            self.found.iter().map(move |found| match found {
                Found::Resident(row) => RowRef::new(*row),
                Found::InArchive(archived) => {
                    // an archived row reached the sink, so the constant that let it in is set
                    let identity = identity
                        .expect("only the identity projection can point at an archived row");
                    RowRef::archived(identity(*archived))
                }
                Found::Built(at) => RowRef::new(&self.scratch[*at]),
            })
        });
        built.into_iter().flatten().chain(placed.into_iter().flatten())
    }

    /// Take every row this get found as an owned row, cloning the ones it borrowed
    ///
    /// This is what a get that cannot answer where its rows lie uses — one that parked on a disk
    /// read, or one answering a share of a split get. Cloning a borrowed row here costs exactly
    /// what `P::from_row` cost before it was borrowed, so the path this feeds is no worse than it
    /// was; it is simply no better either.
    #[must_use]
    pub fn into_owned(self) -> Vec<P>
    where
        P: Clone,
    {
        // a get that pointed at nothing already owns every row it found, in order
        if !self.mixed {
            return self.scratch;
        }
        // the built rows come out in the order they went in, which is the order they are named in
        let mut built = self.scratch.into_iter();
        self.found
            .into_iter()
            .map(|found| match found {
                Found::Resident(row) => row.clone(),
                Found::InArchive(archived) => P::from_archived(archived),
                Found::Built(_) => built
                    .next()
                    .expect("every built row was pushed to the scratch space it is indexed into"),
            })
            .collect()
    }

    /// Point at every row this get found, wherever it lives, with the index naming its partitions
    ///
    /// The borrow is the sink's rather than the partition's, because a built row lives in the
    /// sink. That is what obliges the reply to be serialized while the sink is still alive, and
    /// is the reason a get that has to park cannot take this path at all. A row pointed at in a
    /// partition or in an archive outlives the sink, but the sink cannot say so without splitting
    /// its lifetime in two, and the caller that needs it to is the one that cannot park anyway.
    #[must_use]
    pub fn rows(&self) -> GetRows<RowRef<'_, P>> {
        let rows = self.iter().collect();
        GetRows {
            rows,
            groups: self.groups.clone(),
        }
    }
}

/// The gets a table has parked while it waits for their partitions to be read from disk
///
/// A get can be answered with whole rows or with any of its tables projections, so what a
/// parked get has found so far is a `PendingGet` of a different type for each of them. A table
/// has one of these rather than one map per projection, so the row type is erased here and
/// recovered when the get is picked back up.
///
/// The erasure costs an allocation and a downcast, and only ever on the path that is already
/// waiting on a disk read: a get every one of whose partitions is resident finishes in one
/// execution and is never parked at all.
#[derive(Default)]
pub(crate) struct PendingGets {
    /// What each parked get has found so far, keyed by the query it answers
    parked: HashMap<(Uuid, usize), Box<dyn Any>>,
}

impl std::fmt::Debug for PendingGets {
    /// Print how many gets are parked, since what they hold has no type to print
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingGets")
            .field("parked", &self.parked.len())
            .finish()
    }
}

impl PendingGets {
    /// Build somewhere to park gets waiting on a disk read
    ///
    /// # Arguments
    ///
    /// * `capacity` - The number of parked gets to make room for up front
    pub fn with_capacity(capacity: usize) -> Self {
        PendingGets {
            parked: HashMap::with_capacity(capacity),
        }
    }

    /// Pick a parked get back up, or start it fresh if it has never run before
    ///
    /// A get is replayed with the projection it was sent with, because the query parked on the
    /// partition is a copy of the one that parked it, so the type asked for here is always the
    /// type stored. A downcast that fails would mean two gets shared a query id and index while
    /// asking for different rows, which cannot happen, so it is a panic rather than a fresh
    /// start that would silently drop the rows already found.
    ///
    /// # Arguments
    ///
    /// * `key` - The query id and index of the get being executed
    /// * `partition_keys` - The partition keys this get named, in the order it named them
    /// * `limit` - The most rows this get asked for, if it set a limit
    pub fn resume<P: 'static>(
        &mut self,
        key: &(Uuid, usize),
        partition_keys: &[u64],
        limit: Option<usize>,
    ) -> PendingGet<P> {
        // take this gets progress back out, if it has run before
        match self.parked.remove(key) {
            // carry on filling the slots this get already has
            Some(parked) => match parked.downcast::<PendingGet<P>>() {
                Ok(pending) => *pending,
                Err(_) => panic!("a parked get was resumed with a different projection"),
            },
            // this query has never been executed before so start it off
            None => PendingGet::new(partition_keys, limit),
        }
    }

    /// Whether this get has already run once and parked on a partition read
    ///
    /// A get that has parked cannot be answered out of borrowed rows: what it found on its
    /// earlier passes is owned and outlives the execution that found it, which is the whole
    /// reason it could be parked at all.
    ///
    /// # Arguments
    ///
    /// * `key` - The query id and index of the get being executed
    pub fn is_parked(&self, key: &(Uuid, usize)) -> bool {
        self.parked.contains_key(key)
    }

    /// Park a get until the partitions it is still waiting on have been read
    ///
    /// # Arguments
    ///
    /// * `key` - The query id and index of the get being parked
    /// * `pending` - What this get has found so far
    pub fn park<P: 'static>(&mut self, key: (Uuid, usize), pending: PendingGet<P>) {
        self.parked.insert(key, Box::new(pending));
    }
}

/// Apply a signed change in size to a shards total memory usage
///
/// A shrink is applied with `saturating_add_signed` rather than a cast to `usize`,
/// which would wrap a negative diff to near `usize::MAX` and floor the counter at 0.
///
/// # Arguments
///
/// * `memory_usage` - The total memory usage for this shard
/// * `diff` - The signed change in size to apply
pub(crate) fn adjust_memory_usage(memory_usage: &RefCell<usize>, diff: isize) {
    // compute the new usage first since RefCell panics on an overlapping mutable borrow
    let adjusted = memory_usage.borrow().saturating_add_signed(diff);
    // store our updated usage
    *memory_usage.borrow_mut() = adjusted;
}

/// Summarize what an eviction pass reclaimed
///
/// Both values saturate because the shard counter is an estimate that drifts, so no
/// ordering between `pre`, `post`, and `removed` may be assumed. A plain subtraction
/// here panics the shard from a log statement in a debug build and wraps to near
/// `usize::MAX` in a release one.
///
/// # Arguments
///
/// * `pre` - Shard memory usage before the eviction pass
/// * `post` - Shard memory usage after the eviction pass
/// * `removed` - The total size of the partitions the pass actually dropped
///
/// # Returns
///
/// The bytes the shard counter moved by, and the bytes the dropped partitions were
/// accounted for beyond that — non zero only when the counter had already drifted low.
pub(crate) fn eviction_totals(pre: usize, post: usize, removed: usize) -> (usize, usize) {
    // what the shard counter actually moved by
    let reclaimed = pre.saturating_sub(post);
    // what the dropped partitions were accounted for beyond that, which is only
    // non zero when the counter had already drifted low and floored at 0
    let drift = removed.saturating_sub(reclaimed);
    (reclaimed, drift)
}

#[cfg(test)]
mod tests {
    use super::{adjust_memory_usage, eviction_totals};
    use std::cell::RefCell;

    #[test]
    /// An eviction pass summarizes itself without underflowing on a drifted counter
    fn eviction_logging_cannot_underflow() {
        // an ordinary pass takes the partitions it dropped off of the counter
        assert_eq!(eviction_totals(1000, 600, 400), (400, 0));
        // a counter that had already drifted low floors at 0, so the partitions we
        // dropped are accounted for more than the counter could give back
        assert_eq!(eviction_totals(300, 0, 900), (300, 600));
        // and a counter that somehow grew across the pass reports nothing reclaimed
        // rather than taking the shard down from a log statement
        assert_eq!(eviction_totals(600, 1000, 0), (0, 0));
    }

    #[test]
    /// A shrinking partition subtracts its diff instead of wrapping the counter
    fn memory_usage_shrinks_without_wrapping() {
        // a shrink takes exactly its own size off of our usage
        let memory_usage = RefCell::new(1000);
        adjust_memory_usage(&memory_usage, -100);
        assert_eq!(*memory_usage.borrow(), 900);
        // growing adds the diff back on
        adjust_memory_usage(&memory_usage, 250);
        assert_eq!(*memory_usage.borrow(), 1150);
        // a shrink larger than our usage saturates at 0 rather than wrapping
        adjust_memory_usage(&memory_usage, -2000);
        assert_eq!(*memory_usage.borrow(), 0);
        // and a shrink against an empty counter stays there
        adjust_memory_usage(&memory_usage, -1);
        assert_eq!(*memory_usage.borrow(), 0);
    }
}
