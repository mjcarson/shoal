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

    /// Flatten our slots into the rows this get answers with
    ///
    /// The slots are in the order the query named its partitions, so this is where that order
    /// becomes the order of the rows. Each partition stopped at the limit on its own, so their
    /// total can still be over it and is trimmed here.
    pub fn finish(self) -> Vec<R> {
        // collect every row we found, partition by partition, in the order they were named
        let mut data: Vec<R> = self.slots.into_iter().flatten().flatten().collect();
        // drop anything past the limit this get asked for
        if let Some(limit) = self.limit {
            data.truncate(limit);
        }
        data
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
