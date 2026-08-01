//! Persistent tables cache hot data in memory while also storing data on disk.
//!
//! This means that data is retained through restarts at the cost of speed.

mod sorted;
mod unsorted;

use std::cell::RefCell;

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

#[cfg(test)]
mod tests {
    use super::adjust_memory_usage;
    use std::cell::RefCell;

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
