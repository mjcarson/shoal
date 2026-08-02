# 6. Memory accounting collapsed to zero on a partition load

Filed as *Negative `isize` cast collapses memory accounting*, which was the visible half. The
cast was reachable only because the number handed to it was already wrong, and the two had to be
fixed together.

## Symptom

A shard loads a partition from disk, merges it into the copy it already had resident, and comes
out of the merge believing it is using **no memory at all**. Eviction stops, because the trigger
is `*self.memory_usage.borrow() > self.conf.resources.memory` (`shard.rs:657`) and the left side
is now 0. Nothing is logged, nothing fails, and the true resident set keeps growing past the
configured limit until the counter climbs back over it on its own.

## Cause

### The cast

`PersistentSortedTable::load_partition`, on the branch taken when a merged partition ends up
smaller than it was:

```rust
let diff = partition.size() as isize - old_size as isize;
if diff.is_positive() {
    *self.memory_usage.borrow_mut() += diff as usize;
} else {
    let new_mem_usage = self.memory_usage.borrow().saturating_sub(diff as usize);
    *self.memory_usage.borrow_mut() = new_mem_usage;
}
```

`diff` is negative here, so `diff as usize` wraps to a value near `usize::MAX`. The saturating
subtraction cannot underflow, so it floors at 0 instead — the counter is not merely wrong, it is
destroyed, and everything the shard was tracking for every other table goes with it.

### The merge that made it reachable

The more interesting half. `SortedPartition::merge_from_disk` made the disk copy the base and
replayed the in-memory rows over it:

```rust
let memory = std::mem::replace(self, disk);
self.rows.extend(memory.rows.into_iter());
self.tombstones = memory.tombstones;
self.check_disk = false;
```

`BTreeMap::extend` moves entries; it does not touch `size`. So the merged partition kept the
**disk copy's archived size** and counted none of the rows that existed only in memory. `diff`
was therefore `disk.size - memory.size`, not `merged.size - memory.size`.

That is the whole reason the negative branch existed. In-memory rows win every key collision in
`extend`, so the merged live-row set is always a superset of the in-memory one and a correctly
maintained size can only grow across a merge. **With the size right, `diff` is non-negative by
construction.** The shrinking branch was accounting for an event that cannot happen, using a
cast that corrupted the counter when it did.

Both halves point the same way: a partition that had just grown reported that it shrank, and the
report was applied with arithmetic that turned a shrink into a wipe.

## Evidence

Read from the source, then pinned down by the two unit tests below, which were written against
the unfixed code first.

Merging a three-row in-memory copy with a one-row archive extent, rows of 86 bytes each:

```
assertion `left == right` failed     // merged size
  left: 86                           // the disk copy's size
 right: 344                          // the four rows actually held
```

`diff` from those numbers is `86 - 258 = -172`, which is the negative branch, and applying it:

```
assertion `left == right` failed     // memory usage after a -100 adjustment on 1000
  left: 0
 right: 900
```

Not reproduced against a live server. Doing so needs a partition resident *and* an archive
extent for the same key *and* enough memory pressure for the difference to matter, and the
counter is not observable from a client — the eviction log line
(`.../persistent/sorted.rs:1075-1092`) is the only window onto it, and that line had its own
defect ([resolved as item 13](eviction-log-underflow.md)). That fix widened the window: the event
now reports `drift`, the gap between what an eviction pass actually dropped and what the counter
moved by, which is the reading a reproduction of this defect would have wanted.

## The fix

**A merge recomputes its size.** `merge_from_disk` (`.../tables/partitions.rs:442-462`) now sums
the live rows it ended up holding:

```rust
self.size = self
    .rows
    .values()
    .filter_map(|row| match row {
        MaybeRow::Row(row) => Some(row.deep_size_of()),
        MaybeRow::Tombstone => None,
    })
    .sum();
```

Live rows only, which is the basis `insert` and `remove` already maintain — a tombstone
contributes nothing. The walk is O(n), but it runs once per partition load, on a path that has
just deserialized every one of those rows anyway.

**The adjustment has one home.** `adjust_memory_usage`
(`.../tables/persistent.rs:22-27`) is the single place a signed size change reaches the shard
counter:

```rust
pub(crate) fn adjust_memory_usage(memory_usage: &RefCell<usize>, diff: isize) {
    let adjusted = memory_usage.borrow().saturating_add_signed(diff);
    *memory_usage.borrow_mut() = adjusted;
}
```

`saturating_add_signed` was already the idiom at eleven other sites
(`.../persistent/sorted.rs:392`, `:918`, `:978`, `.../persistent/unsorted.rs:419`, `:619`, …);
`load_partition` was the one place that hand-rolled it and got it wrong. The read is finished
before the write because `RefCell` panics on an overlapping mutable borrow.

`load_partition` (`.../persistent/sorted.rs:269-273`) is now the whole branch replaced by one
call, with `cast_signed` in place of the `as isize` casts to match the partition code.

The eleven correct sites were deliberately **not** swept over to the helper in the same change,
so that the diff is only the defect.

## Alternatives rejected

**`saturating_sub(diff.unsigned_abs())` and nothing else** — the fix direction as originally
filed. It stops the counter collapsing, but it leaves the merged partition reporting the archive
extent's size forever, so a shard that faults in a partition it is actively writing to
*subtracts* memory for growing. The catastrophic failure becomes a quiet undercount, which is
harder to find and no more correct.

**Recompute the size in `load_partition` instead of in `merge_from_disk`.** The caller would get
a right answer while `SortedPartition` kept a wrong one in a public field, for every other
caller of `size()` — including `MaybeLoaded::size`, which is what the LRU stores. The partition
is the thing that knows what it merged, so it is the thing that has to say how big it is.

**Track the delta through the merge instead of recomputing.** It would need the size of every
in-memory row that lost a collision and every archived row that did, which is a walk of both
maps — the same cost as summing the result, with a subtraction to get wrong.

**Assert `diff >= 0` after the merge.** Tempting, since the merge can no longer shrink a
partition. But sorted sizes are still maintained by delta everywhere else and still drift
([item 22](../known-issues.md#22-size-accounting-inconsistencies)), so upward drift in
`old_size` could make the diff negative for reasons that have nothing to do with this path. A
`debug_assert` there would fire in test runs for a condition that is not a bug in this code.
Applying the diff signed handles it without a claim that cannot be kept.

## Invariants to uphold

- **A merge is a union, so a merged partition may never be smaller than either input.** In-memory
  rows win collisions and archived rows fill the gaps. Any change to `merge_from_disk` that can
  break this reopens the negative-`diff` path.
- **`size` counts live rows only.** `insert`, `remove`, `tombstone`, and the merge all use
  `deep_size_of` on the row and nothing on a tombstone. A tombstone still occupies a `BTreeMap`
  slot that nothing accounts for; that undercount is item 22 and is bounded by the tombstone
  sweep, not by this.
- **A signed change to shard memory usage goes through `adjust_memory_usage`, never a cast.**
  `diff as usize` on a negative `isize` is not a conversion, it is a wipe.
- **Finish reading `memory_usage` before writing it.** Every site computes into a local first,
  because a `RefCell` mutable borrow overlapping the shared one panics the shard.
- **The counter is still an estimate.** `load_partition`'s vacant arm accounts an `Accessible`
  partition by its raw archive bytes while the loaded arm accounts rows by `deep_size_of`, and
  sorted sizes drift by delta. This fix makes it track the resident set across a merge; it does
  not make it exact.

## Tests

| Test | Fails without |
| --- | --- |
| `merging_from_disk_recomputes_size` (`.../tables/partitions.rs`) | The recompute. The merged partition reports the archive extent's size — 86 against the 344 it holds |
| `merging_from_disk_sizes_only_live_rows` (`.../tables/partitions.rs`) | The live-row filter. A tombstone that survived the merge is charged for the row it is shadowing |
| `memory_usage_shrinks_without_wrapping` (`.../tables/persistent.rs`) | The signed adjustment. A 100 byte shrink on a 1000 byte counter leaves 0 |
| `delete_survives_eviction`, `writes_survive_eviction` (`shoal/tests/persistent_sorted_table.rs`) | Nothing here, but they are the only tests that drive `load_partition` under real memory pressure, so they are what says the larger reported sizes did not break eviction |

Both merge tests were confirmed to fail against the unfixed `merge_from_disk`, and the helper
test against the original cast, rather than being written afterwards and assumed to cover it.

## Related

- [Memory and Eviction](../../tables/memory-and-eviction.md) — the counter and what it triggers
- [Partitions](../../tables/partitions.md#sizes) — the cached size fields and their bases
- [Query Execution](../../tables/query-execution.md) — the load path this sits on
- [Deleted rows came back](resurrected-deletes.md) — the tombstone bookkeeping the merge preserves
