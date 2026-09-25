# 124. An unsorted update to a loaded partition never re-stamped its generation

*Filed by [Resolved #16](hot-path-panics.md). That change reordered this update so it commits
before it changes a row, and kept the stamping exactly as it was so that this could be fixed
with evidence of its own.*

## Symptom

An update to an unsorted row could be silently lost from reads. The client got its update
acknowledged. Later, after the shard evicted the partition under memory pressure, a read
returned the row **as it was before the update**. It kept doing so until the log holding the
update was compacted. A copy loaded from the stale archive in that window went on serving the
old row until it was itself evicted.

The sorted table was not affected.

## Cause

A resident partition that was written to is `MaybeLoaded::Loaded { partition, generation }`.
`generation` names the intent log that holds the partition's newest change. `mark_evictable`
runs when a log is compacted into the archives, and it compares that log's generation with the
stamp. A partition is evictable once every change to it is in an archive, which means its stamp
is at or below the compacted generation.

Every write path stamps the generation it commits in, except one:

| Path | Stamp |
| --- | --- |
| Sorted update, loaded partition | `*generation = self.generation` |
| Unsorted update, accessible partition (read into rows first) | `generation: self.generation` |
| Unsorted replicated `apply` of an update | `*stamped = generation` |
| **Unsorted update, loaded partition** | **unchanged — `update_loaded` updated the row in place and left the insert's stamp** |

So the following sequence lost the update:

1. An insert lands in log N and stamps the partition N.
2. The log rotates. The update lands in log N+1, applies in place, and the partition is still stamped N.
3. Log N is compacted, and `mark_evictable(N, [key])` finds the partition evictable at N. It goes into the LRU.
4. Memory pressure evicts it.
5. A read loads the archive, which has the insert and not the update.

The update was durable the whole time. It sat in log N+1, and compacting that log would have put
it in the archive. Nothing was lost on disk. What a read served in the meantime was wrong.

## Evidence

**Reproduced against the unfixed tree.** `resident_reads.rs` builds a shard's tables on a
glommio executor without a shard. The test builds them with an intent log that is due to rotate
as soon as it holds anything, so the rotation can be driven by hand. Then it:

1. inserts a row
2. sweeps the table, which rotates the log
3. updates the row
4. hands the compaction's `MarkEvictable` to the table
5. evicts what the LRU holds, the way `evict_data` does
6. reads the row back through the loader

```text
test a_sorted_update_is_not_evicted_before_its_log_is_archived ... ok
test an_unsorted_update_is_not_evicted_before_its_log_is_archived ... FAILED
thread 'an_unsorted_update_is_not_evicted_before_its_log_is_archived' panicked at shoal/tests/resident_reads.rs:527:9:
a read after the eviction lost the update
  left: Some("before")
 right: Some("after")
```

The sorted twin passed before and after the fix, as its path already stamped the generation.

## The fix

`MaybeLoaded::<UnsortedPartition>::update_loaded` takes the generation the update was committed
in and stamps it, and `PersistentUnsortedTable::update` passes `self.generation`. This is the
same stamp the three other paths set. The change is one assignment on a path that was already
borrowing the entry.

## Alternatives rejected

**Stamp at the call site.** `update` could match the partition again after `update_loaded`
returns and stamp it there, as the replicated `apply` does. That spreads one rule across two
statements that nothing forces to stay together. Taking the generation as an argument means a
caller cannot update a loaded row without saying which log the update is in.

**Evict only partitions whose every intent is archived, checked against the archive map.** That
would be correct without any stamps, but it costs a lookup for every partition a marking names,
to recover what the stamp already says.

## Invariants to uphold

- **Every write that changes a resident partition stamps the generation it committed in.**
  `is_evictable` trusts the stamp completely. A new write path, or a new arm of an existing one,
  that changes a row and leaves the stamp alone reopens this.
- **The stamp is the generation of the log that holds the change, not the generation the
  partition was loaded in.** A partition loaded from an archive and then written is stamped
  with the current log's generation, not the archive's.
- **`mark_evictable` is the only place a written partition enters the LRU.** A write pops the
  partition out of the LRU. If anything else puts it back before its log is compacted, the stamp
  never gets a chance to matter.

## Still open

Nothing of this item.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `an_unsorted_update_is_not_evicted_before_its_log_is_archived` (`shoal/tests/resident_reads.rs`) | The partition is marked evictable at the insert's generation and evicted, and the read returns the row without its update. |
| `a_sorted_update_is_not_evicted_before_its_log_is_archived` | Nothing today; it holds the sorted update to the same rule, so the two tables cannot drift apart again. |

## Related

- [Resolved #16](hot-path-panics.md), whose reorder of this update found it.
- [Resolved #5](resurrected-deletes.md), which introduced the flushed generation that
  `mark_evictable` compares against, for the same reason: nothing may be evicted before its change is archived.
- [Resolved #30, 120, 121](resident-copy-collision.md), whose harness the test runs on.
