# 196. The eviction budget undercounted what a node holds

Filed in round 15 of the lab testing from a heap profile, its reported half done there (the WAL
index on `Stats`), and the rest fixed in round 16
([cluster testing](../../cluster-testing/correctness.md#17-round-16)).

## Symptom

A jemalloc heap profile of titan at the end of a whole load
([memory at ten times the dataset](../../cluster-testing/performance.md#memory-at-ten-times-the-dataset))
held 3.4 GiB for rows - Movie rows applied and deserialized 2.4 GiB, keyword rows 0.6, the
table map 0.29, the eviction LRU 0.16 - where `Stats` counted 2.3 GiB. The rows a node kept were
fewer than its budget said, and the memory table's figures did not add up to `resident`.

## Cause

A shard evicts against a counter of what its rows cost, charged as each row is applied or
loaded, and released as its partition is evicted. Three things it counted wrong, and one it did
not count:

- **A sorted row was charged its own bytes alone.** `SortedPartition::insert` charged
  `row.deep_size_of()`, which is the row's inline size and its heap. The row is filed in a
  `BTreeMap` under a clone of its sort key, and the tree's node holds the key, the row and the
  slots nobody has filled yet: for a keyword row of about 145 bytes, another 120 or so a row,
  uncounted. Every recount (`merge_from_disk`, `fold_fragment`) summed rows the same way, so the
  base was consistent and low everywhere.
- **The unsorted replay charged a different base than eviction released.** `replay` charged
  `row.deep_size_of()` and eviction released `partition.size`, the row plus the partition's own
  seventeen bytes ([known issue 22](../known-issues.md#22-size-accounting-inconsistencies)'s
  shape, one more instance of it).
- **A replicated insert into a resident archive charged the row's difference alone.** The
  archive's bytes had been charged when it was read; the insert deserialized the partition,
  which eviction would later release at its deep size, and moved the counter by the row's size
  only. The delete and update on the same path already moved it by the difference between the
  two bases.
- **The eviction list was counted by nobody.** An entry per evictable partition - a boxed node
  and a hash bucket, about 70 bytes - held beside the rows: 0.16 GiB on titan at one copy of the
  dataset, 0.25 GiB on europa under the bench.

## Evidence

**Established by the heap profile**, and pinned by unit tests written first:
`a_sorted_row_is_charged_with_its_key_and_node` fails on the unfixed tree at its first assertion
(a row charged exactly its own bytes); `merging_from_disk_recomputes_size` and
`merging_from_disk_sizes_only_live_rows` were changed to expect the entry rather than the row.
The seeded harness of `hot_path_failures.rs` now writes and deletes a second sorted row before
the tables are opened, so the replay holds a tombstone for the marking to sweep, and asserts the
shard's counter against the resident partitions after the sweep and against zero after eviction;
with the sweep's release left out it fails *the shard counted 304 for partitions holding 237*.

**On the lab**, round 16 compared `cluster stats` with a heap profile of the same node on the
fixed build ([round 16](../../cluster-testing/correctness.md#what-a-row-is-charged)): TBD-LAB.

## The fix

- **A sorted entry is charged whole.** `SortedPartition::entry_size(sort, row)` is the row's
  deep size plus `key_size(sort)`: the key's deep size and `ENTRY_OVERHEAD`, an entry's share of
  a B-tree node with eleven slots and a header at eight entries a node. A row new to the tree
  charges the entry; a row replacing a row charges the difference; a row over a tombstone
  charges the row, since the tombstone kept the key; a tombstone over a row releases the row and
  keeps the key; a tombstone for a key the tree never held charges the key; `drop_tombstones`
  releases the keys it drops, and `mark_evictable`, which sweeps a partition's tombstones once
  its deletes are archived, releases what the sweep took from the shard's counter too - the one
  place a partition's size moved without the counter, found by the reviewer of this change
  before it was committed: the seeded harness counted 304 bytes for partitions holding 237.
  `merge_from_disk` and `fold_fragment` recount by the same rule
  (`recount`). The archived `size` a compactor writes is the new formula's from here on.
- **The unsorted replay charges `partition.size`**, what eviction releases.
- **A replicated sorted insert into a resident archive** moves the counter by the deserialized
  partition's size less the archive's bytes, as the delete and update on that path did.
- **The eviction list is reported.** `Shard::lru_bytes` estimates it from the list's length, on
  `ShardReplication::lru_bytes`, `NodeStats::lru_bytes` and an `lru` column of `cluster stats`,
  beside the maps and the WAL index. It is reported rather than charged: the list is the shard's,
  not a table's, and its entries come and go with marks and pops on twenty paths; a figure that
  is read where the rows' counter is read is what makes the table add up.

## Alternatives rejected

- **Measuring what a row's allocations take, through a counting allocator.** A wrapper around
  the global allocator with a per-thread live counter, read before and after a row is
  deserialized and inserted, would count the allocator's rounding of every String and Vec block
  as well - the part of the gap this fix does not close. It puts a counter on every allocation
  the node makes, its bracket has to exclude every temporary alive at its end, and it makes the
  charge depend on which binary runs the schema. The structural charge was taken first; the
  rounding is filed with what is left of the gap below.
- **A calibrated multiplier on `deep_size_of`.** A number nobody could defend a version later.
- **Charging the eviction list into the counter.** Twenty `put` and `pop` sites across the two
  tables and the shard, and a charge that moves when a partition is *marked*, not when it grows.
  Reported instead, as the table and archive indexes are.
- **Rounding each block to the allocator's size class inside `deep_size_of`.** deepsize2 sums
  capacities in its own impls and offers no hook per block; a fork for it was not worth owning.

## Invariants to uphold

- Whatever a sorted partition charges when an entry enters the tree, it releases when the entry
  leaves, through the same `key_size`/`entry_size`: `resident_reads.rs` asserts the counter is
  exactly zero once everything is evicted.
- A key's charge lives with the key, not the row: a row that becomes a tombstone releases the row
  only, and a sweep of tombstones releases the keys.
- Every path that turns a resident archive into a loaded partition moves the counter by
  `loaded.size() - archive.len()`, never by the mutation's own diff.
- `lru_bytes` is an estimate from the list's length and the entry's shape; change the LRU crate
  or its key and revisit the constants.

## Still open

- **The allocator's rounding is still not counted.** Every String and Vec block a deserialized
  row holds is rounded up to a size class, and a Movie row holds about thirty of them. What that
  costs on the lab is the gap the round's measurement leaves, on the todo page under
  [the row charge](../todos.md#what-a-rows-allocations-take).
- Known issue 22's other mismatches stand as filed.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_sorted_row_is_charged_with_its_key_and_node` (`shoal-core/src/server/tables/partitions.rs`) | A sorted row is charged its own bytes alone, or a tombstone or a sweep moves the size by something other than the key |
| `merging_from_disk_recomputes_size`, `merging_from_disk_sizes_only_live_rows` (the same) | A recount uses a base other than the entry's |
| `resident_reads.rs` (`shoal/tests`) | Eviction leaves memory charged: a charge and its release disagree |
| The seeded harness of `hot_path_failures.rs` (`shoal/tests`), every test that opens it | A replayed delete's tombstone is swept at the marking and the shard's counter keeps the key it held: 304 counted for 237 resident, then 67 left charged after eviction |

## Related

- [Resolved #149](node-memory-budget.md), the process bound that caught the total meanwhile.
- [O83](../optimizations.md#o83-the-partition-index-held-forty-eight-bytes-a-partition), the
  archive map's entry, the other half of what a node holds beside its rows.
- [Cluster testing, round 16](../../cluster-testing/correctness.md#17-round-16).
