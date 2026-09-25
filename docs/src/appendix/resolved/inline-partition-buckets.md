# 150. A table's partition map held every loaded row inline in its buckets, at its peak capacity

## Symptom

A heap profile of hyperion's node under the insert bench, taken while chasing #149, found 3 GB of
a 7 GB process in one kind of allocation: the persistent unsorted table's partition map, one per
shard, about 252 MB each. The eviction budget saw none of it.

## Cause

A persistent table keeps its loaded partitions in a `HashMap<u64, MaybeLoaded<P>>`. A hash map
stores its values inline in its buckets, all of them, empty ones included. It grows to fit the
most partitions it has held at once and never shrinks when they are evicted. `MaybeLoaded::Loaded`
held the partition itself, and a `Movie` partition is hundreds of bytes before any of its strings'
contents. So each shard's map was a table of hundreds of bytes a bucket, sized for its peak.

The budget counts each loaded partition's measured size (`deepsize`), which includes its inline
bytes once. It does not count the empty buckets, the capacity kept after evictions, or the old
table that exists beside the new one while it resizes.

## Evidence

**Profiled on the lab.** A build with the system allocator and frame pointers ran on hyperion alone,
and BCC's `memleak`, recording every allocation of a mebibyte or more for 90 s of load, found:

```text
total outstanding >=1MiB: 3493 MB
  1513.1 MB    6 allocs  MaybeLoaded < UnsortedTable < apply_command
  1513.1 MB    6 allocs  MaybeLoaded < UnsortedTable < apply_command
   134.2 MB    1 allocs  WalInner < RaftCore
    53.5 MB    6 allocs  Lru < UnsortedTable
```

**Tested** by `a_maybe_loaded_partition_is_as_small_as_its_pointer` (`shoal-core`,
`tables/partitions.rs`), written with the fix: a `MaybeLoaded` is no bigger than its archive
handle and a tag (72 bytes), where the test row's partition alone is 88. The profile is the
reproduction. The test pins the size that fixes it.

## The fix

`MaybeLoaded::Loaded` holds `Box<P>`. A bucket holds a pointer and a generation, or the
`Accessible` arm's buffer handle, whatever the row. The partition lives on the heap, where its
size was always counted. Thirty construction sites box their partition, and one that moved a
partition out dereferences the box.

## Alternatives rejected

- **Box the map's values** (`HashMap<u64, Box<MaybeLoaded<P>>>`). Same bucket size, but every
  pattern match on a looked-up value would have had to dereference first. Boxing inside the enum
  leaves the matches as they were.
- **Shrink the map after an eviction.** It bounds the capacity kept after a peak, but not the
  inline row in every occupied bucket, and a shrink is a rehash on the shard loop.
- **Count the map's capacity in the budget.** Accounting for bytes the design could simply stop
  spending.

## Invariants to uphold

- **Nothing row-sized is stored inline in a map sized by partition count.** A new arm of
  `MaybeLoaded`, or a new per-partition map, keeps its row behind a pointer.
- **Whatever serializes a loaded partition dereferences the box.** `rkyv::to_bytes(partition)` on
  a `&Box<P>` compiles and archives a box: a relative pointer, not the rows. The first cut of this
  fix did exactly that in both tables' snapshot cut (`&**partition` now), and an ephemeral
  table's copy on a returning node came back empty. `returning_node_catches_up_by_log_or_snapshot`
  is what caught it.

## Still open

- The archive map (`to_archive`) still holds an entry per partition the shard has *ever* archived,
  about 49 bytes each, and none of it is counted: at the lab's 31 million partitions a node, about
  2.5 GB. It is bounded now only because [#149](node-memory-budget.md) judges a node by its
  resident memory. Filed in [todos](../todos.md).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_maybe_loaded_partition_is_as_small_as_its_pointer` (`shoal-core`, `tables/partitions.rs`) | A loaded partition is inline in every bucket of its table's map again |
| `returning_node_catches_up_by_log_or_snapshot` (`shoal/tests/cluster_fixture.rs`) | A snapshot cut archives the box instead of the partition, and an ephemeral table installs nothing |

## Related

- [Resolved #149](node-memory-budget.md), the node's budget this was found under.
- [Partitions](../../tables/partitions.md#sizes), how a partition's size is measured.
