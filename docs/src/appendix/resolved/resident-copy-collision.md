# 30, 120, 121. A partition read that landed on a copy already in memory

Three items with one cause. Item 30 was filed from reading the sorted table's `load_partition`.
Items 120 and 121 were found by reading the code around item 30 and never appeared on
[Known Issues](../known-issues.md): 120 is the unsorted table's version of the same arm, and 121
is the duplicate read that lets either arm be reached. All three were reproduced before they were
fixed, and all three were fixed in one change.

## Symptom

A partition read from disk could land on a partition that was already resident as an archive
(`MaybeLoaded::Accessible`). The two tables handled that case in opposite ways, and both were
wrong:

- **Sorted (item 30).** The read was thrown away without a word. The `Occupied` arm was an
  `if let MaybeLoaded::Loaded`, so an `Accessible` entry did not match it. Nothing was logged
  and memory was not touched. That result was correct as long as the two copies were identical,
  and nothing checked whether they were.
- **Unsorted (item 120).** The resident archive was **replaced** by the read, and the shard's
  memory counter was charged for the read **without the replaced archive being taken off**.
  Every collision added a whole partition to the counter that no partition held. Nothing ever
  subtracted it, so each one pushed the shard closer to evicting data it had room for. A read
  that differed from the resident copy also changed the row the partition answered with,
  again without a word.

Neither symptom could happen on a standalone server. Both could happen on a cluster node.

## Cause

Two reads of one partition were in flight at once (item 121). A table remembers the reads it
has asked for in two places:

- `blocked`, the queries parked on a read, which `block_on_load` fills;
- `loading`, the reads a replicated apply asked for through `request_load`
  ([F40](../../features/replication.md)), which parks no query.

`request_load` checked both before asking for a read. `block_on_load` checked only `blocked`. So
this sequence produced two reads:

1. An apply needs a partition that is not resident. `request_load` asks for a read and records
   it in `loading`.
2. A get for the same partition arrives before that read lands. `blocked` is empty, so
   `block_on_load` asks for a second read and parks the get.
3. The apply's read lands. The entry is `Vacant`, so the archive becomes resident as
   `Accessible`, and the get is released with it.
4. The get's read lands on the `Accessible` entry. This is the case items 30 and 120 got wrong.

Only `request_load` ever fills `loading`, and only a replicated apply calls it, which is why this
needs a cluster. Even there it needs the get to arrive within the window of one disk read.

The two arms were written on opposite assumptions. The unsorted comment said an `Accessible`
entry "is just another copy of the same archive extent" and so could be overwritten. The sorted
arm treated it as not worth matching at all. Both copies being the same extent is the reason to
**keep** the resident copy: it costs nothing and queries may already have answered from it. It
is not a reason to replace it, and it is not a reason to skip the check.

## Evidence

**Reproduced**, before the fix, by the six tests in `shoal/tests/resident_reads.rs`. They build
a shard's tables the way `Shard::new` does, on a glommio executor of their own, over the
storage a real server left behind. Every partition is archived and evicted. The tests then drive
`request_load`, `handle` and `load_partition` directly. A duplicate read is delivered as a clone
of the first `LoadedPartition`, and a divergent one is a read of another partition relabelled.
That is what the race does, but it happens on demand. Against the unfixed tree five of the six
failed:

```text
test a_divergent_read_of_a_resident_sorted_partition_is_reported ... FAILED
test a_sorted_get_waits_on_an_applys_read_rather_than_asking_again ... FAILED
test an_unsorted_get_waits_on_an_applys_read_rather_than_asking_again ... FAILED
test a_second_read_of_a_resident_unsorted_partition_is_not_charged ... FAILED
test a_second_read_of_a_resident_sorted_partition_is_not_charged ... ok
test a_divergent_read_of_a_resident_unsorted_partition_is_reported ... FAILED

---- a_divergent_read_of_a_resident_sorted_partition_is_reported stdout ----
a divergent read of a resident partition was not reported
  left: 0
 right: 1
---- a_sorted_get_waits_on_an_applys_read_rather_than_asking_again stdout ----
a get asked for a read an apply had already asked for
  left: 2
 right: 1
---- an_unsorted_get_waits_on_an_applys_read_rather_than_asking_again stdout ----
a get asked for a read an apply had already asked for
  left: 2
 right: 1
---- a_second_read_of_a_resident_unsorted_partition_is_not_charged stdout ----
a second read of a resident partition changed the memory counter
  left: 64
 right: 32
---- a_divergent_read_of_a_resident_unsorted_partition_is_reported stdout ----
a divergent read replaced the resident copy
  left: (2, 7098283138315933468)
 right: (2, 12312040635512541377)
```

The unsorted counter doubles, from 32 to 64 bytes, for a partition held once. Each table asks
for two reads where one was in flight. The sorted table stays silent about a copy that
disagrees, and the unsorted table's digest moves because it swapped the row. The sorted test that
passed is the case item 30 said was harmless, and it was: a duplicate read of the same bytes cost
the sorted table nothing. With the fix, all six pass.

The race itself, an apply's read and a get's read in flight together on a cluster node, was
established by reading the source. The harness reproduces each step of it and not the timing.

## The fix

**One read per partition (item 121).** `block_on_load` in both tables now checks `loading`
after `blocked`. A get that finds an apply's read in flight parks in `blocked` behind it and
asks for nothing. That read's landing drains `blocked` like any other, and a failed read
releases it through `fail_partition` like any other. The `skip_disk` check still comes first, so
a query released by a failed read still answers without reading again.

**Both arms named, one rule (items 30 and 120).** Both tables' `Occupied` arms are now an
exhaustive `match`. `Accessible` calls one shared helper, `settle_resident_read`
(`.../tables/persistent.rs`). It keeps the resident archive, drops the read, and charges
nothing. It compares the two copies byte for byte. If they agree it logs at `DEBUG`. If they
disagree it logs at `ERROR`, with both lengths. `Loaded` keeps what each table did before: the
sorted table merges the disk copy under its in-memory rows, and the unsorted table keeps its
newer row or tombstone.

The helper does not pop the partition from the LRU, and the first landing's arm still does.
After the `blocked` check, a second landing normally has no parked queries to release. Popping
an evictable archive that no query will re-mark would make it permanently unevictable.

## Alternatives rejected

**Overwrite and subtract.** Keeping the unsorted table's overwrite but crediting the replaced
archive's size would fix the counter. But it would still swap a copy that queries may have
answered from for one nobody has compared with it. It would also validate the same bytes a
second time for nothing.

**Deduplicate only, and leave the arms.** With `block_on_load` fixed, neither arm is reached by
the race this page describes. But a snapshot install's stale-read re-issue and whatever path is
added next can still land a read on a resident archive. An arm that is wrong when reached is
still wrong. Item 30's own fix direction was to name the arm, because a pattern that quietly
does not match is how it went unnoticed.

**Merge an archive into an archive.** Deserializing both copies and merging them, as the
`Loaded` arm does, would handle a divergence rather than report it. But there is nothing to
merge between two copies of one extent, and when they disagree, a merge picks a winner
silently. That is exactly the silence item 30 was about.

**`debug_assert_eq!` instead of a logged comparison.** A release build would say nothing, and a
debug build would panic the shard. That leaves every query parked on it, the outcome
[Resolved #16, 51](partition-load-failure.md) exists to prevent. The comparison is one `memcmp`
on a path that runs only after a race, so it costs nothing in practice.

**Deduplicate inside the storage engine.** `FileSystem::load_partition` could drop a request for
a partition already queued. But `loading` and `blocked` are the table's records of what it asked
for, and a stale read after a snapshot install depends on them (`self.stale`). A second record
of the same thing, in the engine, would drift from them.

## Invariants to uphold

- **An `Accessible` entry is never written to in place.** A write turns it into `Loaded` first.
  That is what makes a resident archive's bytes the archive's bytes, and what lets the resident
  copy win without being re-read. A path that edits an archive where it lies breaks the reason
  the comparison expects equality.
- **Every read a table asks for is recorded in `loading` or `blocked` until it lands or fails.**
  `block_on_load` and `request_load` both check both. A third path that asks for a read must
  check both too, or it reopens item 121.
- **Every landing and every failure drains `blocked` for its partition.** That is what makes it
  safe for a query to park on `loading` alone: `load_partition` and `fail_partition` both remove
  the whole `blocked` entry, whichever read it was asked for.
- **A read landing on a resident archive changes neither the entry, the counter nor the LRU.**
  Only the first landing (the `Vacant` arm) charges and pops.

## Still open

- **The two arms still account differently** ([Memory and Eviction](../../tables/memory-and-eviction.md)):
  an `Accessible` entry is charged by its archive bytes and a `Loaded` one by `deep_size_of`.
  That is drift, bounded and unchanged by this fix, and noted on
  [Resolved #6](memory-accounting.md#invariants-to-uphold).
- **A divergence is reported, not repaired.** If the `ERROR` event ever fires, the resident copy
  is still the one served. That is the conservative choice, but it answers from a copy that
  disagrees with its own archive. [F44](../../features/repair.md)'s scrub is what would
  establish which copy is right.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_second_read_of_a_resident_unsorted_partition_is_not_charged` | `shoal/tests/resident_reads.rs` | Item 120: the counter reads 64 for a 32 byte partition held once |
| `a_divergent_read_of_a_resident_unsorted_partition_is_reported` | `shoal/tests/resident_reads.rs` | Item 120: the resident row is replaced (the digest moves) and nothing is logged |
| `a_divergent_read_of_a_resident_sorted_partition_is_reported` | `shoal/tests/resident_reads.rs` | Item 30: no `ERROR` event for a read that disagrees with the resident copy |
| `a_second_read_of_a_resident_sorted_partition_is_not_charged` | `shoal/tests/resident_reads.rs` | Nothing today. It guards the sorted arm against picking up item 120's charge, and against reporting a duplicate that agrees |
| `a_sorted_get_waits_on_an_applys_read_rather_than_asking_again` | `shoal/tests/resident_reads.rs` | Item 121: two read requests on the loader channel where one was in flight |
| `an_unsorted_get_waits_on_an_applys_read_rather_than_asking_again` | `shoal/tests/resident_reads.rs` | Item 121, unsorted half. Both dedupe tests also check that the apply's read releases the parked get |

The harness is the first test that builds a shard's tables without a shard. A test body returns
its failure rather than panicking, because a panic on a glommio executor thread aborts the whole
test binary while it unwinds.

## Related

- [Resolved #31](multi-log-recovery.md), the same question answered for recovery, by removing
  the collision rather than handling it.
- [Resolved #6](memory-accounting.md), the counter the unsorted arm was inflating.
- [Resolved #16, 51](partition-load-failure.md), `fail_partition` and why a parked query must
  always be released.
- [F40. Replication](../../features/replication.md), which introduced `request_load` and `loading`.
- [F43. Node recovery](../../features/node-recovery.md), the `stale` re-issue, the other path that
  can land a read on a resident partition.
- [Query Execution](../../tables/query-execution.md#merging-the-loaded-partition), the merge this
  sits beside.
