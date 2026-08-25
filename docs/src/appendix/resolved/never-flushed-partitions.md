# 80. A sorted partition that was never on disk asked storage about it on every get

## Symptom

A sorted partition that had only ever been written to asked its storage engine to load it on
**every** get, update and delete that touched it, for an answer that could not change: there was
no archive to load. The answer was always the same and always thrown away.

Nothing failed. The lookup is cheap — a `HashMap` probe on the archive map for the filesystem
engine, a `Ok(false)` for the ephemeral one — and the query answered correctly either way, out of
the rows the partition was already holding.

The second cost was the one that mattered. A partition that might still have rows on disk may not
be answered in place: [F27](../../features/grouped-responses.md) serializes a get's reply out of
the rows the partition is holding, and refuses to do so for a partition that is about to park on a
read. A flag that was set and never cleared made that refusal permanent, so **the sorted half of
[O2](../optimizations.md#o2-every-returned-row-is-copied-at-least-twice) was built, tested and
unreachable in the one case it was built for.** The unsorted table has no such flag and did take
the path.

The ephemeral sorted table is the sharp end of this. `NoStorage::load_partition` answers `false`
unconditionally — nothing is ever on disk, because nothing is ever written — so an ephemeral
sorted table asked a question with a compile-time answer once per query, forever, and never
borrowed a row in its life.

## Cause

`PersistentTable::block_on_load` asked storage to load a partition and, when told there was
nothing to load, returned `false` without recording it:

```rust
// if this partition has no data on disk then there is nothing to wait for
if !will_load {
    return false;
}
```

`SortedPartition::check_disk` starts `true` in `SortedPartition::new` and was only ever cleared by
`merge_from_disk`, by a partition arriving from a read, or by the `Accessible` → `Loaded`
conversion in `delete`. None of those happens when the answer was "there is nothing on disk", so
the flag stayed set for the life of the in-memory partition.

Two sibling sites lost the same answer. `insert` and `update` both deserialize an `Accessible`
partition into a `Loaded` one, and neither cleared `check_disk` afterwards — `delete` did, with a
comment saying exactly why. An accessible partition **is** the copy from disk, and
`partition_needs_disk` says so; but the flag those two wrote into the loaded partition came out of
the archive, where the compactor had serialized whatever `SortedPartition::new` left there. So a
partition that had been read from disk and then updated could go back to consulting disk, one
wasted read, until the merge cleared it again.

## Evidence

**Reproduced.** `shoal/tests/disk_lookups.rs` counts the `PersistentTable::block_on_load` spans a
process opens — that span is the lookup, and it is in the shipping code rather than added for the
test. Six gets over two never-flushed partitions, one in a persistent sorted table and one in an
ephemeral one, against the tree before the fix:

```
---- a_partition_that_was_never_on_disk_is_looked_up_once stdout ----
assertion `left == right` failed: a partition that storage said was not on disk was asked about again
  left: 6
 right: 2
```

Six, not two: one lookup per get, per partition, rather than one per partition. The count is
exact, which is what says the lookup happens every time rather than sometimes.

The item was originally filed from a probe — [F27](../../features/grouped-responses.md)'s
borrowing path, watched while `a_repeated_get_answers_the_same_rows_and_names_their_partition`
ran, and reached **zero** times against the sorted table and ten times across the unsorted suite.
That probe is not in the tree; the count above is what replaced it.

## The fix

`block_on_load` records the answer it was given:

```rust
// if this partition has no data on disk then there is nothing to wait for
if !will_load {
    // and nothing will be until this shard writes it, at which point what it writes
    // is what this partition is already holding - so remember this answer instead of
    // asking again on every query that touches this partition
    self.mark_absent_from_disk(partition_key);
    return false;
}
```

`mark_absent_from_disk` clears `check_disk` on the partition if this shard is holding one. It is a
method rather than three lines inline because the reasoning it depends on is longer than the code,
and belongs next to it.

The two conversion sites were brought in line with `delete`: `insert` and `update` now clear
`check_disk` when they turn an `Accessible` partition into a `Loaded` one, because that partition
is the copy from disk.

**Why clearing it is sound**, which is the question the item was filed on rather than fixed on:
can a partition acquire rows on disk that the in-memory copy does not already have? No, for a
partition that has been continuously resident since the answer was recorded.

- An archive is written by `FileSystemCompactor::write_partition`, out of `self.loaded` — this
  shard's own intent log replayed over **this partition's previous archive**. If storage said
  there was no archive, that second input is empty.
- Every intent in that log was applied to the in-memory partition when it was accepted, in the
  same `insert`/`update`/`delete` call that committed it. So the archive's keys are a subset of
  memory's, and `merge_from_disk` makes memory win a collision anyway.
- Archives are per shard (`ArchiveMap::new(shard_name, ...)`) and a partition belongs to one
  shard, so no other writer can put rows in this partition's archive.
- A partition dropped from memory takes the answer with it: `evict` removes the whole map entry,
  and the next write rebuilds it through `SortedPartition::new`, which assumes disk again.

## Alternatives rejected

**Clearing the flag at each call site instead.** `get`, `exists`, `delete` and `update` all read
`check_disk` and all call `block_on_load`. Four copies of the same three lines is how `delete`
came to clear the flag on its `Accessible` conversion while `insert` and `update` did not — the
defect this fix also had to repair. One callee that all four already go through is the only place
that cannot drift.

**Recording the answer for a partition that is not resident at all.** A get naming a partition
this shard has never held and that is not on disk still asks on every get. Recording that would
mean inserting an empty partition into the map to hold the flag, which puts an entry in the memory
accounting, in the LRU and in the eviction path for a partition that holds nothing — and one per
key any client ever asks about. The lookup stays. It is noted under **Still open** rather than
taken.

**Skipping the probe entirely for an engine that never has anything on disk.** Filed in
[TODOs](../todos.md) as part of a fast path through the ephemeral tables, and still the right
shape for that table — but it fixes one engine rather than the flag, and would have left the
persistent table asking on every get of a partition it had never flushed, which is the larger of
the two populations.

**Making `check_disk` `#[rkyv(with = Skip)]`.** This is the tidy version of the `insert`/`update`
repair: the field is serialized into archives today, where it means nothing, and a partition read
back from disk would then always deserialize it as `false` — which is exactly right. It changes
the archive format, so every archive written by an older build would be misread by a newer one.
Not worth a format break for a field three assignments already cover.

**Instrumenting `get_sealed` so a test could assert the borrowing path was taken directly.** That
would test the consequence rather than the cause, and it would put a span on the hot path this
fix exists to unlock. The lookup count is the same claim measured where it is free: a partition
that makes no lookups is a partition `can_answer_in_place` accepts, because
`partition_needs_disk` is the whole of what it asks about a partition.

## Invariants to uphold

- **`check_disk` may only be cleared by something that knows memory holds everything disk does.**
  Today that is four places: `merge_from_disk`, the three `Accessible` → `Loaded` conversions, and
  `mark_absent_from_disk`. A fifth needs the same argument made for it.
- **An archive may only be built from this shard's intents over this partition's previous
  archive.** The fix rests on this and nothing else. A compaction that could pull rows in from
  anywhere else — another shard's log, a repair stream, a restored backup — makes a cleared flag a
  lie, and a partition that was written while its shard believed disk was empty would answer
  short. Any such feature has to set `check_disk` back to `true` on every partition it touches.
- **A query released by a failed read must not clear the flag.** `block_on_load` returns on
  `meta.skip_disk` *before* it asks storage, so the failure path never reaches
  `mark_absent_from_disk`. A partition whose archive could not be read is still on disk, and
  saying otherwise would convert a read failure into silently missing rows. This is the same rule
  [Resolved #16, 51](partition-load-failure.md) and [Resolved #57](missing-archive.md) state from
  the other side.
- **Eviction is whole-partition.** Clearing the flag is only safe because a partition cannot lose
  *some* of its rows to memory pressure while keeping the entry that says disk is empty.
- **`SortedPartition::new` is the only place `check_disk` becomes true.** A partition that has
  been told disk is empty stays told for its whole life, so anything that wants the question asked
  again has to build a new partition or set the flag itself.

## Still open

- **A partition that is not resident and not on disk is still asked about on every get.** Getting
  a partition key nothing has ever written costs one archive-map probe per get, forever. See
  **Alternatives rejected** for what recording it would cost instead.
- **The ephemeral sorted table still asks once per partition.** Down from once per query, but
  `NoStorage::load_partition` is a function whose answer is known at compile time. The fast path
  in [TODOs](../todos.md) is what removes the last one.
- **This fix is not measured.** It makes the sorted table eligible for the borrowing path; what
  that is worth on the grid's read arms is a capture nobody has taken, and
  [F27](../../features/grouped-responses.md) predicted the macro layer would not move precisely
  because of this item. That prediction is now testable.

## Tests

| Test | What it catches |
| --- | --- |
| `a_partition_that_was_never_on_disk_is_looked_up_once` (`shoal/tests/disk_lookups.rs`) | The whole of it: six gets over two never-flushed sorted partitions, one persistent and one ephemeral, must produce exactly two `block_on_load` spans. Reverting `mark_absent_from_disk` gives six |
| `a_repeated_get_answers_the_same_rows_and_names_their_partition` (`shoal/tests/persistent_sorted_table.rs`) | That the borrowing path this unblocks answers what the copying path answered — the same rows, in the same order, with the same group index. It was written for that comparison and did not make it until now |
| `get_by_sort_key_reads_from_disk`, `get_by_range_reads_from_disk`, `projection_survives_a_blocked_disk_read` (`shoal/tests/persistent_sorted_table.rs`) | That a partition which really is on disk is still read. A `mark_absent_from_disk` that cleared the flag unconditionally rather than only on `!will_load` would answer every one of these short |
| `get_by_sort_key_spans_memory_and_disk`, `get_by_range_spans_memory_and_disk` (`shoal/tests/persistent_sorted_table.rs`) | The harder half of the same thing: a partition holding some of its rows in memory and the rest in an archive. These are what fail if the flag is cleared for a partition that was written *and* flushed |
| `delete_when_not_resident` (`shoal/tests/persistent_sorted_table.rs`) | That a write which parks on a read still parks. It reaches `block_on_load` through the arm that reads `check_disk` directly, rather than through `get` |
| `delete_survives_eviction`, `writes_survive_eviction`, `get_stops_at_its_limit_under_eviction` (`shoal/tests/persistent_sorted_table.rs`) | That an evicted partition asks again. The cleared flag is dropped with the map entry, and a partition rebuilt by `SortedPartition::new` assumes disk |

## Related

- [F27](../../features/grouped-responses.md) — the borrowing path this makes reachable for sorted
  tables, and whose Limitations section was written around this item
- [O2](../optimizations.md#o2-every-returned-row-is-copied-at-least-twice) — the entry F27 half
  closed; its sorted half was unreachable until now
- [Resolved #4](unsorted-disk-consultation.md) — the unsorted table's own `block_on_load`, and why
  there are two of them
- [Resolved #16, 51](partition-load-failure.md) and [Resolved #57](missing-archive.md) — why a
  *failed* read leaves `check_disk` true, which is the rule this fix had to route around
- [Resolved #31](multi-log-recovery.md) — the other place a cleared `check_disk` decides what a
  partition is allowed to forget
- [F9](../../features/ephemeral-tables.md) — the tables that ask a question with a compile-time
  answer, and the fast path still filed for them in [TODOs](../todos.md)
- [Item 81](../known-issues.md) — found on the way out of the same call: the stage flag that says
  a query waited on a disk read is defined, never set and never read
