# 5. Deleted rows came back

Originally filed as *Pruned partitions leak a stale archive map entry*, which turned out to be
one of three ways the same thing happened. A row was deleted, the delete was acknowledged, and
some time later the row was readable again.

## Symptom

`DELETE` returned success, `EXISTS` returned `false`, and then — after a restart, or after the
shard came under memory pressure — the row was back. No error was logged, because nothing had
failed. Every layer did exactly what it was told.

## Cause

Three separate defects, all of them a version of *the copy on disk outlived the thing that was
hiding it*.

### A pruned partition kept its archive entry

Compaction dropped an emptied partition from `loaded` and stopped there:

```rust
if let ShouldPrune::Yes = T::apply_intents(&mut self.loaded, partition, intents) {
    self.loaded.remove(&partition);
    // TODO: does anything else need to be done to remove this partition
    // from archive maps?
}
```

The in-code TODO was right to worry. The partition was never rewritten, but its old
`ArchiveEntry` stayed in `to_archive`, still pointing at the pre-delete copy in the old
archive. Once the delete intent had been compacted away, nothing anywhere said the row was
gone — except a map entry that said where to find it.

### Sorted mutations never refreshed their partition's generation

A partition may only be evicted once its changes are in an archive. That is the generation
check:

```rust
pub fn is_evictable(&self, flushed_generation: u64) -> bool {
    match self {
        Self::Loaded { generation, .. } => *generation <= flushed_generation,
        Self::Accessible(_) => true,
    }
}
```

`shoal-core/src/server/tables/partitions.rs:46-51`

Sorted `insert`, `delete`, and `update` all bound `MaybeLoaded::Loaded { partition, .. }` and
mutated through it, so the stored `generation` kept whatever value it had when the partition
was first created or deserialized — forever. A partition written in generation 3, compacted,
and then deleted from in generation 7 still claimed 3, so any later `MarkEvictable` marked it
evictable while its delete intent was still in an open log. Unsorted tables replace the whole
`MaybeLoaded` on every mutation (`.../persistent/unsorted.rs:408`, `:612`, `:692`), so they
never had this half.

It fired constantly rather than occasionally, because `FileSystemCompactor::loaded` was never
cleared between jobs — only `changes`, `entries`, and `removals` were drained — and
`write_partition` iterates it. Every partition a compactor had ever touched was rewritten and
re-added to `to_mark` on every subsequent compaction, so stale-generation partitions were
offered up for eviction over and over.

### The load path marked partitions evictable at the open generation

This one needed no stale generation at all, and it affected **both** table types.

`load_partition` returned `self.generation` — the generation currently accepting writes — and
the generated dispatch turned it straight into a `MarkEvictable`
(`shoal-derive/src/traits/db.rs:139-166`):

```rust
if let Some((unblocked, generation)) = self.#field_ident.load_partition(loaded_kinds.loaded).await {
    let mark_evict_msg = ServerMsg::MarkEvictable { generation, table, partitions: vec![id] };
    for (meta, unwrapped) in unblocked { /* re-queue the blocked query */ }
    shard_local_tx.send(mark_evict_msg).await.unwrap();
}
```

The compactor's `MarkEvictable` carries the generation of the log it has just sealed **and
compacted** (`.../fs.rs:388`, incremented afterwards at `:391`), so there `gen <= flushed`
really does mean durable. The load path's did not: it handed out the generation the query it
was about to release would write in. The check compared a generation against itself and passed.

| Step | State |
| --- | --- |
| 1 | Partition lives only in an archive, nothing resident |
| 2 | `DELETE` arrives, is parked in `blocked`, the loader is asked for the partition |
| 3 | `load_partition` installs `Accessible` and returns `(blocked queries, self.generation = G)` |
| 4 | The delete replays: deserialize, tombstone, install `Loaded { generation: G }`, intent → **open log G** |
| 5 | `MarkEvictable { generation: G }` arrives → `G <= G` → into the LRU |
| 6 | Memory pressure → `evict` drops the partition and its tombstone with it |
| 7 | The next read takes the `Vacant` arm, installs the pre-delete archive extent — **the row is back** |

Sorted archives never contain tombstones, because compaction removes deleted rows outright
(`.../persistent/sorted.rs:1383-1386`), so there is nothing on disk to shadow the resurrected row
until the log holding the delete is compacted.

## Evidence

**The first defect was confirmed, not inferred.** Reproduced with a temporary integration test
against a real server:

| Session | Action | `exists` |
| --- | --- | --- |
| 1 | insert row, shut down | — |
| 2 | restart (insert compacts into an archive), delete row | `false` ✅ |
| 3 | restart — delete intent replayed from the log | `false` ✅ |
| 4 | restart — delete intent has been compacted away | **`true` ❌** |

By session 4 the delete intent had been compacted and its log deleted, the tombstone existed
nowhere, and the map still pointed at the original archive extent. The row came back.

**The other two were confirmed the same way**, by tests that are now permanent. Against the
pre-fix tree, with a shard held under memory pressure:

```
@@ exists SortedExists { partition_keys: [3966638079184949396], ... } -> true
thread 'delete_survives_eviction' panicked: assertion failed: !exists
```

and, for the stale generation, 200 acknowledged inserts into one partition read back as 198:

```
assertion `left == right` failed
  left: 198
 right: 200
```

The unsorted twin of the first assertion failed identically, which is what showed that the
"unsorted path is covered" note this entry used to carry was wrong.

## The fix

**Pruned partitions lose their archive entry.** `MapIntent` has a `Remove(u64)` variant. On
`ShouldPrune::Yes` the compactor records the key (`.../fs/compactor.rs:201-210`), writes a
removal intent to the map intent log, and drops the entry from `to_archive` after the sync —
the same publish-after-sync order `set_partition` follows (`.../fs/compactor.rs:245-272`).
Pruned keys also join `to_mark`, so the tombstone shadowing them becomes evictable in the same
generation the entry disappears.

**Both tables track a real flushed generation.** `flushed_generation` is a second counter
beside `generation` (`.../persistent/sorted.rs:100-107`, `.../persistent/unsorted.rs:94-101`),
advanced only in `mark_evictable` from the generation the compactor reports
(`.../persistent/sorted.rs:1054`, `.../persistent/unsorted.rs:765`). `load_partition` returns
*that* instead of the open generation (`.../persistent/sorted.rs:305`,
`.../persistent/unsorted.rs:286`), so releasing a blocked query no longer authorises evicting
what it is about to write.

Generation counters now start at 1 (`.../fs.rs:294-295`) so that 0 can mean "nothing has been
compacted yet", and each table adopts the generation its forced startup compaction opens
(`.../persistent/sorted.rs:218-221`) rather than assuming it is still writing into the
generation it replayed. `compact_intent` reports a generation even when the log compacted to
nothing (`.../fs/compactor.rs:324-374`), so an empty generation advances the watermark instead
of pinning everything tagged with it.

**Sorted mutations refresh their generation.** Each in-place mutation that commits an intent
now assigns it (`.../persistent/sorted.rs:371`, `:742`, `:914`), on the branches that write and
not on the ones that answer "no such row".

**The compactor forgets what it has written.** `self.loaded.clear()` after the write loop
(`.../fs/compactor.rs:254`), so a job rewrites only the partitions its own log changed.

**Sorted tombstones have a bounded lifetime.** A tombstone is needed exactly until the log
holding its `Delete` intent has been compacted; after that the archive behind it no longer
contains the row. `SortedPartition` counts its tombstones
(`.../tables/partitions.rs:279-286`) and `mark_evictable` sweeps them from any partition whose
generation is covered (`.../persistent/sorted.rs:1059-1066`). Without this a hot partition that
is never evicted accumulates tombstones forever, each one occupying a `BTreeMap` slot while
contributing nothing to the partition's accounted size.

**Sorted queries queue behind an in-flight load.** `get`, `exists`, `delete`, and `update` each
used to issue their own loader request for the same key, so a partition with N waiters was read
N times. They now check `blocked` first (`.../persistent/sorted.rs:432`, `:504`, `:591`,
`:660`, `:749`, `:837`, `:924`, `:1008`), the way unsorted's `block_on_load` already did
(`.../persistent/unsorted.rs:300-337`).

## Alternatives rejected

**Have the load path pass `self.generation - 1`.** A rotated log is not a compacted log. The
compaction job is queued and runs on the medium priority queue, so the previous generation may
still be entirely in an intent log when the next one opens. This narrows the window instead of
closing it, which is the worst outcome: a race that reproduces once a week.

**Suppress the load path's `MarkEvictable` entirely.** The partition is popped from the LRU
while it is being loaded, and this message is what puts it back. Dropping it would pin every
partition a read-only workload ever touched, and read-only is exactly the workload that faults
the most partitions in.

**Write tombstones into sorted archives.** It would make an evicted tombstone recoverable, but
it moves the reclamation problem instead of solving it: the archive grows with every delete and
something still has to decide when a tombstone may be dropped. Compaction is already the point
where the row can simply be omitted.

**Recompute tombstone counts on demand instead of maintaining a counter.** `mark_evictable` can
be handed a thousand partitions at a time, and walking every row of each to discover there is
nothing to sweep is a per-generation scan of the whole resident set.

## Invariants to uphold

- **A partition holding changes that are not yet in an archive must carry the open
  generation.** Sorted partitions are mutated in place, so every mutating path has to assign
  `*generation = self.generation`; unsorted gets it for free by replacing the whole
  `MaybeLoaded`. A new sorted mutation path that forgets this is invisible until something is
  evicted under pressure.
- **`MarkEvictable` may only carry a generation whose log has been sealed and compacted.** The
  compactor is the only legitimate source of a new value. Everything else passes
  `flushed_generation` along.
- **Generation `0` means "nothing compacted yet" and no partition may ever carry it.** Both the
  storage engine and the tables start at 1.
- **Sorted archives never contain tombstones.** Compaction hard-removes deleted rows, so an
  evicted sorted tombstone is unrecoverable. That is why the eviction gate is load-bearing
  rather than an optimisation.
- **A tombstone may only be dropped once the archive behind it no longer holds the row** — that
  is, under the generation gate, never on a timer, a size threshold, or an LRU decision.
- **Publish after sync.** The in-memory map is repointed only after both the data and the map
  intent are on disk, and `MapIntent::Remove` follows the same order as `Entry`.
- **A pruned key must join `to_mark` in the same generation its archive entry disappears**, or
  the tombstone shadowing it is pinned in memory for good.
- **A loader may only ever hold a sender for its own shard.** The load dedup work touches the
  blocked-query path, which is adjacent to the
  [unsafe `Send` invariant](../known-issues.md#unsafe-send-invariant) on `ServerMsg::Partition`.

## Tests

| Test | Fails without |
| --- | --- |
| `delete_when_not_resident` (`shoal/tests/persistent_unsorted_table.rs`) | The compactor's `MapIntent::Remove` half — `assertion failed: !exists` on the fourth restart |
| `delete_when_not_resident` (`shoal/tests/persistent_sorted_table.rs`) | The same, for sorted partitions that prune to empty |
| `delete_survives_eviction` (both files) | The flushed generation. The tombstone is evicted and the archive copy is faulted straight back in |
| `writes_survive_eviction` (`shoal/tests/persistent_sorted_table.rs`) | The generation refresh on sorted mutations. Acknowledged inserts disappear from reads |
| `remove_intent_drops_an_entry` (`.../fs/map.rs`) | Replay of a removal intent |
| `tombstones_are_counted`, `dropping_tombstones_keeps_live_rows`, `merging_from_disk_keeps_tombstones` (`.../tables/partitions.rs`) | The tombstone bookkeeping the sweep depends on |

The eviction tests run against a config with `resources.memory` set to one byte
(`utils::build_pressured_config`, `shoal/tests/utils.rs`), so every shard loop iteration evicts
everything the LRU is holding. Nothing else in the suite forces an eviction, which is why this
class of bug survived so long: the older tests all end with a clean shutdown, and shutdown
flushes.

Note the existing `delete_survives_restart` (`shoal/tests/persistent_sorted_table.rs`) still
does not catch any of this: despite its name it deletes and checks `exists` in the *same*
session and never restarts afterwards.

## Related

- [Compaction](../../storage/compaction.md) — generations, pruning, and the publish-after-sync ordering
- [Partitions](../../tables/partitions.md) — tombstones and `MaybeLoaded`
- [Memory and Eviction](../../tables/memory-and-eviction.md) — what marks a partition evictable
- [Unsorted updates and deletes never consult disk](unsorted-disk-consultation.md) — the fix that made this one reachable
