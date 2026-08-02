# 9. Orphaned update intents panicked, then were dropped silently

Fixed in two halves. The panics went first — they lived on the unsorted path, and the sorted
path never had an equivalent because its `apply_intents` has always seeded from the partition's
archive copy. What was left after that was the observability half: an intent whose base row was
genuinely gone was dropped with only a `warn!`, and nothing counted it. That half is now fixed
too, and this page carries both.

## Symptom

**Before the first half:** a shard that would not start. Both panics were on the startup path,
which is the worst place for one — a shard that cannot start cannot be recovered without
deleting data.

**After it:** a shard that started, and no way to find out what it had thrown away doing so.
The question an operator actually has after a crash is "did this restart lose anything?", and
the only way to answer it was to grep logs for a `warn!` on every shard. Nothing counted the
drops, nothing aggregated them, and on the sorted path nothing even logged them.

## Cause

Two sites for the panics, sharing one assumption — that an update always has a base row within
reach:

```rust
// TODO handling a partition missing
None => panic!("Missing partition?"),
```

fired during startup replay when an update's base partition was not resident and `scan`'s
`load_partition_direct` found nothing.

```rust
UnsortedIntents::Update(update) => match &mut maybe_partition {
    Some(partition) => partition.update(&update),
    None => panic!("Applying update to no partition?"),
},
```

fired during compaction when a log contained an update whose insert had been compacted in an
earlier generation. `apply_intents` for unsorted tables started from `None` rather than from the
partition's current archive copy, so a perfectly ordinary sequence — insert in generation 3,
compact, update in generation 4 — crashed the shard when generation 4 was compacted.

The second was made freshly reachable by [item 4](unsorted-disk-consultation.md): once an update
against an archive-only partition could succeed, it wrote exactly the orphaned intent that
panicked. The two had to be fixed together.

For the second half the cause was simply that nothing counted. Three separate places discarded
data during recovery and each one only logged, at most:

| Site | What it discarded |
| --- | --- |
| `.../persistent/unsorted.rs`, replay | An update whose partition was in no archive — a `warn!` |
| `.../persistent/sorted.rs`, replay | The same thing, via `partition.update(&update).unwrap_or(0)` — **no `warn!` at all** |
| `.../persistent/unsorted.rs`, `apply_intents` | The same thing during compaction — a `warn!` |
| `.../storage/fs.rs`, replay loop | An entry that could not be replayed — a `warn!` |
| `.../storage/fs/reader.rs` | Everything after a torn or corrupt entry — a `warn!`, and the caller could not tell this apart from a clean end of log |

## Evidence

The panics were established by reading the source. The observability half was established by
running the fixed server against a deliberately damaged log: the `tmdb` example was started on a
two shard config, stopped, a single bit was flipped a megabyte into `Movie/intents/Shard-0-active`,
and it was started again. Before the fix the only output was the reader's `warn!` about a
checksum mismatch, on a line that says "treating as end of intent log" and does not say how much
was after it. After the fix the same run also produces, once per shard:

```
INFO Shard::init: msg="Recovery complete" shard="Shard-1" updates_after_delete=0
WARN Shard::init: msg="Recovery discarded data" shard="Shard-0" orphaned_updates=0
     unreplayable_entries=0 truncated_logs=1 updates_after_delete=0
```

That is the whole of what this half was for: one line per shard, at a level that is hard to
miss, that answers the question directly.

## The fix

**The panics.** `UnsortedPartition::apply_intents` seeds from `loaded.remove(&key)`, matching the
sorted implementation:

```rust
// start from this partitions current archive copy if it has one, since an
// update can target a row whose insert was compacted generations ago
let mut maybe_partition = loaded.remove(&key);
```

and both sites warn and skip the intent instead of panicking
(`.../persistent/unsorted.rs`, in `replay` and in `apply_intents`).

**The counts.** A `RecoveryStats` in `.../tables/storage.rs`, next to `FlushProgress` and
`ShouldPrune` — a plain `Copy` struct, because each table owns its own and they are summed on
demand rather than shared through an `Arc<RefCell<_>>`:

```rust
pub struct RecoveryStats {
    /// Update intents whose base partition was in no archive and in no earlier log
    pub orphaned_updates: u64,
    /// Update intents that targeted a row a delete had already tombstoned
    pub updates_after_delete: u64,
    /// Intent log entries that could not be replayed at all
    pub unreplayable_entries: u64,
    /// Intent logs whose reader stopped early on a truncated or corrupt tail
    pub truncated_logs: u64,
}
```

`IntentReadSupport::replay` and `::apply_intents` take a `&mut RecoveryStats`,
`StorageSupport::read_intents` returns one, and each persistent table keeps what its own
recovery discarded. `ShoalDatabase::recovery_stats` — generated by the `#[db]` derive, folding
`merge` over the table fields — sums them, and `Shard::init` emits one event per shard once
every table has been replayed.

**Three of those counters mean loss and one does not**, which is the point of splitting them.
An update that lands on a row a delete already tombstoned was *meant* to be dropped, and
counting it as loss would make the numbers that do mean loss worthless. `is_clean` ignores
`updates_after_delete`, and only a not-clean recovery is reported at `WARN`.

**The sorted path was made to agree with the unsorted one.** `SortedPartition::update` answers
`None` both for a tombstoned row and for a row that was never there, and the old
`.unwrap_or(0)` collapsed the two. A `replay_update` helper now classifies the miss by looking
at what is actually under the sort key, and warns on the half that means loss.

**A damaged tail is now distinguishable from a clean one.** `IntentLogReader` grew a
`truncated` flag, set on each of the branches that give up on a damaged entry. Finding where to
set it turned up a bug in the reader itself, described under *Invariants* below.

**Compaction reports separately.** `compact_if_needed(true)` only *dispatches* a job — the
compactor is its own task — so its drops cannot be in a synchronous startup summary. It counts
into its own `RecoveryStats` per job and emits its own `WARN`, which keeps working for the life
of the shard rather than only at startup.

## Alternatives rejected

**Prevent the orphans instead of counting them, by loading an update's backing partition before
replaying it.** This was the first thing tried and it is not possible, because that prescan
already exists. `scan` runs as a complete pass over a log before its replay pass and calls
`load_partition_direct` for every `Update`, and the archive map it consults is fully built by
`ArchiveMap::new` inside `FileSystem::new`, before `read_intents` is ever called. An orphan
therefore means `map.find_partition` had no entry anywhere — the base row is in no archive — and
no amount of loading conjures data that is not on disk. What the question *did* turn up is that
the prescan was per-log and clobbered, which is [item 31](multi-log-recovery.md), fixed
alongside this.

**Keep the panic but make it a `ServerError`.** It is the same outcome — a shard that will not
start — dressed up. An intent whose base row is genuinely gone is a data loss that has already
happened; refusing to start does not undo it and does prevent recovering everything else.

**A config flag that refuses to start after any loss.** Considered and dropped for the same
reason, plus a worse one: a torn tail is what an *ordinary* crash leaves behind, so this would
block startup on the common case. It would have to default off, and a safety net that defaults
off is a footgun with a manual.

**Thread an `Arc<RefCell<RecoveryStats>>` down through `D::new` the way `memory_usage` is.**
That is the established pattern for shard-wide state, but it costs a signature change to
`ShoalDatabase::new`, to the derive that generates it, and to every table constructor, and buys
nothing: recovery finishes before `Shard::init` runs, so summing owned per-table counters on
demand gives the same answer. The accessor is additive, and nothing else had to move.

**Report one total for the whole pool rather than one line per shard.** There is no moment to
report it at. `ShoalPool::start` spawns its shard threads and returns without joining them, so
no point exists at which every shard has finished starting. Per-shard is what can honestly be
said, and it is in *Limitations* rather than left to be discovered.

## Invariants to uphold

- **Compaction must seed from the partition's current archive copy.** An intent log holds a
  delta, not a whole partition, and the insert an update refers to may be generations old.
- **Startup must not panic on a malformed or orphaned intent.** The startup path is the recovery
  path; a crash there is unrecoverable without deleting data.
- **`Shard::init` must stay downstream of `D::new`.** Tables are built — and every intent log
  replayed — inside `Shard::new`. If table construction ever moves later than `init`, the summary
  becomes a report of nothing.
- **A drop that is not loss must never be counted as loss.** `updates_after_delete` exists so
  that `orphaned_updates` can be trusted. Both `apply_intents` implementations track which rows
  the batch itself deleted, because a compaction-time delete *removes* the row rather than
  tombstoning it, so without that bookkeeping a correctly dropped update is indistinguishable
  from one whose insert was lost.
- **Only a damaged tail sets `truncated`.** A zero size header is how a partly filled log ends
  and a `PAD_SENTINEL` region is how a partial flush is aligned; flagging either would report
  data loss on every clean startup. **The converse also has to hold**, and originally did not:
  the `size + 8 > remaining` branch was documented and commented as padding, but a torn size
  header lands there too. Reading past the end of a file inside an already aligned block zero
  fills rather than returning a short read, so a half written header comes back as a nonzero
  size that cannot fit. That branch now distinguishes the two by whether the size is zero, and
  warns and flags when it is not. This was found by writing the test for the flag and watching
  the truncated-header case fail.

## Still open

**There is still no metric, only an event.** The counts are reachable in-process through
`ShoalDatabase::recovery_stats`, which is the hook a real metrics surface would read, but no
such surface exists. Filed in [TODOs](../todos.md#observability).

**Nothing aggregates across shards.** See *Alternatives rejected*; also filed in
[TODOs](../todos.md#observability).

**The summary event has no automated test.** Tracing is only initialized by the example binary,
not by `ShoalPool::start`, so the integration tests cannot observe events at all. The counting
that feeds the event is unit tested; the event itself was verified by hand, as described under
*Evidence*. Noted in [Test Coverage](../test-coverage.md).

**Mid-log corruption still discards the rest of the log.** It is now counted and reported, which
is what this item asked for, but the data after a flipped bit is still dropped. See
[Recovery](../../storage/recovery.md#truncation-and-corruption).

## Tests

| Test | Fails without |
| --- | --- |
| `reader_flags_damaged_tails` (`.../storage/fs/tests.rs`) | The `truncated` flag. A torn header, an entry claiming more than remains, an oversized size header and a bad checksum all stop the read silently |
| `reader_does_not_flag_healthy_tails` (`.../storage/fs/tests.rs`) | The other half, and the one that matters more. An empty log, a fully read log, a zero size header and a pad region must not be flagged, or every clean startup reports data loss |
| `recovery_stats_is_clean_ignores_deleted_rows` (`.../tables/storage.rs`) | The split between loss and not-loss. An update onto a deleted row makes a healthy shard log `WARN` |
| `recovery_stats_merge_accumulates` (`.../tables/storage.rs`) | Per-table summing, and the saturating add — a wrapped counter reads as a clean recovery |
| `replay_update_separates_deleted_rows_from_lost_ones` (`.../tables/partitions.rs`) | The sorted classification. A tombstoned row and a missing row are both counted as loss |
| `replay_update_applies_to_a_live_row` (`.../tables/partitions.rs`) | Nothing on its own — it pins that the common path still applies the update and counts nothing |
| `apply_intents_counts_an_orphaned_update` (`.../tables/partitions.rs`) | Counting on the compaction path |
| `apply_intents_separates_deleted_partitions_from_lost_ones` (`.../tables/partitions.rs`) | The `deleted_here` bookkeeping. An insert, delete and update in one batch reports data loss that did not happen |
| `apply_intents_counts_nothing_when_it_applies` (`.../tables/partitions.rs`) | Nothing on its own — it pins that an ordinary compaction stays clean |
| `update_when_not_resident` (`shoal/tests/persistent_unsorted_table.rs`) | The original fix. The shard panics on startup |

## Related

- [Multi-log recovery discarded already-replayed intents](multi-log-recovery.md) — item 31, found
  by asking whether these orphans could be prevented, and fixed alongside this
- [Recovery](../../storage/recovery.md)
- [Compaction](../../storage/compaction.md#3-apply)
- [Observability](../../operations/observability.md)
