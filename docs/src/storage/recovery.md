# Recovery

Recovery runs per table, per shard, during `PersistentSortedTable::new` /
`PersistentUnsortedTable::new` — before the shard accepts any connections:

```rust
table.recovery = table.storage.read_intents(conf, table.generation, &mut table.partitions, &mut table.memory_usage).await?;
table.storage.compact_if_needed::<R>(true).await?;
```

`shoal-core/src/server/tables/persistent/sorted.rs`

Two steps: replay every intent log into memory, then force a compaction so the replayed state
is written into archives and the logs can be discarded. `read_intents` returns a
[`RecoveryStats`](#what-recovery-discards) recording anything it had to throw away, which the
shard reports once it has finished starting.

## Replay order

Sealed logs first, oldest generation first, then the active log:

```
  archives (already compacted)      ← oldest state
        │
        ├── Shard-0-inactive-3      ← rotated, compaction never finished
        ├── Shard-0-inactive-4      ← rotated, compaction never finished
        └── Shard-0-active          ← newest state
                                    ▼ replayed last, wins
```

Generation ordering comes from parsing the suffix and sorting:

```rust
let prefix = format!("{shard_name}-inactive-");
if let Some(gen_str) = name.strip_prefix(&prefix) {
    if let Ok(gen) = gen_str.parse::<u64>() { inactive_logs.push((gen, entry.path())); }
}
inactive_logs.sort_by_key(|(gen, _)| *gen);
```

`.../fs.rs:188-209`

Ordering is essential — intents are not commutative. An insert followed by a delete in a later
generation must not be replayed the other way round.

A sealed log is the mark of a crash that interrupted compaction between `refresh` and the
compactor's `remove`. Finding one at startup is a diagnostic, and it is worth treating as one.

~~But not always, because an empty rotated log is never removed at all. A clean shutdown leaves a
zero byte one behind, so the presence of an inactive log does not by itself mean a compaction was
interrupted.~~ **No longer true.** The compactor used to remove a log only on the branch that had
partitions to write, so every clean shutdown left a zero byte `Shard-N-inactive-1` behind and the
signal meant nothing. Fixed by
[item 14](../appendix/resolved/empty-rotated-logs.md) — a rotation that compacts nothing now
deletes its log too.

> This uses blocking `std::fs::read_dir` rather than glommio IO, with a comment noting it
> "only runs during startup recovery". Fine here; it would block the executor anywhere else.

~~Note that inactive logs are deleted *immediately after replay*, before the forced compaction
that follows. A crash in that window loses their contents, since the state exists only in memory
at that point.~~ **No longer true.** Every log is deleted only after every log has been replayed,
which is the fourth phase below. A crash part way through recovery now leaves all of them on disk
and recovery starts over. Changed by
[item 31](../appendix/resolved/multi-log-recovery.md).

## The three-phase replay

Recovery reads **every** log before it replays **any** of them:

```
phase 1  read every log once, in generation order, collecting each log's entries
         and unioning the partition keys their Update intents name
phase 2  load every collected key exactly once, skipping any key already held
phase 3  replay every log's entries, in the same generation order
phase 4  delete the inactive logs
```

`.../storage/fs.rs`, `read_intents`

**Why the loading is separate.** `Update` intents carry only changed fields, so replaying one
requires the base partition. `scan_keys` names the partitions an update needs:

```rust
match intent {
    ArchivedSortedIntents::Insert(_) | ArchivedSortedIntents::Delete { .. } => (),
    ArchivedSortedIntents::Update(update) => { to_load.insert(update.partition_key.to_native()); }
}
```

Inserts need nothing (they carry the whole row); deletes need nothing (a tombstone is written
unconditionally). Only updates need to read.

**Why the phases, and not per-log passes.** This used to be two passes *per log* — scan a log,
replay it, move to the next. Loading a partition writes an archive copy into the partition map,
and an archive copy is by definition older than any intent still sitting in a log, so from the
second log onward the scan pass was overwriting partitions the previous log had just replayed
into. That was [item 31](../appendix/resolved/multi-log-recovery.md), and it lost committed data
silently. Doing all the loading before any of the replaying removes the collision rather than
guarding against it, which is why the ordering is stated as an invariant on that page:

> **No scan pass may ever run after a replay pass.**

Loading once across all logs, rather than once per log, also fixed a `memory_usage` drift: the
key set was previously rebuilt per *entry*, so two updates naming the same partition charged it
twice.

`load_partition_direct` bypasses the loader task and reads synchronously — during startup there
is no shard loop to post a `ServerMsg::Partition` back to. It also skips any key already in the
partition map, so a copy read from disk can never displace a newer one.

Every log's entries are held in memory as a `Vec<ReadResult>` until phase 3. Replay memory is
therefore proportional to the total size of all logs, each bounded by `intent_log_size` (default
10 MiB) per table per shard. That is a change: it used to be the size of the largest single log.
The count of inactive logs is the count of interrupted compactions, normally zero or one, so the
difference is small — but it is a real one.

## Replaying an intent

`replay` (`.../persistent/sorted.rs:1123-1268`) folds each intent into the partition map,
handling both `Loaded` and `Accessible` states and tracking the memory delta.

Two subtleties:

**Deletes are unconditional tombstones.**

```rust
// insert a tombstone unconditionally so it overlays disk data later
partition.tombstone(&sort_key)
```

`.../persistent/sorted.rs:1201-1202`

Not `remove`. During normal operation a delete only tombstones a row that exists; during
replay the row may live in an archive that has not been read yet, so the tombstone must be
recorded regardless, to shadow it when it is
([Partitions](../tables/partitions.md#tombstones)).

**Partitions from disk never need to go back.**

```rust
// partitions that come from reads never have to go back to disk
partition.check_disk = false;
```

`.../persistent/sorted.rs:1163-1164`

An `Accessible` partition is a complete archive copy, so once deserialized the in-memory copy
is authoritative.

The unsorted variant used to be less forgiving — an update whose partition was absent panicked
outright, taking down a shard on the one path where that is least recoverable. It now warns,
counts and skips the intent, which is reachable whenever an update's base row was compacted into
an archive in an earlier generation *and* `load_partition_direct` found nothing, for instance if
the map entry was lost. See
[Resolved Issues #9](../appendix/resolved/orphaned-update-intents.md).

The sorted variant used to be the *quieter* of the two: it applied an update with
`partition.update(&update).unwrap_or(0)`, which discards a miss without so much as a warning.
Both now go through the same classification, because `SortedPartition::update` answers `None`
for two different situations and only one of them is a problem:

| What is under the sort key | What it means | Counted as |
| --- | --- | --- |
| `MaybeRow::Tombstone` | A delete already took this row; the update was *meant* to be dropped | `updates_after_delete` |
| Nothing at all | This row's insert is gone | `orphaned_updates`, and a `warn!` |

Unsorted `Delete` intents replay into a tombstone rather than a removal:

```rust
// build the tombstone for this deleted partition, since the pre-delete
// copy may still be in an archive that has not been compacted yet
let tombstone = UnsortedPartition::tombstone(partition_key);
```

Replaying a delete as a plain `partitions.remove` would be wrong for exactly the reason the
comment gives: between the delete and the compaction that prunes it, the archive still holds
the row, and a `remove` leaves nothing to shadow it with.

## Truncation and corruption

`IntentLogReader::next_buff` (`.../fs/reader.rs`) treats every anomaly as end of log, and marks
itself `truncated` for the ones that mean something was damaged rather than that the log simply
ended:

| Condition | Action | `truncated` |
| --- | --- | --- |
| File is empty | `None` | no |
| Fewer than 8 bytes of size header | warn, `None` | yes |
| `size + 8` exceeds remaining bytes, size nonzero | warn, `None` | **yes** |
| `size + 8` exceeds remaining bytes, size zero | `None` (padding past the last record) | no |
| `size == 0` | `None` | no |
| Fewer than 8 bytes of checksum | warn, `None` | yes |
| Short payload | warn, `None` | yes |
| Checksum mismatch | warn, `None` | yes |
| `PAD_SENTINEL` | skip to the next alignment boundary and keep reading | no |
| Otherwise | `Some(read)` | no |

**The two rows for `size + 8` exceeding the remaining bytes used to be one row, and calling it
padding was wrong.** A torn size header lands there too — reading past the end of a file inside
an already aligned block zero fills rather than returning a short read, so half of a written
header comes back as a *nonzero* size that cannot possibly fit. A zero size there is the
unwritten tail of a partly filled log; anything else is an entry whose data was never written.
This was found by writing the test for the `truncated` flag and watching the truncated-header
case fail ([item 9](../appendix/resolved/orphaned-update-intents.md)).

**Stopping at the first bad record is right for a torn tail and wrong for mid-log corruption.**
Direct IO writes whole buffers, so a crash truncates at a buffer boundary and everything before
it is intact — stopping there loses exactly the uncommitted tail.

But the reader cannot distinguish "torn tail" from "one corrupt record with good records after
it". A single flipped bit mid-log still discards every subsequent intent. ~~Nothing counts these
events, and nothing surfaces them beyond the log.~~ **They are counted now**: the reader's
`truncated` flag becomes a `truncated_logs` count, and a shard that recovered with a nonzero one
says so at `WARN` when it finishes starting. What has *not* changed is the discarding itself —
the data after the flipped bit is still dropped, it is just no longer dropped in silence.

The `size == 0` case exists because DMA writes are block-aligned: the file may be padded with
zeros past the last record, and a zero size is that padding.

## What recovery discards

`read_intents` returns a `RecoveryStats` (`.../tables/storage.rs`) counting everything it could
not apply:

| Counter | Meaning | Loss? |
| --- | --- | --- |
| `orphaned_updates` | An update whose base partition was in no archive and in no earlier log | yes |
| `unreplayable_entries` | An entry that could not be replayed at all | yes |
| `truncated_logs` | A log whose reader gave up on a damaged entry | yes — but see below |
| `updates_after_delete` | An update onto a row a delete had already taken | **no** |

The last one is counted precisely so the other three can be trusted. An update that lands on a
deleted row was meant to be dropped, and folding it in with real loss would make the number
useless. `RecoveryStats::is_clean` ignores it.

**`truncated_logs` currently makes that same mistake in the other direction.** A torn tail on the
*active* log is what an ordinary crash leaves behind, and writes are acknowledged only after they
are durable ([items 1-3](../appendix/resolved/durability.md)), so the half-written entry at the
end belongs to a write no client was told about — dropping it is the design working, not loss.
It is counted as loss anyway, so every unclean shutdown emits `WARN Recovery discarded data`.
Filed as [item 47](../appendix/known-issues.md#47-a-torn-tail-on-the-active-log-is-counted-as-data-loss),
with the reader-side and position-side ways of splitting it. Until then, read a `truncated_logs`
of 1 on a shard that was killed as expected, and a higher one, or one on a shard that shut down
cleanly, as the signal.

Each table keeps its own; `ShoalDatabase::recovery_stats`, generated by the `#[db]` derive, sums
them across a shard's tables; and `Shard::init` emits one event per shard once every table has
been replayed — `INFO` when clean, `WARN` with the counts when not:

```
WARN Shard::init: msg="Recovery discarded data" shard="Shard-0" orphaned_updates=0
     unreplayable_entries=0 truncated_logs=1 updates_after_delete=0
```

Per-shard is as far as this goes. `ShoalPool::start` spawns its shard threads and returns without
joining them, so there is no moment at which every shard has finished starting and a pool-wide
total could be reported. See [Observability](../operations/observability.md).

Compaction counts the same way but reports separately, because it runs for the life of a shard
rather than only at startup — a forced compaction at startup is *dispatched* to the compactor
task, not awaited, so its drops could not be in the startup summary even if they belonged there.

## Forced compaction

After replay:

```rust
table.storage.compact_if_needed::<R>(true).await?;
```

`.../persistent/sorted.rs:210`

`force = true` rotates the active log unconditionally and queues compaction, so replayed state
is folded into archives promptly and the next restart has less to replay. It also means a
restart always advances the generation counter.

## What survives a crash

| Event | Survives? |
| --- | --- |
| Intent written and its buffer retired by the device | Yes — replayed from the log |
| Intent staged in the DMA buffer, not yet written | No |
| Intent written but not yet fdatasynced | No — and it was never acknowledged, so no client was told otherwise |
| Compaction that synced its archive writes | Yes |
| Compaction that crashed mid-way | No, but the sealed log is replayed instead |
| A partition pruned by compaction | Yes — the prune writes a `MapIntent::Remove` and drops the entry from `to_archive` ([Compaction](compaction.md#3-apply)) |
| Archive map snapshot | Yes — temp/rename/dir-fsync |
| Data acknowledged to the client | Yes — acknowledgement waits for an `fdatasync` covering the record, unless `durability: Async` is set |
| A pad region between two flushes | Not data, and skipped on replay via `PAD_SENTINEL` |

## Design notes

**Recovery is just replay; there is no separate recovery format.** The same intent records
that serve the write path serve the recovery path, and the same `apply_intents` logic serves
compaction. One representation, three uses.

**Idempotence by construction.** Sealed logs are deleted only after their contents are
durable elsewhere, so replaying an already-compacted log is harmless — inserts overwrite with
the same value, deletes tombstone rows that are already gone.

**Fail-forward on corruption.** Shoal chooses availability: truncate and start rather than
refuse to start. For a database with no replication to fall back on, that is defensible, but
~~it should be loud, and it is not~~ **it has to be loud, and it now is** — a shard that
discarded anything says so at `WARN` before it serves a query
([item 9](../appendix/resolved/orphaned-update-intents.md)). Refusing to start instead was
rejected on that page: a torn tail is what an ordinary crash leaves behind, so refusing would
block the common case.

**Load before replay, never during it.** Every partition an update needs is loaded before any
log is replayed. This is the invariant [item 31](../appendix/resolved/multi-log-recovery.md)
turns on, and the reason recovery is phased rather than per-log.

## Limitations

- Mid-log corruption discards the remainder of the log. It is now counted and reported, but the
  data is still discarded.
- No checksum on archive data, so archive corruption is not detected at all.
- `MapCorruption` is fatal with no rebuild-by-scan path, even though archives carry size
  prefixes specifically to enable one.
- Every log is buffered in memory until the replay phase, so peak recovery memory is the total
  size of all logs rather than the largest one.
- ~~No metric or alert for truncated or corrupt logs.~~ Counted and reported per shard now,
  though still only as a log event rather than a metric — nothing scrapes it.
- ~~The unsorted replay path panics rather than skipping an unresolvable update.~~ It warns,
  counts and skips.
- ~~Inactive logs are deleted before the forced compaction that persists their contents.~~ They
  are deleted only after every log has been replayed.
- Nothing aggregates the per-shard recovery counts into one number for a pool.
- Durability is only as good as the filesystem underneath. btrfs silently falls back to
  buffered IO for a misaligned O_DIRECT write instead of returning `EINVAL`, so an alignment
  bug in the write path would not surface there — the write-path tests deliberately run
  against a real filesystem rather than tmpfs for the same reason.
