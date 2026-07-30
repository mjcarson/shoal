# Recovery

Recovery runs per table, per shard, during `PersistentSortedTable::new` /
`PersistentUnsortedTable::new` — before the shard accepts any connections:

```rust
table.storage.read_intents(conf, table.generation, &mut table.partitions, &mut table.memory_usage).await?;
table.storage.compact_if_needed::<R>(true).await?;
```

`shoal-core/src/server/tables/persistent/sorted.rs:199-211`

Two steps: replay every intent log into memory, then force a compaction so the replayed state
is written into archives and the logs can be discarded.

## Replay order

```rust
let inactive_logs = Self::find_inactive_intent_logs(&intent_dir, &self.shard_name);
for (gen, inactive_path) in &inactive_logs {
    event!(Level::INFO, msg = "Recovering", gen);
    self.replay_intent_log::<P, R>(inactive_path, generation, partitions, memory_usage).await?;
    glommio::io::remove(inactive_path).await?;
    event!(Level::INFO, msg = "Replayed and removed", gen);
}
let active_path = intent_dir.join(format!("{}-active", self.shard_name));
self.replay_intent_log::<P, R>(&active_path, generation, partitions, memory_usage).await?;
```

`shoal-core/src/server/tables/storage/fs.rs:439-458`

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

Sealed logs exist only when a crash interrupted compaction between `refresh` and the
compactor's `remove`. In a clean shutdown there are none.

> This uses blocking `std::fs::read_dir` rather than glommio IO, with a comment noting it
> "only runs during startup recovery" (`.../fs.rs:194`). Fine here; it would block the
> executor anywhere else.

Note that inactive logs are deleted *immediately after replay*, before the forced compaction
that follows. A crash in that window loses their contents, since the state exists only in
memory at that point. The window is small but real.

## The two-pass replay

Each log is read twice:

```rust
let mut reads = Vec::with_capacity(1000);
let mut reader = IntentLogReader::new(intent_path).await?;
// pass 1: scan
while let Some(read) = reader.next_buff().await? {
    <P as IntentReadSupport<R>>::scan(&read, self, partitions, memory_usage).await?;
    reads.push(read);
}
// pass 2: replay
for read in reads {
    if let Err(err) = <P as IntentReadSupport<R>>::replay(&read, generation, partitions, memory_usage) {
        tracing::warn!("Skipping intent entry that was not fully committed: {err:#?}");
        continue;
    }
}
```

`.../fs.rs:147-180`

**Why two passes.** `Update` intents carry only changed fields, so replaying one requires the
base partition. `scan` looks ahead for update intents and pre-loads their partitions off disk:

```rust
match intent {
    ArchivedSortedIntents::Insert(_) | ArchivedSortedIntents::Delete { .. } => (),
    ArchivedSortedIntents::Update(update) => { to_load.insert(update.partition_key.to_native()); }
}
for partition_key in to_load {
    if let Some(partition_read) = storage.load_partition_direct(partition_key).await? {
        *memory_usage.borrow_mut() += partition_read.len();
        partitions.insert(partition_key, MaybeLoaded::Accessible(partition_read));
    }
}
```

`.../persistent/sorted.rs:1081-1112`

Inserts need nothing (they carry the whole row); deletes need nothing (a tombstone is written
unconditionally). Only updates need to read.

`load_partition_direct` bypasses the loader task and reads synchronously
(`.../fs.rs:523-539`) — during startup there is no shard loop to post a `ServerMsg::Partition`
back to.

The whole log is held in memory as a `Vec<ReadResult>` between passes. Replay memory is
therefore proportional to log size, bounded by `intent_log_size` (default 10 MiB) per table
per shard.

`scan` allocates a fresh `HashSet::with_capacity(1000)` per record to hold at most one key
(`.../persistent/sorted.rs:1090`) — a per-record allocation of a 1000-slot set for a
single-element lookup.

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

The unsorted variant is less forgiving. An update whose partition is absent panics outright:

```rust
None => panic!("Missing partition?"),
```

`.../persistent/unsorted.rs:867-868`

Reachable whenever an update's base row was compacted into an archive in an earlier
generation *and* `scan`'s `load_partition_direct` found nothing — for instance if the map
entry was lost. See
[Known Issues](../appendix/known-issues.md#9-recovery-and-compaction-panic-on-orphaned-update-intents).

## Truncation and corruption

`IntentLogReader::next_buff` (`.../fs/reader.rs:41-100`) treats every anomaly as end of log:

| Condition | Action |
| --- | --- |
| File is empty | `None` |
| Fewer than 8 bytes of size header | warn, `None` |
| `size + 8` exceeds remaining bytes | `None` (padding past the last record) |
| `size == 0` | `None` |
| Fewer than 8 bytes of checksum | warn, `None` |
| Short payload | warn, `None` |
| Checksum mismatch | warn, `None` |
| Otherwise | `Some(read)` |

```rust
if expected_checksum != actual_checksum {
    tracing::warn!("Checksum mismatch at position {} ... - treating as end of intent log", ...);
    return Ok(None);
}
```

`.../fs/reader.rs:87-93`

**This is right for a torn tail and wrong for mid-log corruption.** Direct IO writes whole
buffers, so a crash truncates at a buffer boundary and everything before it is intact —
stopping at the first bad record loses exactly the uncommitted tail.

But the reader cannot distinguish "torn tail" from "one corrupt record with good records
after it". A single flipped bit mid-log silently discards every subsequent intent, with only
a `warn!` to show for it. Nothing counts these events, and nothing surfaces them beyond the
log.

The `size == 0` case exists because DMA writes are block-aligned: the file may be padded with
zeros past the last record, and a zero size is that padding.

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
| Intent written but only in the page cache/device cache | Unknown — no `fdatasync` on this path ([Known Issues](../appendix/known-issues.md#3-no-fdatasync-on-the-steady-state-write-path)) |
| Compaction that synced its archive writes | Yes |
| Compaction that crashed mid-way | No, but the sealed log is replayed instead |
| A partition pruned by compaction | **No — resurrected**, because the stale map entry is never removed ([Known Issues](../appendix/known-issues.md#5-pruned-partitions-leak-a-stale-archive-map-entry)) |
| Archive map snapshot | Yes — temp/rename/dir-fsync |
| Data acknowledged to the client | **Not guaranteed** — acknowledgement does not currently imply a durable write ([Known Issues](../appendix/known-issues.md#1-commit-does-not-report-a-log-position)) |

## Design notes

**Recovery is just replay; there is no separate recovery format.** The same intent records
that serve the write path serve the recovery path, and the same `apply_intents` logic serves
compaction. One representation, three uses.

**Idempotence by construction.** Sealed logs are deleted only after their contents are
durable elsewhere, so replaying an already-compacted log is harmless — inserts overwrite with
the same value, deletes tombstone rows that are already gone.

**Fail-forward on corruption.** Shoal chooses availability: truncate and start rather than
refuse to start. For a database with no replication to fall back on, that is defensible, but
it should be loud, and it is not.

## Limitations

- Mid-log corruption silently discards the remainder of the log.
- No metric or alert for truncated or corrupt logs.
- No checksum on archive data, so archive corruption is not detected at all.
- `MapCorruption` is fatal with no rebuild-by-scan path, even though archives carry size
  prefixes specifically to enable one.
- Whole logs are buffered in memory between the two replay passes.
- The unsorted replay path panics rather than skipping an unresolvable update.
- Inactive logs are deleted before the forced compaction that persists their contents.
