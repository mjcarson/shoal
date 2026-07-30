# Compaction

Compaction converts intent logs into archives, and reclaims space from archives that have
gone mostly stale. Both jobs run on one background task per shard per table:
`FileSystemCompactor` (`shoal-core/src/server/tables/storage/fs/compactor.rs`), spawned onto
the medium-priority task queue (`.../fs.rs:130-133`).

## Jobs

```rust
pub enum CompactionJob {
    IntentLog { path: PathBuf, generation: u64 },
    Archives,
    Shutdown,
}
```

`shoal-core/src/server/tables/storage.rs:104-112`

The compactor loop is a `match` on these (`.../fs/compactor.rs:553-569`). Jobs arrive on an
unbounded channel from the table's own `compact_if_needed`.

## Triggering

Checked on every call to `get_flushed`, i.e. once per shard loop iteration
(`.../persistent/sorted.rs:1039`):

```rust
let max_size = self.table_conf.latency_sensitive.intent_log_size;
if force || self.intent_log2.get_unflushed_pos() > max_size {
    let mut new_path = self.table_conf.get_intent_path(R::name());
    new_path.push(format!("{}-inactive-{}", self.shard_name, self.generation));
    let flushed_pos = self.intent_log2.refresh(&new_path).await?;
    self.intent_tx.send(CompactionJob::IntentLog { path: new_path, generation: self.generation }).await?;
    self.generation += 1;
    self.intent_tx.send(CompactionJob::Archives).await?;
    Ok(FlushProgress { durable_pos: flushed_pos, generation: self.generation, rotated: true })
} else {
    Ok(FlushProgress {
        durable_pos: self.intent_log2.get_flushed_pos(),
        generation: self.generation,
        rotated: false,
    })
}
```

`.../fs.rs:362-406`

`rotated` is what tells the caller it cannot compare positions across the boundary: the new
file's offsets restart at 0, so `get_flushed` drains its pending responses wholesale instead
of testing them. They are all durable — `refresh` fdatasynced the old file before renaming it.

Three things happen atomically from the table's point of view: the active log is renamed and
fsynced (`refresh`, see [The Intent Log](intent-log.md#rotation)), an intent compaction job is
queued for it, and an archive compaction job is queued behind it.

The threshold compares `get_unflushed_pos()` — bytes *accepted*, not bytes durable — so
rotation bounds log size and replay time regardless of IO progress.

`force = true` is passed once, at table construction (`.../persistent/sorted.rs:210`), so
every table compacts whatever it just replayed immediately on startup.

## Generations

`generation` is a per-table counter incremented on each rotation. It names "the epoch whose
writes are now sealed", and it is what makes eviction safe.

Every in-memory partition records the generation it was last modified in:

```rust
pub enum MaybeLoaded<P: PartitionSupport> {
    Loaded { partition: P, generation: u64 },
    Accessible(ReadResult),
}

pub fn is_evictable(&self, flushed_generation: u64) -> bool {
    match self {
        Self::Loaded { generation, .. } => *generation <= flushed_generation,
        Self::Accessible(_) => true,
    }
}
```

`.../tables/partitions.rs:28-52`

A `Loaded` partition may only be evicted once its generation has been compacted — otherwise
its changes exist only in an uncompacted log and dropping it would lose them until recovery.
An `Accessible` partition is always evictable: it is a read-only view of bytes already on
disk.

This is the correct invariant, and it has a consequence:
[under sustained writes, partitions are never evictable](../tables/memory-and-eviction.md#the-generation-trap).

## Intent log compaction

```rust
async fn compact_intent(&mut self, path: PathBuf, generation: u64) -> Result<(), ServerError> {
    self.sort_intent_log(&path).await?;
    if !self.changes.is_empty() {
        self.load_partitions_for_intents().await?;
        self.apply_intents().await?;
        let partitions = self.write_partition().await?;
        self.send_mark_evictables(generation, partitions).await?;
        glommio::io::remove(path).await?;
    }
    Ok(())
}
```

`.../fs/compactor.rs:279-311`

```
  Shard-0-inactive-7
        │
        │ 1. sort_intent_log: read every record, group by partition key
        ▼
  changes: HashMap<u64, Vec<Intent>>
        │
        │ 2. load_partitions_for_intents: read current archive copy of each
        ▼
  loaded: HashMap<u64, Partition>
        │
        │ 3. apply_intents: replay in order; empty partitions are pruned
        ▼
  loaded (merged)
        │
        │ 4. write_partition: serialize each into the active archive,
        │    log a MapIntent::Entry, sync, publish to the shared map
        ▼
  archives/<active-uuid>  +  archives/intents/Shard-0
        │
        │ 5. MarkEvictable ──▶ shard
        │ 6. remove the inactive log
        ▼
```

### 1. Sort by partition

```rust
while let Some(read) = reader.next_buff().await? {
    let (partition_key, intent) = T::partition_key_and_intent(&read)?;
    self.changes.entry(partition_key).or_default().push(intent);
}
```

`.../fs/compactor.rs:136-146`

Grouping by partition turns scattered log records into one merge per partition, and preserves
per-partition ordering because the log is read in order.

Note `changes` is a field, not a local. It is drained by `apply_intents`
(`.../fs/compactor.rs:196`) and reused across jobs — deliberate allocation reuse, but it means
an error partway through leaves stale state for the next job.

### 2. Load current copies

```rust
for partition in self.changes.keys() {
    if let Some(entry) = self.map.to_archive.borrow().get(partition) {
        let handle = self.map.get_archive(&entry.archive).await?;
        let read = handle.read_at(entry.offset, entry.size).await?;
        let archived = <T as RkyvSupport>::access(&read)?;
        handle.close().await?;
        let deserialized = <T as RkyvSupport>::deserialize(archived)?;
        self.loaded.insert(*partition, deserialized);
    }
}
```

`.../fs/compactor.rs:171-188`

Read-modify-write per partition. This is the expensive part of compaction and the reason it
runs on the medium-priority queue: a rotation touching a thousand partitions performs a
thousand random reads.

### 3. Apply

`apply_intents` (`.../fs/compactor.rs:194-206`) dispatches to the table type. For sorted
tables:

```rust
for intent in intents {
    match intent {
        SortedIntents::Insert(row) => { entry.insert(row); }
        SortedIntents::Delete { sort_key, .. } => { entry.rows.remove(&sort_key); }
        SortedIntents::Update(update) => { entry.update(&update); }
    }
}
if entry.is_empty() { ShouldPrune::Yes } else { ShouldPrune::No }
```

`.../persistent/sorted.rs:1275-1306`

**This is where tombstones actually die.** At runtime a delete inserts `MaybeRow::Tombstone`
so it can shadow data still on disk; at compaction the row is genuinely removed, because the
rewritten archive simply will not contain it ([Partitions](../tables/partitions.md)).

`ShouldPrune::Yes` drops the partition from `loaded`, so it is not rewritten. But:

```rust
if let ShouldPrune::Yes = T::apply_intents(&mut self.loaded, partition, intents) {
    self.loaded.remove(&partition);
    // TODO: does anything else need to be done to remove this partition
    // from archive maps?
}
```

`.../fs/compactor.rs:198-203`

The TODO is correct to worry: the partition is not rewritten, but **its old `ArchiveEntry`
is never removed from `to_archive`**. The map keeps pointing at the pre-delete copy in the old
archive. A subsequent read finds that entry, loads the stale partition, and resurrects deleted
rows. See
[Known Issues](../appendix/known-issues.md#5-pruned-partitions-leak-a-stale-archive-map-entry).

The unsorted variant additionally panics on an update with no preceding insert in the same
batch (`.../persistent/unsorted.rs:896-901`) — which is exactly what happens when the insert
lives in an earlier, already-compacted log.

### 4. Write out

`write_partition` (`.../fs/compactor.rs:210-254`) serializes each partition into the active
archive, logs a `MapIntent::Entry`, syncs both writers, and only *then* publishes entries to
the shared map:

```rust
self.writer.sync().await?;
self.map_writer.sync().await?;
for (id, entry) in self.entries.drain(..) {
    self.map.set_partition(id, entry);
}
```

`.../fs/compactor.rs:239-245`

Ordering matters: the in-memory map is not repointed until the data and the map intent are
both on disk. A crash before the sync leaves the map pointing at the old copy, which is still
intact — the compaction is simply lost and will be redone from the sealed log.

Note `sync()` here is `DmaStreamWriter::sync`, glommio's, which does flush and fsync — unlike
`StreamWriter::sync`, which only issues a background write
([The Intent Log](intent-log.md#group-commit)). Same name, different guarantee.

### 5. Mark evictable

`MarkEvictable { generation, table, partitions }` goes back to the shard
(`.../fs/compactor.rs:257-271`), which routes it to the table's `mark_evictable`. Partitions
whose generation is now covered are added to the shard LRU as eviction candidates
([Memory and Eviction](../tables/memory-and-eviction.md)).

### 6. Delete the log

Only reached when `changes` was non-empty. **An intent log that produced no changes is never
deleted** (`.../fs/compactor.rs:298-309`), so it stays on disk and is replayed on every
subsequent startup. See
[Known Issues](../appendix/known-issues.md#14-empty-rotated-intent-logs-are-never-deleted).

## Archive compaction

`compact_archives` (`.../fs/compactor.rs:315-485`) reclaims space from archives whose live
fraction has dropped.

```rust
let mut sorted = self.map.sort_by_load();          // least utilised first
for (used, archive_ids) in &sorted.sorted {
    for old_id in archive_ids {
        if let Some(entries) = sorted.entries.remove(&old_id) {
            let archive = DmaFile::open(&path).await?;
            let size = archive.file_size().await?;
            if *used as f64 > size as f64 * 0.50 { continue; }        // >50% live: skip
            if *old_id == *self.map.active.borrow() {
                if size < MIN_ARCHIVE_COMPACTABLE { continue; }       // small active: skip
                *self.map.active.borrow_mut() = Uuid::new_v4();       // rotate instead
                ...
                continue;
            }
            for mut entry in entries { /* copy live data into the active archive */ }
            /* log DeleteArchive, queue the file for removal */
        } else {
            /* empty archive: log DeleteArchive, queue for removal (unless active) */
        }
    }
}
```

The rules:

- **Skip archives more than 50% live.** Rewriting them costs more than it reclaims.
- **Never compact the active archive in place.** If it qualifies, mint a new active uuid and
  swap the writer instead. The comment explains why: compacting it "can lead to dangling
  partitions if we have already compacted data to the prior active archive in this
  compaction" (`.../fs/compactor.rs:361-363`). Entries written to it earlier *in this same
  pass* would be invalidated.
- **`MIN_ARCHIVE_COMPACTABLE` guards the active archive** from rotating while it is still
  small — otherwise a lightly used table would churn through uuids.

  ```rust
  /// This is 100 Mebibytes
  const MIN_ARCHIVE_COMPACTABLE: u64 = 10 << 20;
  ```

  `.../fs/compactor.rs:31-33` — the comment says 100 MiB; the value is 10 MiB.

- **Empty archives are deleted outright**, unless active — an empty active archive might just
  not have been written to yet (`.../fs/compactor.rs:412-418`).

Live entries are copied into the active archive with the same size-prefix framing, their
`ArchiveEntry` is updated to the new location, and a `MapIntent::Entry` is logged
(`.../fs/compactor.rs:369-387`). Only after both writers sync are entries published and old
archives removed from the map, and only then are the files unlinked
(`.../fs/compactor.rs:449-483`).

That ordering is the crash-safe one: new copy on disk → map repointed → old file deleted. A
crash anywhere leaves the map pointing at data that still exists.

## Shutdown

`shutdown` (`.../fs/compactor.rs:488-534`) flushes and closes the archive writer, then checks
whether the active archive ended up empty — by asking the filesystem for its size rather than
trusting the writer position, "to ensure we don't delete any archives with data"
(`.../fs/compactor.rs:497-499`). If empty, it is logged as deleted and unlinked. Then the map
writer is flushed and closed.

## Design notes

**One background task, two job kinds, one channel.** Intent and archive compaction never run
concurrently for a table, so they cannot race over the active archive or the map. Rotation
queues them in order — intent first, then archives — so archive compaction always sees the
freshly written partitions.

**Compaction is idempotent.** A sealed log is deleted only after its results are durable, so
a crash mid-compaction just means it runs again from the same input.

**Publish-after-sync, everywhere.** In both paths, the in-memory map is updated only after
the data and the map intent have been synced. The shared map never points at bytes that are
not on disk.

**50% is the space/write-amplification knob.** Compacting at higher utilisation reclaims less
per byte rewritten. It is hardcoded (`.../fs/compactor.rs:336`), as is
`MIN_ARCHIVE_COMPACTABLE` (marked `// TODO make size configurable`,
`.../fs/compactor.rs:343`).

## Limitations

- Pruned partitions leak a stale map entry, resurrecting deleted data
  ([Known Issues](../appendix/known-issues.md#5-pruned-partitions-leak-a-stale-archive-map-entry)).
- Empty rotated logs are never deleted and are replayed forever.
- Compaction thresholds are hardcoded.
- `load_partitions_for_intents` issues one random read per changed partition with no
  batching, sorting by offset, or readahead.
- No throttling: a large rotation floods the medium-priority queue with reads and writes.
- `changes`, `loaded`, and `entries` persist across jobs; an error mid-job leaves them dirty.
- Unsorted compaction panics on an update whose insert was compacted in an earlier generation
  (`.../persistent/unsorted.rs:900`).
