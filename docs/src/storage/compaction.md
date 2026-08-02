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

The compactor loop is a `match` on these (`.../fs/compactor.rs:576-592`). Jobs arrive on an
unbounded channel from the table's own `compact_if_needed`.

## Triggering

Checked on every call to `get_flushed`, i.e. once per shard loop iteration
(`.../persistent/sorted.rs:1126`):

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

`.../fs.rs:365-409`

`rotated` is what tells the caller it cannot compare positions across the boundary: the new
file's offsets restart at 0, so `get_flushed` drains its pending responses wholesale instead
of testing them. They are all durable — `refresh` fdatasynced the old file before renaming it.

Three things happen atomically from the table's point of view: the active log is renamed and
fsynced (`refresh`, see [The Intent Log](intent-log.md#rotation)), an intent compaction job is
queued for it, and an archive compaction job is queued behind it.

The threshold compares `get_unflushed_pos()` — bytes *accepted*, not bytes durable — so
rotation bounds log size and replay time regardless of IO progress.

`force = true` is passed once, at table construction (`.../persistent/sorted.rs:218`), so
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

`.../tables/partitions.rs:29-52`

A `Loaded` partition may only be evicted once its generation has been compacted — otherwise
its changes exist only in an uncompacted log and dropping it would lose them until recovery.
An `Accessible` partition is always evictable: it is a read-only view of bytes already on
disk.

Two rules make that check mean what it says, and both were broken until recently
([Resolved Issues #5](../appendix/resolved/resurrected-deletes.md)):

- **Every mutation stamps the partition with the open generation.** Unsorted tables replace the
  whole `MaybeLoaded` and get this for free; sorted partitions are mutated in place, so each
  path assigns it explicitly (`.../persistent/sorted.rs:371`, `:742`, `:914`).
- **Only a compacted generation may be compared against.** `MarkEvictable` from the compactor
  carries the generation of the log it just sealed and compacted; every other sender passes the
  table's `flushed_generation`, which is advanced only from those messages. Generations start
  at 1 (`.../fs.rs:294-295`) so 0 can mean "nothing compacted yet".

This is the correct invariant, and it has a consequence:
[under sustained writes, partitions are never evictable](../tables/memory-and-eviction.md#the-generation-trap).

## Intent log compaction

```rust
async fn compact_intent(&mut self, path: PathBuf, generation: u64) -> Result<(), ServerError> {
    let truncated = self.sort_intent_log(&path).await?;
    let partitions = if self.changes.is_empty() {
        // warn if this log was empty because we could not read any of it
        if truncated { event!(Level::WARN, ..); }
        // this log had nothing to compact so there is nothing to write
        Vec::default()
    } else {
        self.load_partitions_for_intents().await?;
        self.apply_intents().await?;
        self.write_partition().await?
    };
    // delete our no longer needed inactive intent log, which is safe for both arms
    glommio::io::remove(path).await?;
    // tell our shard this generation is now durable even if it was empty, since
    // that is what tells our table how far its data has been compacted
    self.send_mark_evictables(generation, partitions).await?;
    Ok(())
}
```

`.../fs/compactor.rs:324-374`

The `MarkEvictable` is sent unconditionally, even for a log that compacted to nothing. It is
not only a list of partitions — it is also how a table learns how far its data has been
compacted, so skipping it for an empty generation would pin every partition tagged with that
generation until some later compaction happened to lift the watermark past it.

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
        │ 5. remove the inactive log
        │ 6. MarkEvictable ──▶ shard
        ▼
```

### 1. Sort by partition

```rust
while let Some(read) = reader.next_buff().await? {
    let (partition_key, intent) = T::partition_key_and_intent(&read)?;
    self.changes.entry(partition_key).or_default().push(intent);
}
```

`.../fs/compactor.rs:153-160`

Grouping by partition turns scattered log records into one merge per partition, and preserves
per-partition ordering because the log is read in order.

Note `changes` is a field, not a local. It is drained by `apply_intents`
(`.../fs/compactor.rs:201`) and reused across jobs — deliberate allocation reuse, but it means
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

`.../fs/compactor.rs:184-203`

Read-modify-write per partition. This is the expensive part of compaction and the reason it
runs on the medium-priority queue: a rotation touching a thousand partitions performs a
thousand random reads.

### 3. Apply

`apply_intents` (`.../fs/compactor.rs:204-216`) dispatches to the table type. For sorted
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

`.../persistent/sorted.rs:1370-1401`

**This is where tombstones actually die.** At runtime a delete inserts `MaybeRow::Tombstone`
so it can shadow data still on disk; at compaction the row is genuinely removed, because the
rewritten archive simply will not contain it ([Partitions](../tables/partitions.md)).

The unsorted version does the same job with one row instead of a map, and seeds from the
partition's current archive copy so an update whose insert lives in an earlier, already
compacted log still has something to apply itself to:

```rust
// start from this partitions current archive copy if it has one, since an
// update can target a row whose insert was compacted generations ago
let mut maybe_partition = loaded.remove(&key);
```

`.../persistent/unsorted.rs`

`ShouldPrune::Yes` drops the partition from `loaded`, so it is not rewritten — and the key is
recorded so its archive entry can be dropped too:

```rust
if let ShouldPrune::Yes = T::apply_intents(&mut self.loaded, partition, intents) {
    // this partition should be pruned as it is empty
    self.loaded.remove(&partition);
    // this partition is not going to be rewritten, so its old archive entry
    // has to go too or the map keeps pointing at its pre-delete copy
    self.removals.push(partition);
}
```

`.../fs/compactor.rs`

Those removals are written to the map intent log as `MapIntent::Remove(key)` in step 4 and
applied to `to_archive` only after the sync, like every other map change. They also join the
`to_mark` list, so the tombstone shadowing a pruned partition becomes evictable in the same
generation its archive entry disappears — the tombstone is needed exactly until then, and no
longer.

This used to be a `// TODO: does anything else need to be done to remove this partition from
archive maps?`, and the answer was yes: the entry stayed in `to_archive`, a subsequent read
found it, loaded the stale partition, and resurrected deleted rows. See
[Resolved Issues #5](../appendix/resolved/resurrected-deletes.md).

### 4. Write out

`write_partition` (`.../fs/compactor.rs:216-276`) serializes each partition into the active
archive, logs a `MapIntent::Entry`, syncs both writers, and only *then* publishes entries to
the shared map:

```rust
self.writer.sync().await?;
self.map_writer.sync().await?;
for (id, entry) in self.entries.drain(..) {
    self.map.set_partition(id, entry);
}
```

`.../fs/compactor.rs:255-262`

`loaded` is cleared once its partitions have been written (`.../fs/compactor.rs:254`). It used
not to be, and because this loop iterates the whole map rather than the current job's changes,
**every partition the compactor had ever read was re-serialized and re-mapped on every
compaction** — unbounded write amplification, and a `to_mark` list that grew with the table
rather than the job.

Ordering matters: the in-memory map is not repointed until the data and the map intent are
both on disk. A crash before the sync leaves the map pointing at the old copy, which is still
intact — the compaction is simply lost and will be redone from the sealed log.

Note `sync()` here is `DmaStreamWriter::sync`, glommio's, which does flush and fsync — unlike
`StreamWriter::sync`, which only issues a background write
([The Intent Log](intent-log.md#group-commit)). Same name, different guarantee.

### 5. Delete the log

`glommio::io::remove(path)` (`.../fs/compactor.rs:370`), on every path out of the compaction.
It sits after the write and before the `MarkEvictable`, which is the ordering that matters: the
log is removed only once `write_partition` has synced both the archive and the map intent, and
the generation is announced durable only once the log holding it is gone.

~~Only reached when `changes` was non-empty. An intent log that produced no changes is never
deleted, so it stays on disk and is replayed on every subsequent startup.~~ **No longer true.**
The removal used to be the last statement of the branch that had partitions to write, so a
rotation that compacted nothing — which is what every restart of a quiet table produces — left
its log behind until the next startup swept it. Fixed by
[item 14](../appendix/resolved/empty-rotated-logs.md), which also made a log the reader could
read no entries from delete with a warning rather than silently.

The log still gets created, though: a forced rotation happens whether or not the active log holds
anything ([O21](../appendix/optimizations.md#o21-a-forced-rotation-of-an-empty-intent-log-does-the-whole-rotation-anyway)).

### 6. Mark evictable

`MarkEvictable { generation, table, partitions }` goes back to the shard
(`.../fs/compactor.rs:306-320`), which routes it to the table's `mark_evictable`. Partitions
whose generation is now covered are added to the shard LRU as eviction candidates, sorted
partitions have their now-redundant tombstones swept, and the table advances its record of how
far its data has been compacted
([Memory and Eviction](../tables/memory-and-eviction.md#becoming-evictable)).

This is last, not fifth as this page used to number it. The removal has always come first on the
path that had partitions to write; item 14 made that true of the other path too, and the order is
now an invariant rather than an accident of which branch the code took.

## Archive compaction

`compact_archives` (`.../fs/compactor.rs:342-512`) reclaims space from archives whose live
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

- ~~Empty rotated logs are never deleted and are replayed forever.~~ Deleted like any other
  rotated log since [item 14](../appendix/resolved/empty-rotated-logs.md). They are still
  *created* on every forced rotation
  ([O21](../appendix/optimizations.md#o21-a-forced-rotation-of-an-empty-intent-log-does-the-whole-rotation-anyway)).
- Compaction thresholds are hardcoded.
- `load_partitions_for_intents` issues one random read per changed partition with no
  batching, sorting by offset, or readahead.
- No throttling: a large rotation floods the medium-priority queue with reads and writes.
- `changes`, `entries`, and `removals` are drained per job and `loaded` is cleared after each
  write, but an error mid-job leaves all four dirty for the next one.
- An intent whose base row is gone is dropped with a `warn!` and nothing counts it.
