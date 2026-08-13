# Storage Overview

Persistence in Shoal is built from three on-disk structures per table, per shard. Everything
in this section describes `FileSystem<D>`
(`shoal-core/src/server/tables/storage/fs.rs`), which is the only implementation of the
`StorageSupport` trait that persists anything. The other, `NoStorage`, is the absence of every
structure described here and is what makes a table ephemeral
([F9](../features/ephemeral-tables.md)).

## The three structures

```
                  writes
                     │
                     ▼
        ┌────────────────────────┐
        │      INTENT LOG        │   append-only WAL, one per shard per table
        │   Shard-0-active       │   [size][checksum][rkyv intent] …
        └───────────┬────────────┘
                    │ rotate when > intent_log_size
                    ▼
        ┌────────────────────────┐
        │  Shard-0-inactive-<g>  │   sealed, awaiting compaction
        └───────────┬────────────┘
                    │ FileSystemCompactor
                    │  group intents by partition,
                    │  merge with current archive copy
                    ▼
        ┌────────────────────────┐         ┌──────────────────────────┐
        │       ARCHIVES         │◀────────│       ARCHIVE MAP        │
        │  archives/<uuid>       │  offset │  partition key →         │
        │  [size][rkyv partition]│  size   │   {archive, offset, size}│
        └────────────────────────┘         └──────────────────────────┘
                                             maps/Shard-0  (snapshot)
                                             archives/intents/Shard-0 (its own WAL)
```

| Structure | Contains | Written by | Read by |
| --- | --- | --- | --- |
| **Intent log** | Individual mutations, in arrival order | The shard, synchronously on every write | Startup recovery; the compactor |
| **Archives** | Whole serialized partitions | The compactor | The loader, on partition faults |
| **Archive map** | Where each partition lives | The compactor | Every read that misses memory |

## Why this shape

**A write must be cheap.** Appending one serialized intent to a log is a sequential write of
tens to hundreds of bytes. Rewriting the partition it belongs to would mean a read, a merge,
and a much larger write. So writes go to the log and the merge is deferred.

**A read must not scan a log.** A partition's current state lives in exactly one archive
extent, found by one map lookup and read with one `read_at`. The log is never consulted at
read time — only at startup.

**Compaction is where the two meet.** It converts a log of mutations into whole partitions,
which is also when deletes actually free space and tombstones actually disappear
([Compaction](compaction.md)).

This is recognisably an LSM shape, with two deliberate differences: there is exactly one
"level" (an archive extent is either current or garbage, never merged across levels), and
the map from key to extent is an explicit index rather than an implicit consequence of sorted
files.

## Intent logs

One per shard per table, at
`<latency_sensitive.path>/<table>/intents/Shard-N-active`.

Each record is framed:

```
┌──────────────┬───────────────────┬──────────────────────┐
│ size (8 B)   │ gxhash checksum   │ rkyv-archived intent │
│              │ (8 B)             │ (size bytes)         │
└──────────────┴───────────────────┴──────────────────────┘
```

`shoal-core/src/server/tables/storage/fs.rs:322-348`

The intent types are per table kind:

```rust
pub enum SortedIntents<T> {
    Insert(T),
    Delete { partition_key: u64, sort_key: T::Sort },
    Update(SortedUpdate<T>),
}

pub enum UnsortedIntents<T> {
    Insert(T),
    Delete { partition_key: u64 },
    Update(UnsortedUpdate<T>),
}
```

`.../persistent/sorted.rs:42-51`, `.../persistent/unsorted.rs:42-48`

Details in [The Intent Log](intent-log.md).

## Archives

Uuid-named files under `<throughput_sensitive.path>/<table>/archives/`, each holding many
partitions written back to back:

```
┌───────────┬──────────────────────┬───────────┬──────────────────────┬───
│ size(8 B) │ rkyv partition       │ size(8 B) │ rkyv partition       │ …
└───────────┴──────────────────────┴───────────┴──────────────────────┴───
             ▲
             └── ArchiveEntry.offset points here (after the size field)
```

`.../fs/compactor.rs:216-237`

The inline size prefix is not used for normal reads — the map already knows the size. Its
comment says it exists "only … in recovery operations of archive files"
(`.../fs/compactor.rs:220-221`), i.e. to rebuild a lost map by scanning. No such rebuild path
exists yet.

One archive is *active* at a time (`ArchiveMap::active`, `.../fs/map.rs:317`), receiving all
newly compacted partitions. Archives become garbage gradually as their partitions are rewritten
elsewhere, and are reclaimed by archive compaction ([Compaction](compaction.md)).

## The archive map

The index from partition key to extent:

```rust
pub struct ArchiveEntry {
    pub key: u64,
    pub archive: Uuid,
    pub offset: u64,
    pub size: usize,
}
```

`.../fs/map.rs:27-37`

Held in memory as `RefCell<HashMap<u64, ArchiveEntry>>` and persisted two ways at once — a
checksummed full snapshot at `maps/Shard-N`, plus an intent log of incremental changes at
`archives/intents/Shard-N`. Details in
[Archives and the Archive Map](archives-and-map.md).

The map is also the authority on **whether a partition exists on disk at all**. This is what
makes a miss cheap:

```rust
match self.map.find_partition(partition_id) {
    Some(_) => { loader_tx.send(LoaderMsg::Request { .. }).await?; Ok(true) }
    None => Ok(false),
}
```

`.../fs/fs.rs:499-517`

A get for a partition that has never been written costs one hash lookup and no IO.

The check is deliberately made against the map rather than against the archive the read will
open, which allows a race: the compactor can prune the entry between this `Some(_)` and the read
running. That race is the reason a read reports its own outcome rather than only its data — a
pruned partition is reported as absent, and the queries parked on it are released and answer
correctly by finding nothing
([Resolved #16, 51](../appendix/resolved/partition-load-failure.md)).

## Durability model

1. A write serializes its intent and appends it to the log. `commit` returns the log offset
   one past the record it just wrote (`.../fs.rs:351`).
2. The response is parked in `PendingResponse` against that position
   (`.../storage.rs:66-69`).
3. The `StreamWriter` writes buffers out asynchronously. Completions land in `FlushState`,
   shared between the writer and its detached IO tasks, which advances a *contiguous*
   watermark and then group commits an `fdatasync` behind it.
4. The completion also posts a `ServerMsg::DataFlushed` to the shard, which is what tells it to
   go and look. `PendingResponse::get` then releases every response at or below the durable
   watermark (`.../storage.rs`).

The client therefore hears "inserted" only after the intent is fdatasynced.

**Step 4's wakeup is a requirement, not a nicety.** The shard used to sweep its tables after every
message it handled, so the message was one of several things that could trigger a release; since
[F5](../features/flushed-sweep-gate.md) it is the only one. A watermark that advances without one
behind it is a write that is durable and never acknowledged.

Two properties make step 3 sound, and both matter:

- **The watermark is a low-water mark, not a maximum.** With `write_behind` defaulting to 128,
  io_uring completions routinely arrive out of order. Taking the highest completed offset
  would advance the watermark past data still in flight, so `FlushState` only advances over
  writes that have completed contiguously from the front of its queue.
- **The gate is `synced_pos`, not `written_pos`.** O_DIRECT skips the page cache but not the
  drive's own volatile write cache. Only an `fdatasync` makes a write survive power loss.

Set `durability: Async` (see [Configuration](../getting-started/configuration.md)) to gate on
`written_pos` instead and skip the fsync. That is faster and honest about what it gives up: a
write can be acknowledged and then lost to power loss.

If you want to know what the fsync actually costs on your hardware, measure it rather than
guessing — [Benchmarking](../operations/benchmarking.md) covers how, and why btrfs is a poor
host for this write path. **Measure it on your own hardware, not from the numbers here**: the
development machine writes to an Intel Optane SSD, whose fsync latency is roughly an order of
magnitude below a consumer NVMe, so it is close to the best case this decision has
([Performance Baseline](../operations/performance-baseline.md#hardware)). An `Async` versus
`Fsync` comparison has not been captured yet.

Also solid: the archive map's snapshot uses a proper write-temp → sync → rename → fsync parent
sequence (`.../fs/map.rs:206-228`), and intent log rotation fsyncs before and after the
rename.

## Crash consistency

The intent log is the recovery source. At startup a shard replays any sealed
`-inactive-<gen>` logs oldest-first, then the active log
(`.../fs/fs.rs:443-458`). A partially written record at the tail — a short header, a short
body, or a checksum mismatch — is treated as end of log and discarded
(`.../fs/reader.rs:41-100`), which is exactly right for a torn final write.

The gap: because rotation deletes a sealed log only *after* compaction has written and synced
the new partitions, a crash mid-compaction leaves the sealed log in place and it is replayed
next boot. Compaction is therefore idempotent by construction. But because "flushed" does not
currently imply "durable", acknowledged writes may not be in the log at all. See
[Recovery](recovery.md).

## Design notes

**Two writer profiles, one engine.** Intent logs and archives have opposite IO
characteristics, so the config splits them — small buffers and deep write-behind for the log,
large buffers and shallow depth for archives
([Configuration](../getting-started/configuration.md#two-writer-profiles)). They can also sit
on different devices.

**Direct IO throughout.** Everything opens with `dma_open` and writes through `DmaBuffer`,
bypassing the page cache. The database manages its own memory
([Memory and Eviction](../tables/memory-and-eviction.md)) rather than competing with the
kernel's, and write latency is not distorted by cache flushes. The cost is that all IO is
alignment-constrained, which is why `StreamWriter` exists at all.

**Per-shard files, no coordination.** Nothing is shared, so no locking, and no shard can
block another on IO. The cost is that shard count is baked into the layout
([Partitioning](../architecture/partitioning.md#limitations)).

## Limitations

- Acknowledgement latency now includes an `fdatasync`. Group commit amortises this under
  load, but a single isolated write pays a full write plus fsync round trip. This is measured:
  `stream::write_helper`, the DMA write and the sync behind it, averages **32.6 ms per call**
  against 344 ns for the partition insert it is persisting
  ([Performance Baseline](../operations/performance-baseline.md#profile--where-the-time-goes)).
  The write path waits on storage, not on CPU.
- The filesystem matters more than it looks. **Shoal's storage is now on XFS**; it was on
  btrfs, which is copy-on-write and commits a log tree on every `fdatasync`, making it a poor
  choice for a write-ahead log. There is no before-and-after measurement of that change — no
  baseline existed when it happened. btrfs also silently falls back to buffered IO for a
  misaligned O_DIRECT write where ext4 and XFS return `EINVAL`, so a bug in the write path's
  alignment could hide there and **cannot hide on XFS** — the same misalignment now fails the
  write outright.
- One *persisting* storage engine. `NoStorage` is the second implementor and answers "nothing"
  to most of the trait, so the parts of `StorageSupport` that describe how bytes reach a device
  still have exactly one user ([F9](../features/ephemeral-tables.md)).
- No checksums on archive data — only on intent log records and the map snapshot. A corrupt
  archive extent is detected only if rkyv validation happens to fail.
- No way to rebuild a lost archive map by scanning archives, despite the size prefixes being
  written for that purpose.
- No compression and no block-level encoding; partitions are stored as raw rkyv archives.
