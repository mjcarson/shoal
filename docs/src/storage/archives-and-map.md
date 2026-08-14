# Archives and the Archive Map

Archives hold compacted partition data. The archive map says where each partition lives. Both
are per shard, per table.

## Archives

Uuid-named files under `<throughput_sensitive.path>/<table>/archives/`. Partitions are packed
back to back, each preceded by its size:

```rust
let archived = rkyv::to_bytes::<_>(partition)?;
let size = archived.len();
self.writer.write_all(&size.to_le_bytes()).await?;
let offset = self.writer.current_pos();          // AFTER the size prefix
self.writer.write_all(archived.as_slice()).await?;
let intent = MapIntent::entry(*key, active_id, offset, size);
```

`shoal-core/src/server/tables/storage/fs/compactor.rs:216-231`

`offset` is captured after the size is written, so `ArchiveEntry.offset` points at the rkyv
payload directly and a read is a single `read_at(offset, size)` with no header parsing.

The size prefix is redundant for normal reads. Its stated purpose is recovery — "this size is
only used in recovery operations of archive files"
(`.../fs/compactor.rs:220-221`) — i.e. rebuilding a lost map by scanning an archive. **No such
recovery path exists.** The prefixes are written and never read.

Archives are append-only and immutable. Updating a partition writes a new copy into the
active archive and repoints the map; the old copy becomes garbage, reclaimed later by archive
compaction ([Compaction](compaction.md)).

There is exactly one active archive at a time, in `ArchiveMap::active`
(`.../fs/map.rs:334`). It is created lazily by `get_active_writer` (`.../fs/map.rs:411-433`),
which also registers the new archive in `all_archives`.

Archive payloads carry **no checksum**. Intent log records do, and the map snapshot does, but
a partition read out of an archive is trusted. Corruption surfaces only if rkyv's `access`
validation happens to reject it — and several call sites `.unwrap()` that result
(`.../persistent/sorted.rs:245`, `:355`, `:453`).

## The archive map

```rust
pub struct ArchiveEntry {
    pub key: u64,        // partition key
    pub archive: Uuid,   // which archive file
    pub offset: u64,     // byte offset of the rkyv payload
    pub size: usize,     // payload length
}
```

`.../fs/map.rs:27-37`

In memory:

```rust
pub struct ArchiveMap {
    table_name: String,
    pub active: RefCell<Uuid>,
    pub to_archive: RefCell<HashMap<u64, ArchiveEntry>>,
    pub loaded_archives: RefCell<HashMap<Uuid, DmaFile>>,
    pub all_archives: RefCell<HashSet<Uuid>>,
    pub map_path: PathBuf,
    pub temp_map_path: PathBuf,
    pub intent_path: PathBuf,
    conf: FileSystemTableConf,
}
```

`.../fs/map.rs:312-332`

`RefCell` throughout, and shared as `Arc<ArchiveMap>` between the table, the loader, and the
compactor — all on the same thread. Same pattern as the shard's `memory_usage`
([Thread per Core](../architecture/thread-per-core.md#what-a-shard-owns)): `Arc` for sharing
within a thread, `RefCell` for mutation, no atomics.

`loaded_archives` is a cache of open `DmaFile` handles, avoiding an `open` per partition
fault. It is unbounded — every archive ever touched stays open for the process lifetime, and
`ArchiveMap::new` preallocates for 1000 (`.../fs/map.rs:382`). File descriptors are only
released by `remove_archive` (`.../fs/map.rs:538-548`) or the shutdown drain (`:615`).

**Nothing bounds it, and nothing has hit the ceiling yet.** An archive is only removed when
compaction reclaims it, so a long-lived table accumulates a descriptor per archive it has ever
read. `EMFILE` is not hypothetical here — it is the failure the loader's `Retryable`
classification exists to ride out, precisely because a descriptor another read is holding may come
back ([Resolved #16, 51](../appendix/resolved/partition-load-failure.md#the-fix)). So the retry
loop and this cache are two halves of the same unsolved problem, and
[O15](../appendix/optimizations.md#o15-one-partition-load-costs-a-dup-and-a-close) says so from the
other side: it is filed as an optimization because no `EMFILE` has been observed, which makes it an
argument rather than a symptom.

Handles are handed out with `dup()` (`.../fs/map.rs:418`, `:430`, `:484`) so a caller can
`close()` its copy without disturbing the cached one — `read_partition_helper` relies on exactly
that (`.../fs/loader.rs:92-94`).

**Two functions fill that cache, and only one of them creates.** `get_active_writer` creates the
active archive, which is the only archive that is ever created. `get_archive` opens an archive
that must already be there and reports `ShoalError::ArchiveMissing` when it is not; it used to
open with `create(true)`, which turned a deleted archive into an empty one and the read of it into
a corruption failure two hops later ([Resolved #57](../appendix/resolved/missing-archive.md)).
Because the two share this one cache, `get_archive` still opens for writing as well as reading —
a read-only handle cached for the active archive id would be duplicated into a writer that cannot
write.

## Two persistence mechanisms

The map is persisted twice over, for two different reasons.

```
   compaction writes an entry
              │
              ├──▶ archives/intents/Shard-N     append MapIntent  (incremental, cheap)
              │
              └──▶ (when intent log > 1 MiB)
                   maps/temp/Shard-N ──rename──▶ maps/Shard-N     (full snapshot, atomic)
                   then delete the intent log
```

### The map intent log

Same framing as the table intent log — size, checksum, payload — written by a macro so the
three call sites stay consistent:

```rust
macro_rules! write_map_intent {
    ($map_writer:expr, $intent:expr, $variant:ident) => {{
        let archived_intent = rkyv::to_bytes::<Error>(&$intent)?;
        let size = archived_intent.len();
        let mut hasher = GxHasher::default();
        hasher.write(archived_intent.as_slice());
        let checksum = hasher.finish();
        $map_writer.write_all(&size.to_le_bytes()).await?;
        $map_writer.write_all(&checksum.to_le_bytes()).await?;
        $map_writer.write_all(archived_intent.as_slice()).await?;
        debug_assert!($intent.is_kind(MapIntentKinds::$variant));
        match $intent {
            MapIntent::$variant(entry) => entry,
            _ => unsafe { std::hint::unreachable_unchecked() },
        }
    }};
}
```

`.../fs/compactor.rs:41-67`

The macro is used for `Entry` intents but *not* for `DeleteArchive` ones, which are open-coded
three times in `compact_archives` and `shutdown` (`.../fs/compactor.rs:390-407`, `:419-436`,
`:502-519`) — the same fifteen lines repeated. A `DeleteArchive` variant call would collapse
them.

Two intent kinds:

```rust
pub enum MapIntent {
    DeleteArchive(Uuid),
    Entry(ArchiveEntry),
}
```

`.../fs/map.rs:47-53`

### The snapshot

```rust
pub struct SerializedMap {
    all_archives: HashSet<Uuid>,
    to_archive: HashMap<u64, ArchiveEntry>,
}
```

`.../fs/map.rs:90-96`

Saving is the careful part:

```rust
let archived = rkyv::to_bytes::<Error>(&serializable)?;
let mut hasher = GxHasher::default();
hasher.write(&archived);
let map_hash = hasher.finish();

let temp_map = OpenOptions::new().create_new(true).write(true).truncate(true)
    .dma_open(&map.temp_map_path).await?;
let mut writer = DmaStreamWriterBuilder::new(temp_map).build();
writer.write_all(&map_hash.to_le_bytes()).await?;
writer.write_all(&archived).await?;
writer.sync().await?;
writer.close().await?;
glommio::io::rename(&map.temp_map_path, &map.map_path).await?;
if let Some(parent) = map.map_path.parent() {
    let dir = glommio::io::Directory::open(parent).await?;
    dir.sync().await?;
    dir.close().await?;
}
```

`.../fs/map.rs:190-230`

Write to a temp file → sync the data → rename over the target → **fsync the parent
directory**. That last step is what makes the rename itself durable, and it is the piece most
implementations omit. This is the most carefully written durability code in the repository —
notably more careful than the intent log's steady-state path.

The 8-byte hash prefixes the archive so a torn or corrupt snapshot is detected on load:

```rust
if read.len() < 8 { return Err(ServerError::Shoal(ShoalError::TruncatedIntentLog)); }
let expected = u64::from_le_bytes(read[..8].try_into()?);
let mut hasher = GxHasher::default();
hasher.write(&read[8..]);
if expected != hasher.finish() {
    return Err(ServerError::Shoal(ShoalError::MapCorruption { found, expected }));
}
```

`.../fs/map.rs:154-170`

Map corruption is fatal — there is no rebuild-by-scan fallback, so a bad map means the
shard's archives are unreachable even though the data is intact.

`create_new(true)` on the temp file means a leftover temp from a crashed save makes the next
save fail with `AlreadyExists`. There is no cleanup of stale temp files at startup.

### Loading

```rust
let mut map = rkyv::deserialize::<SerializedMap, rkyv::rancor::Error>(archived)?;
map.load_intent_log(intent_path).await?;
```

`.../fs/map.rs:174-176`

Snapshot first, then replay the intent log over it (`.../fs/map.rs:99-123`) — `Entry` inserts
or overwrites, `DeleteArchive` removes from `all_archives`. Last write wins, so replay order
matters and the log is read strictly in order.

Note `DeleteArchive` removes only from `all_archives`, never from `to_archive`. That is
consistent with how compaction works — every entry on a deleted archive is rewritten and
re-`Entry`d *before* the `DeleteArchive` is logged — so the later `Entry` records already
repoint those partitions. It relies on ordering within the log, which replay preserves.

### Compaction of the map itself

```rust
pub async fn compact_map(&self) -> Result<DmaStreamWriter, ServerError> {
    SerializedMap::save(self).await?;
    if let Err(error) = glommio::io::remove(&self.intent_path).await { /* ignore NotFound */ }
    let writer = self.new_writer().await?;
    Ok(writer)
}
```

`.../fs/map.rs:522-544`

Snapshot, drop the log, start a new one. Triggered whenever the map intent log passes 1 MiB
(`.../fs/compactor.rs:247-252`, `:467-472`) and once at compactor construction
(`.../fs/compactor.rs:108`).

There is a window here: the snapshot is renamed into place, then the intent log is deleted.
A crash between the two replays intents already folded into the snapshot. That is safe — the
intents are idempotent — so the ordering is the correct way round.

## Usage tracking for compaction

```rust
pub struct SortedUsageMap {
    pub sorted: BTreeMap<usize, Vec<Uuid>>,      // live bytes -> archives
    pub entries: HashMap<Uuid, Vec<ArchiveEntry>>, // archive -> its live entries
}
```

`.../fs/map.rs:234-239`

`sort_by_load` (`.../fs/map.rs:486-519`) walks `to_archive`, sums live bytes per archive, and
buckets archives by that total in a `BTreeMap` — so iterating it yields archives from least
to most utilised, which is the order archive compaction wants. Archives in `all_archives` with
no live entries appear at key 0 and are reclaimed outright.

Note it sums `entry.size`, the live bytes, and compares against the file's actual size
(`.../fs/compactor.rs:334-336`) — so the ratio is genuinely live/total, not an estimate.

## Design notes

**Snapshot plus log, for a small mutable index.** The map is small and changes in bursts, so
a full rewrite per change would be wasteful and a pure log would grow unbounded. Snapshot plus
incremental log with a size-triggered fold is the standard answer.

**Offsets point past the length prefix.** One `read_at` per partition fault, no parsing.

**Immutable archives, indirection through the map.** Updating a partition never rewrites an
archive in place, so readers never see a partially rewritten extent and no locking is needed
between the compactor and the loader.

## Limitations

- No checksums on archive payloads.
- No way to rebuild a lost map; `MapCorruption` is unrecoverable despite the data being
  intact.
- `loaded_archives` is an unbounded fd cache.
- A stale `maps/temp/Shard-N` from a crashed save permanently breaks map compaction
  (`create_new(true)`), with no startup cleanup.
- `DeleteArchive` writing is duplicated three times instead of using the macro.
- `SerializedMap` does not persist `active`; a fresh `Uuid::new_v4()` is chosen on every
  startup (`.../fs/map.rs:380`), so every restart begins a new active archive and leaves the
  previous one to be reclaimed later. Flagged in-code as
  `// TODO make issue about SerializedMap not needing to track active` (`.../fs/map.rs:368`).
