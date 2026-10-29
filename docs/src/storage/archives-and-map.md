# Archives and the Archive Map

Archives hold compacted partition data. The archive map says where each partition lives. Both
are per shard, per table.

## Archives

Uuid-named files under `<throughput_sensitive.path>/<table>/archives/`. Since
[F44](../features/repair.md) an archive is **format 2**: a sixteen byte header, then partitions
packed back to back, each preceded by its size and a checksum over its bytes:

```text
[b"SHOALARC"][version u32 = 2][reserved u32]        the header, once
[size u64][gxhash64 u64][rkyv payload]              per record
[size u64][gxhash64 u64][rkyv payload]
…
```

```rust
let archived = rkyv::to_bytes::<_>(partition)?;
let offset = write_record(&mut self.writer, archived.as_slice()).await?;   // AFTER the prefix
let intent = MapIntent::entry(*key, active_id, offset, archived.len());
```

`shoal-core/src/server/tables/storage/fs/compactor.rs` (`write_partition`), `fs/map.rs`
(`write_record`)

`offset` is the payload's, so `ArchiveEntry.offset` points at the rkyv bytes directly and the
checksum sits at `offset - 8`. A read of a format 2 record is one `read_at(offset - 8, size + 8)`,
a hash of the payload compared with the eight bytes ahead of it, and a slice of the same buffer
handed on - the checksum costs no second read. `write_record` is the one place a record is
written - a compaction, an archive compaction and a snapshot install all go through it - and
`ArchiveMap::read_record` is the one place a record is read - the loader, the compactor's
merge, an archive compaction, a snapshot cut and a direct read - so every record a format 2
archive holds carries a checksum and every read of one is verified once.

~~The size prefix is redundant for normal reads. Its stated purpose is recovery — "this size is
only used in recovery operations of archive files" — i.e. rebuilding a lost map by scanning an
archive. **No such recovery path exists.** The prefixes are written and never read.~~ The size
prefix is still never read by a normal read, and there is still no rebuild-by-scan path; but
it now sits beside the checksum a read *does* use, and the header makes an archive
self-describing, which a scan would need.

**Format 1** is what every archive written before F44 is: no header, `[size][payload]` records,
nothing to verify. `ArchiveMap::get_archive` reads the first sixteen bytes of an archive when
it opens it and records the format for the life of the handle (`format_of`); a format 1 record
is read as it always was and counted as an *unverified read* in the map's `IntegrityCounters`,
which the replication report carries. An archive's format never changes: a restart mints a new
active archive, so a format 1 one is never written to again, and archive compaction rewrites
what is still live in it into the format 2 active archive **whatever its utilization** - the
fifty percent rule that leaves a well used archive alone is waived for one with no checksums,
since rewriting is how its records come to have them. That is a one-time cost on the first
archive compaction after the upgrade, proportional to the live bytes of the old archives.

Archives are append-only and immutable. Updating a partition writes a new copy into the
active archive and repoints the map; the old copy becomes garbage, reclaimed later by archive
compaction ([Compaction](compaction.md)).

There is exactly one active archive at a time, in `ArchiveMap::active`
(`.../fs/map.rs:334`). It is created lazily by `get_active_writer` (`.../fs/map.rs:411-433`),
which also registers the new archive in `all_archives`.

~~Archive payloads carry **no checksum**. Intent log records do, and the map snapshot does, but
a partition read out of an archive is trusted. Corruption surfaces only if rkyv's `access`
validation happens to reject it.~~ A format 2 record that does not hash to its checksum is
`ShoalError::CorruptArchive`, naming the archive and the partition, before a byte of it reaches
rkyv; the loader classes it `Fatal` (the bytes will not change on a retry), the queries parked
on it hear `ErrorCode::CorruptArchive`, and the map counts a *checksum failure*. A snapshot cut
that meets one fails rather than sending the record on, so a corrupt copy is never a source,
and an archive compaction that meets one fails rather than rewriting it under a fresh checksum,
so corruption is never laundered ([F44](../features/repair.md)).

## The archive map

```rust
pub struct ArchiveEntry {
    pub key: u64,        // partition key
    pub archive: Uuid,   // which archive file
    pub offset: u64,     // byte offset of the rkyv payload
    pub size: usize,     // payload length
}
```

`shoal-core/src/server/tables/storage/fs/map.rs`

In memory:

```rust
pub struct ArchiveMap {
    table_name: String,
    pub active: RefCell<Uuid>,
    index: PagedIndex,                          // where every partition's records are, paged
    folder: Cell<Option<FoldFn>>,
    usage: RefCell<TabletUsage>,                // bytes, partitions and chains per tablet
    archive_bytes: RefCell<HashMap<Uuid, u64>>, // live bytes per archive, prefixes included
    pub loaded_archives: RefCell<HashMap<Uuid, DmaFile>>,
    formats: RefCell<HashMap<Uuid, ArchiveFormat>>,
    pub integrity: IntegrityCounters,
    pub all_archives: RefCell<HashSet<Uuid>>,
    pub map_path: PathBuf,                      // the manifest
    pub temp_map_path: PathBuf,
    pub intent_path: PathBuf,
    conf: FileSystemTableConf,
}
```

**The index is paged.** Since [F76](../features/paged-archive-map.md) the map holds no entry in
memory for every partition it names. Its index, `PagedIndex` (`fs/index/`), is a small
log-structured merge of sorted runs:

- the **delta**, `BTreeMap<u64, Change>`, every change since the last flush, newest per key: a
  partition's chain, or `Removed`. It is what the map's intent log holds, kept in memory, and is
  bounded by `storage.*.filesystem.map.delta_entries` (16,384);
- **runs**, immutable files of 4 KiB pages in key order, newest first. A run keeps its
  directory (each page's first key), its archive table and a blocked Bloom filter of its keys
  in memory (`filter_bits`, 10 bits a key) and its pages on disk;
- a **page cache**, the least recently used pages point lookups read, up to `page_cache_bytes`
  (2 MiB);
- the **manifest**, which names the runs and is the map's commit point.

~~**The index is compact.** Since [O83](../appendix/optimizations.md#o83-the-partition-index-held-forty-eight-bytes-a-partition)
`to_archive` is a `PartitionIndex`: each partition's `Slot`, 16 bytes beside its 8 byte key (the
archive as a number into the index's table of archive ids, a `u32` size, a `u64` offset), where it
was a 40 byte `ArchiveEntry` repeating the key. Readers are handed an `ArchiveEntry` built from
the slot, and the saved map holds the slots and the table.~~ O83's slot layout lives on in the
page: an entry is 28 bytes on disk, its archive a number into the run's own table, and a reader
is handed an `ArchiveEntry` built from it. What a partition costs in memory is its filter's bits,
about a byte and a quarter, where it was about fifty bytes.

**A lookup answers as of its start.** `probe` is synchronous and says `Absent`, `Found` or
`Unknown` from the delta, each run's filter and directory, and the cache; `chain_of` and
`chains_of` read the pages they need, one read a page however many keys it serves. A lookup
takes the delta's answer and the run set (an `Rc`) before it awaits anything, so a flush or a
merge while it reads changes nothing it sees: a run merged away is unlinked, and the lookup's
handle on it outlives the unlink. A scan (`scan_tablets`, `scan_all`) is the same: the delta's
entries in its ranges are copied and the runs taken when it is made, and its pages are read a
few at a time and never through the cache. A tablet is a key's top twelve bits, so a tablet is
one contiguous key range of every run.

**Changes say what they replace.** `set_partition`, `set_chain` and `remove_partition` are
synchronous - the compactor's repoints must not yield - and write only the delta. Each takes the
chain the map held for the partition, which every writer has at hand (it merged over it, gathered
it to move it, or scanned it to drop it), and moves `usage` and `archive_bytes` by the difference;
a debug build checks it against `probe`.

**Chains.** Since [F61](../features/fragmented-partitions.md) a partition's entry is its base
and the fragments merged over it since it was last written whole, oldest first, and the map has a
`folder`, the table's `FoldFn`. ~~The map also holds `fragments: RefCell<HashMap<u64, Vec<ArchiveEntry>>>`~~
Since F76 the fragments are in the entry itself, in the delta and in a page. The fragments are
never read on their own: `read_partition`/`read_chain` read the base and each fragment, each
verified, and fold them with the `FoldFn` into `PartitionBytes::Folded`; a partition with no chain
comes back as `PartitionBytes::Record`, the read itself. `set_partition` ends a chain,
`set_chain` sets one whole, `remove_partition` drops both, and ~~`entries_of`~~ `gather` leaves
chained bases out of an archive's records so the archive pass never copies a base alone.
`TabletUsage` counts fragments as bytes and each chain once in `chained`.

`RefCell` throughout, and shared as `Arc<ArchiveMap>` between the table, the loader, and the
compactor — all on the same thread. Same pattern as the shard's `memory_usage`
([Thread per Core](../architecture/thread-per-core.md#what-a-shard-owns)): `Arc` for sharing
within a thread, `RefCell` for mutation, no atomics. No borrow is held across an await: a page
read takes the cache's borrow before and after it, never during.

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
`close()` its copy without disturbing the cached one — `ArchiveMap::read_record` relies on
exactly that.

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
   compaction repoints a partition
              │
              ├──▶ archives/intents/Shard-N       append MapIntent, and the delta in memory
              │
              ├──▶ (when the delta holds delta_entries partitions)
              │    maps/Shard-N.run-<id>             the delta written as a new run, synced
              │    maps/Shard-N.run-<id>             runs merged while newest × ratio ≥ next
              │    maps/temp/Shard-N ──rename──▶ maps/Shard-N    the manifest naming the runs
              │    then the merged-away runs are deleted; the intent log is kept
              │
              └──▶ (when the intent log passes its bound) the same commit, then the log is
                   deleted and begun again
```

~~`(when intent log > a quarter of the saved map, and > 1 MiB) maps/temp/Shard-N ──rename──▶
maps/Shard-N (full snapshot, atomic)`~~ Before F76 the second half was a snapshot of the whole
map, rewritten each time ([F76](../features/paged-archive-map.md)).

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

~~Two intent kinds~~ Four intent kinds, new ones appended:

```rust
pub enum MapIntent {
    DeleteArchive(Uuid),
    Entry(ArchiveEntry),
    Remove(u64),
    Chain(ChainEntry),   // F61: the base and every fragment, oldest first
}
```

`Chain` names the whole chain rather than the fragment added, so replaying an intent log twice,
which a crash between a map save and the log's deletion does, lands on the same chain. A chain
any record of which lies past its archive's end is skipped as a torn `Entry` is
([Resolved #159](../appendix/resolved/map-ahead-of-archive.md)).

`.../fs/map.rs:47-53`

### ~~The snapshot~~ The runs and the manifest

~~`SerializedMap`, the whole map serialized, saved to a temp file and renamed over the last~~
Since F76 there is no snapshot of the whole map. A **run** is written once, by a flush of the
delta or a merge of two runs, and never changed:

```text
page     [checksum u64][entries u16][fragments u16][reserved u32]
         [key u64][offset u64][archive u32][size u32][first fragment u16][fragments u8][flags u8]  × entries
         [offset u64][archive u32][size u32]                                                       × fragments
run      [page 0][page 1]...[page n-1][footer, padded to a page]
footer   rkyv RunFooter { first_keys, last_key, archives, filter, entries, removed }
trailer  [footer offset u64][footer length u64][footer checksum u64][b"SHOALRUN"]   the file's last 32 bytes
```

`fs/index/page.rs`, `fs/index/run.rs`

Every slot is the same length, so a lookup is a binary search of the page as it was read; a page
holds about 145 entries. A page carries a checksum over everything after it, and one that does
not match is `ShoalError::MapCorruption`, logged with the run and the page. A run is synced
before any manifest names it.

The **manifest** is the commit point, and it is saved the way the whole map was:

```text
[gxhash64 of the rest][b"SHOALMAP"][rkyv Manifest { runs, next_run, state }]
```

`fs/index/manifest.rs`. `state` is what the map counts - `all_archives`, the per tablet bytes,
partitions and chains, and the live bytes per archive - so an open need not count them again.
The save writes a temp file, syncs it, renames it over the last and **fsyncs the parent
directory**, which also makes every run's directory entry durable, since the runs live beside it.
A temp file a crashed save left is removed first ([Resolved #135](../appendix/resolved/leftover-temp-map.md)).

A map from before F76 is a checksum and a whole serialized index: its checksum still matches,
and what follows it is not the manifest's magic, so it is refused by name rather than misread.
There is no conversion; a node of a build before F76 is destroyed and its data loaded again.

~~Map corruption is fatal — there is no rebuild-by-scan fallback, so a bad map means the shard's
archives are unreachable even though the data is intact.~~ A manifest that fails its checksum
still fails the shard's start. A page that fails its checksum fails the lookups it serves - a
read of a partition on it answers `Internal` - and nothing rebuilds it: there is still no
rebuild-by-scan path ([todos](../appendix/todos.md#archive-map-reconstruction)).

### Loading

`ArchiveMap::new` opens the manifest and the runs it names, reading each run's footer and nothing
else (`PagedIndex::open`). Every `Shard-N.run-*` the manifest does not name is a run a flush or
merge wrote before a crash, and is removed before any new run is written. Then the intent log is
replayed into the delta (`ArchiveMap::replay`): what the runs hold for every key the log names is
looked up at once, and the intents are applied in order, each moving the counters by what it
replaces. `Entry` and `Chain` set, `Remove` removes, `DeleteArchive` takes the archive out of
`all_archives`. Last write wins, so the log is read strictly in order. An `Entry` or `Chain` any
record of which lies past the end of its archive on disk is skipped, and the entry before it
stands: a build before [Resolved #159](../appendix/resolved/map-ahead-of-archive.md) could log an
intent ahead of its record, and a crash between them left one. A map whose log is missing has
nothing to replay.

~~`let mut map = rkyv::deserialize::<SerializedMap, …>(archived)?; map.load_intent_log(intent_path).await?;`
Snapshot first, then replay the intent log over it.~~ The open reads a manifest and a few footers
however many partitions the map names, where it read and deserialized the whole map.

Note `DeleteArchive` removes only from `all_archives`, never from the index. That is consistent
with how compaction works — every entry on a deleted archive is rewritten and re-`Entry`d
*before* the `DeleteArchive` is logged — so the later `Entry` records already repoint those
partitions. It relies on ordering within the log, which replay preserves.

### ~~Compaction of the map itself~~ Flushing and merging

```rust
pub async fn compact_map(&self) -> Result<DmaStreamWriter, ServerError> {
    self.commit().await?;                                  // flush, merge, manifest
    if let Err(error) = glommio::io::remove(&self.intent_path).await { /* ignore NotFound */ }
    let writer = self.new_writer().await?;
    Ok(writer)
}
```

`PagedIndex::commit` writes the delta as a new run and takes what it wrote out of the delta, with
nothing awaited between, then merges while the newest run's entries times `merge_ratio` (4) are
at least the next run's; a merge into the oldest run drops the removals no older run can need.
It then saves the manifest, and only then deletes the runs merged away. ~~Triggered whenever the
map intent log passes a quarter of the map as last saved, and never below 1 MiB, the fold rewrote
the whole map ([O62](../appendix/optimizations.md#o62-every-compaction-rewrites-the-shards-whole-archive-map)).~~
A commit is made after a job once the delta holds `delta_entries` partitions (`flush_due`), and
keeps the intent log: replaying it over the runs it was flushed into changes nothing, and a flush
that had to delete it would depend on the archive directory the log lives in. The log is begun
again (`compact_map`, a commit and then the log deleted and a new one opened) once it passes
1 MiB or `delta_entries` × 128 bytes, whichever is more (`rotate_due`), and once at compactor
construction. The runs' sizes grow geometrically, so a map of N partitions is about
log4(N / delta) runs and writes a partition's entry about that many times; nothing rewrites the
whole map unless a merge reaches the oldest run, which a merge into a large map logs at info with
how long it held the compactor. The log a restart replays is bounded the same way.

There is a window here: the manifest is renamed into place, then the intent log is deleted. A
crash between the two replays intents already flushed into the runs. That is safe — replaying an
intent over a map that holds it changes nothing, and the counters move by what each intent
replaces — so the ordering is the correct way round.

A rehome stages its destination's runs without a manifest as its delta fills, and commits once at
the step's end, since the destination naming the step's archive is what marks the step done
([F47](../features/local-rehome.md)).

## Usage tracking for compaction

```rust
pub struct SortedUsageMap {
    pub sorted: BTreeMap<usize, Vec<Uuid>>,      // live bytes -> archives
}
```

~~`sort_by_load` walks `to_archive`, sums live bytes per archive~~ `sort_by_load` reads
`archive_bytes`, which every change keeps, and buckets archives by their live bytes in a
`BTreeMap` — so iterating it yields archives from least to most utilised, which is the order
archive compaction wants. Archives in `all_archives` with no live entries appear at key 0 and are
reclaimed outright. An archive pass then chooses its archives, least used first, until their
live bytes cover its budget, and gathers every chosen archive's records in one pass over the
index (`ArchiveMap::gather`), since the index is on disk and a pass an archive would read it once
for each ([F76](../features/paged-archive-map.md)).

~~Note it sums `entry.size`, the live bytes, and compares against the file's actual size
(`.../fs/compactor.rs:334-336`) — so the ratio is genuinely live/total, not an estimate.~~
It summed `entry.size`, the *payload* bytes, against a file that also holds every record's
sixteen byte prefix and the header, so a fully live archive of short records read as under half
live and every pass copied it. Since [Resolved #179](../appendix/resolved/archive-usage-prefix.md)
each entry counts its payload and its prefix, and the ratio is live over total but for the header.

## Design notes

~~**Snapshot plus log, for a small mutable index.** The map is small and changes in bursts, so
a full rewrite per change would be wasteful and a pure log would grow unbounded.~~
**Runs plus log, for an index larger than memory.** The map is not small: at ten times the lab's
dataset it was most of a node's memory, and it grows with every partition ever archived. A
delta backed by the intent log, flushed into immutable sorted runs merged geometrically, keeps
what is in memory bounded and what is rewritten logarithmic ([F76](../features/paged-archive-map.md)).

**Offsets point past the length prefix.** One `read_at` per partition fault, no parsing.

**Immutable archives, and immutable runs, indirection through the map.** Updating a partition
never rewrites an archive in place, so readers never see a partially rewritten extent and no
locking is needed between the compactor and the loader. A run is never rewritten either, so a
cached page is never stale and a lookup holding a run merged away still reads it.

## Limitations

- ~~No checksums on archive payloads.~~ Format 2 records carry one
  ([F44](../features/repair.md)); a format 1 archive is unverified until archive compaction
  rewrites it, and the count of unverified reads is what says whether any are left.
- No way to rebuild a lost map; ~~`MapCorruption` is unrecoverable despite the data being
  intact~~ a manifest that fails its checksum fails the shard's start, and a page that fails its
  checksum fails every lookup it serves, despite the data being intact.
- Each run's filter is in memory, about a byte and a quarter a partition at ten bits a key: the
  one part of the map that still grows with it ([F76](../features/paged-archive-map.md#limitations)).
- The delta's cap is checked after a job, so a job can carry it past `delta_entries` by the
  partitions it repointed.
- A cold point read of a partition whose page is not cached costs an index page read before its
  record's.
- `loaded_archives` is an unbounded fd cache.
- ~~A stale `maps/temp/Shard-N` from a crashed save permanently breaks map compaction
  (`create_new(true)`), with no startup cleanup.~~ Removed before every save since
  [Resolved #135](../appendix/resolved/leftover-temp-map.md).
- `DeleteArchive` writing is duplicated three times instead of using the macro.
- The manifest does not persist `active`; a fresh `Uuid::new_v4()` is chosen on every
  startup, so every restart begins a new active archive and leaves the previous one to be
  reclaimed later.
