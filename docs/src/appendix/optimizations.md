# Optimizations

Work that would make Shoal faster, indexed the same way as [Known Issues](known-issues.md): each
entry names what the code does, where, and why it costs. Item numbers are prefixed `O` and are
never reused.

**None of these are measured.** They come from reading the source, and they are ordered by the
size of the argument for them, not by observed benefit. Anything here should be confirmed against
a profile before it is acted on — [Benchmarking](../operations/benchmarking.md) and the `hotpath`
feature are what that is for. A change that removes work from a path nothing is waiting on is a
change that only adds risk.

Defects are in [Known Issues](known-issues.md); several entries below share a root cause with one
and say so. An entry that has been done is struck through and kept, with what replaced it, for the
same reason a resolved issue keeps its page.

---

## Read path

### O1. Queries are fully deserialized on arrival

```rust
// load our arhived query from buffer
let archived = Queries::access(&data)?;
// deserialize our queries
let queries = <Queries<D::ClientType> as RkyvSupport>::deserialize(archived)?;
```

`shard.rs:520-522`

Every `String`, `Vec`, and filter in every query of the bundle is allocated and copied out of a
buffer that already holds them in a readable layout. This branch is named for making *responses*
zero-copy; the request half was not converted.

The machinery for it already exists and is unused: `ShoalDatabase::unarchive_queries`
(`shared/traits.rs:307-311`) returns `&ArchivedQueries` via `access_unchecked` and has no callers.

The obstacle is real, though, and worth stating: `send_to_shard` consumes the queries by value
(`shard.rs:458`) and `ServerMsg::Query` carries an owned `QueryKinds` (`messages.rs:91-96`), so
this is not a call-site swap. It needs the archived form to survive as far as the shard that
executes the query, which means the `BytesMut` has to travel with it.

### O2. Every returned row is copied at least twice

`SortedPartition::get` deep-clones out of the `BTreeMap`:

```rust
found.push(row.clone());
```

`tables/partitions.rs:374`

The archive path is worse — it materializes an owned row from bytes per row
(`tables/partitions.rs:609`, and `:203` for unsorted):

```rust
let loaded = R::deserialize(row).unwrap();
found.push(loaded);
```

Then `Shard::reply` serializes the whole `Vec<T>` back into bytes (`shard.rs:543`). A read served
from an `Accessible` partition therefore goes **bytes → owned rows → bytes**, and a read served
from memory goes **rows → cloned rows → bytes**.

The response type is what forces it: `ResponseAction::Get(Option<Vec<T>>)`
(`shared/responses.rs:31`) can only hold owned rows.

**Narrowed, not closed, by [F2](../features/projections.md).** Both lines above are now
`P::from_row(row)` and `P::from_archived(row)`, where `P` is what the get asked to be answered with.
A get that named a projection copies only the fields that projection declared, so the archive path
materializes a smaller owned value and the wire carries less. A get that named none still copies the
whole row twice — the identity projection is exactly the two lines above — so the shape of this
entry is unchanged and only its magnitude moved.

### O3. Every archived read is fully validated, inside a tracing span

```rust
#[instrument(name = "RkyvSupport::access", skip_all, err(Debug))]
fn access(raw: &[u8]) -> Result<&<Self as Archive>::Archived, rkyv::rancor::Error> {
    rkyv::access::<<Self as Archive>::Archived, rkyv::rancor::Error>(raw)
}
```

`shared/traits.rs:62-77`

`rkyv::access` is the *checked* entry point: it runs `bytecheck` over the whole buffer, O(bytes),
every call. The `#[instrument]` adds a span creation and enter on top of that.

Call sites on live request paths, one per query against an `Accessible` partition:
`tables/partitions.rs:189`, `:217`, `:243`, `:260`, `:593`; `.../persistent/sorted.rs:263`,
`:370`, `:610`, `:763`, `:935`.

The bytes were written by this process and read back from a file it owns, so they are validated
once per read from disk at best and once per *query* as it stands. Validating at load and using
`access_unchecked` afterwards removes both the walk and the span from the read path — at the cost
of making [archive checksums](todos.md#archive-checksums) matter more, since validation is
currently the only thing standing between a corrupt archive and a bad pointer.

This is the entry with the best ratio of cost removed to code changed.

### O5. The hottest maps use SipHash

`partitions: HashMap<u64, MaybeLoaded<..>>`, `blocked: HashMap<u64, ..>`, and `pending_data`
(`.../persistent/sorted.rs:98`, `:112`, `:118`; `unsorted.rs:176`, `:193`) all use std's default
hasher. `partitions` is looked up at least once per query.

The LRU sitting beside them already uses `BuildHasherDefault<GxHasher>` (`shard.rs:338`), and
`gxhash` is already a dependency, so this is a type annotation rather than a change.

### O12. `to_blocked` clones the whole filter set per blocked partition

`SortedGet::to_blocked` (`shared/queries/sorted.rs`) clones `sort_select` and `filters` into the
narrowed query, and `get` calls it once for every partition that has to be read from disk. A get
across 100 cold partitions makes 100 copies of the same filters and the same selection, all of
which are then held in `blocked` until the loads land. A range clones two bounds rather than a key
set, so it is the cheaper of the two selections to copy — but the filters dominate either way.

### O13. `blocked.retain(..)` runs inside the per-key loop

`.../persistent/sorted.rs:424`, `:473`, `:630` — a linear scan of the blocked list per partition
key, making a get over *n* keys O(n²). Small *n* today, but *n* is the number of partitions a
single query names, which is the one thing a caller controls directly.

### ~~O19. A wanted sort key is re-archived for every archived partition it is sought in~~

**Done**, by [F1](../features/sort-key-ranges.md). `MaybeLoaded::seek_archived` used to serialize
and validate the key it was looking for once per key per partition, so a get naming *k* sort keys
across *p* archived partitions did that *k × p* times for *k* distinct values.

`SeekBytes` (`.../tables/partitions.rs`) now owns the archived forms of a query's keys and bounds,
and `PersistentSortedTable::get`/`exists` build it **at most once per execution** — lazily, on the
first partition actually being read in place, so a get every one of whose partitions is resident
still builds none of it. Serialization is down from *k × p* to *k*; validation is still per
archived partition, because holding a `&Archived<Sort>` across the loop would need a
self-referential struct.

Kept here rather than deleted because the shape it settled on is the one an equivalent change
elsewhere should copy: bytes in the carrier, references built at the point of use.

### O20. A sort-key get reads a partition it may not need

An in-memory row or tombstone shadows whatever an archive holds for the same key
(`SortedPartition::merge_from_disk`), so a get whose named sort keys are *all* resolved in memory —
as live rows or as tombstones — could answer without reading the partition at all, even with
`check_disk` set. The read is currently unconditional, which is deliberate: see the invariant in
[item 8](resolved/sort-keys.md#invariants-to-uphold). Recorded here rather than lost, with two
warnings attached. It pays only when the *whole* key set hits, and it makes the cost of a query
depend on what happens to be resident, which is the kind of thing that turns a reproducible
latency into a flaky one.

**It does not extend to a range.** A key set can in principle be checked off; a range cannot,
because there is no way to know that the rows in memory are *all* of the rows in that span without
reading the archive that might hold more. Ranges made this optimization strictly narrower rather
than more attractive — see [F1](../features/sort-key-ranges.md#invariants-to-uphold).

---

### O18. The gathered reorder rehashes every row's partition key

`ResponseAction::order_by_partitions` (`shared/responses.rs`) sorts the merged rows of a split
query by where their partition was named. A `Response` carries rows and nothing else, so the only
way to ask a row which partition it came from is to hash its partition key again:

```rust
rows.sort_by_cached_key(|row| ranks.get(&row.get_partition_key()).copied().unwrap_or(usize::MAX));
```

`sort_by_cached_key` keeps that to one hash per row rather than one per comparison, and for a
string partition key a gxhash over the field is cheap next to the row clone that already happened
to get here. Still, the information was known and thrown away: every shard produced its rows
grouped by partition already, in the right relative order.

Two ways out, both bigger than they look. A k-way merge over the shares by partition rank would
be `O(n)` with no hashing, but `merge` is called pairwise as shares arrive rather than once at
the end, so it means buffering the shares and merging them together. Alternatively a share could
carry its rows grouped — `Vec<(u64, Vec<T>)>` rather than `Vec<T>` — which removes the question
entirely, at the cost of a wire format change that lands on the same `ResponseAction::Get` shape
**O2** wants to change for a different reason. Worth doing with O2 rather than before it.

[F2](../features/projections.md) added a second argument for the grouped share. A projection has to
carry its table's partition key for no reason other than this rehash, which is a real constraint on
what a projection is allowed to leave out — a projection of a title alone is not expressible. Taking
this entry would lift that requirement as well as removing the hash.

## Write path

### O4. `deep_size_of()` is a recursive walk called on every mutation

It measures the whole object graph, so its cost is proportional to the row, not constant. Call
sites on the write path:

| Where | Calls per operation |
| --- | --- |
| `SortedPartition::insert` (`tables/partitions.rs:322`, `:328`) | Two — the new row and the one it replaced |
| `SortedPartition::update` (`:501`, `:505`) | Two — before and after |
| `SortedPartition::remove` (`:400`), `tombstone` (`:423`) | One |
| `UnsortedPartition::new` (`:82`), `update` (`:160`) | One |
| `merge_from_disk` (`:462-469`) | Every live row in the merged result |

Carrying a row's measured size alongside it would make all of these O(1). It would also settle
[item 22](known-issues.md#22-size-accounting-inconsistencies) — the mismatched bases between
`UnsortedPartition::new` and `update` exist precisely because the size is re-derived at each site
instead of being owned by one.

### O11. A fresh `AlignedVec` per write and per response

- `FileSystem::commit` (`.../fs.rs:331`) allocates via `RkyvSupport::serialize`, then copies the
  bytes a second time into the DMA buffer (`.../fs.rs:350`).
- `Shard::reply` (`shard.rs:543`) allocates one per response.
- `write_map_intent!` (`.../fs/compactor.rs:44`) allocates one per archive entry written.

rkyv can serialize into a caller-supplied buffer, so all three could reuse one. `commit` is the
interesting one, because the destination buffer it copies into is already there — `prep` hands
back a `&mut [u8]` sized for the record (`.../fs/stream.rs:519-530`).

### O17. `handle_flushed` runs on every message

`shard.rs:791` calls it unconditionally each loop iteration, and it reaches
`tables.handle_flushed` → per-table `get_flushed` → `compact_if_needed`
(`.../persistent/sorted.rs:1097-1116`). So every message pays a pass over every table, including
every `DataFlushed` wakeup — of which there is one per completed write.

---

## Recovery

### ~~O7. Startup reads the same archive once per update intent~~

**Done**, as a side effect of fixing [item 31](resolved/multi-log-recovery.md) rather than as an
optimization in its own right — the correctness fix and this wanted the same change.

`scan` used to be called once per intent record and, per call, allocate a set sized for a
thousand keys in order to hold at most one:

```rust
// build a set of partitions to load from disk
let mut to_load = HashSet::with_capacity(1000);
```

It then called `load_partition_direct` for that key, which opens, reads, and closes an archive.
Because the set was per record, nothing deduplicated across records: *N* update intents against
one partition cost *N* archive reads.

`scan` was split into a synchronous `scan_keys` that only names partition keys, and the loading
moved into `read_intents`, which unions the keys across **every** log and then loads each one
once. So the deduplication is now across all logs rather than merely across the records of one —
and doing it only per log was never an option, because loading after a replay is precisely what
item 31 was.

The set is allocated once per recovery instead of once per record.

Note in passing that recovery still holds every record's `ReadResult` in memory before replaying
any of them, and now does so for every log at once rather than one log at a time, so peak memory
went from the size of the largest log to the total size of all logs. That is a deliberate
trade — see [Recovery](../storage/recovery.md#limitations).

### O22. Recovery loads the partitions it scanned one await at a time

`FileSystem::load_scanned` (`.../storage/fs.rs`) walks the key set the prescan built and awaits
one load per key:

```rust
for partition_key in to_load {
    if partitions.contains_key(&partition_key) { continue; }
    if let Some(partition_read) = self.load_partition_direct(partition_key).await? {
```

This is [O8](#o8-partitions-are-read-one-at-a-time-each-with-its-own-dup-and-close)'s shape on
the recovery path rather than the compaction one, and each iteration also pays
[O15](#o15-one-partition-load-costs-a-dup-and-a-close)'s `dup`/`close`. Neither of those covers
this loop, so it is filed separately — but it should be fixed in the same change as O8, since
grouping by archive file is the same work in both places.

What makes it newly worth filing is that [item 31](resolved/multi-log-recovery.md) is what made
it possible. Under the old `scan` the loads were interleaved with reading, one key at a time,
discovered as each record went past — there was no set to batch. The whole key set is now known
before a single load happens, which is exactly the precondition for grouping them by archive and
issuing them concurrently. The correctness fix handed this optimization its opening.

Worth taking together with the [item 22](known-issues.md#22-size-accounting-inconsistencies)
bullet about this same loop: it is where a partition enters the memory counter in archive bytes,
so whatever touches it next is already reading that line.

**Not taken**, because it is on the startup path rather than a hot one, and the set is normally
small — an interrupted compaction is rare and the active log usually names few partitions.

---

## Compaction

### O8. Partitions are read one at a time, each with its own `dup` and `close`

```rust
for partition in self.changes.keys() {
    if let Some(entry) = self.map.to_archive.borrow().get(partition) {
        let handle = self.map.get_archive(&entry.archive).await?;
        let read = handle.read_at(entry.offset, entry.size).await?;
```

`.../fs/compactor.rs:178-194`

Serially awaited, one read per partition, with no grouping by archive file and no coalescing of
entries that happen to be adjacent in the same archive. `get_archive` returns a `dup` of a cached
handle (`.../fs/map.rs:455-475`) and the caller closes it, so each read also costs a `dup`/`close`
pair. `compact_archives` (`:396-414`) has the same shape.

Grouping `changes` by `entry.archive` before reading would let one handle serve many reads, and
glommio's read APIs can issue them concurrently rather than one await at a time.

(This is also the loop with the borrow-across-await in
[item 35](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor).)

### O9. Every intent log rotation walks the entire on-disk partition set

`compact_if_needed` queues a `CompactionJob::Archives` on every rotation
(`.../fs.rs:385-394`), and that job calls `sort_by_load` (`.../fs/map.rs:516-548`), which iterates
all of `to_archive` and **copies every `ArchiveEntry`** into a fresh
`HashMap<Uuid, Vec<ArchiveEntry>>`:

```rust
for (_, archive_entry) in self.to_archive.borrow().iter() {
    let entry: &mut usize = used_by.entry(archive_entry.archive).or_default();
    *entry += archive_entry.size;
    let entries_entry = sorted.entries.entry(archive_entry.archive).or_default();
    entries_entry.push(*archive_entry);
}
```

The cost is O(total partitions on disk) per rotation, regardless of how few of them changed.

`compact_archives` then opens **every** candidate archive with `DmaFile::open`
(`.../fs/compactor.rs:359`) — bypassing the handle cache in `loaded_archives` that
`get_archive` maintains — purely to call `file_size()`, and closes it again for the ones it skips
on the 50% utilization test (`:363-369`).

Maintaining a per-archive used-byte total incrementally in `set_partition` and `remove_partition`
would replace the whole scan, and archive sizes are already known to the writer.

### O10. `SerializedMap::save` snapshots by cloning

```rust
all_archives: map.all_archives.borrow().clone(),
to_archive: map.to_archive.borrow().clone(),
```

`.../fs/map.rs:205-206` — a full copy of the archive map before every serialization, and
`compact_map` runs whenever the map intent log passes 1 MiB (`.../fs/compactor.rs:269-274`,
`:494-499`).

### O16. Compaction shares the shard's executor

The compactor and the loader are both spawned onto `medium_priority` on the same glommio executor
as the query loop (`.../fs.rs:164-167`, `:486-488`). A long `compact_archives` competes directly
with query serving, and the task queue's share (`Shares::Static(500)` against the high priority
queue's 1000, `shard.rs:320-330`) is the only lever over it. That is a deliberate design — it is
what thread-per-core buys — but it means O8 and O9 are not merely background costs.

Note also that `write_partition` iterates `self.loaded`, a `HashMap`
(`.../fs/compactor.rs:223`), so partitions land in the archive in hash order and reads of
related partitions get no locality from it.

### O21. A forced rotation of an empty intent log does the whole rotation anyway

`compact_if_needed` rotates on `force` without looking at whether the active log holds anything
(`.../fs.rs:397-440`), and startup always forces one
(`.../tables/persistent/sorted.rs:218`). A table nobody wrote to therefore pays, per restart per
shard, a rename, a fresh file for the new active log, a `CompactionJob::IntentLog` that reads a
zero length file, the `glommio::io::remove` that now deletes it
([item 14](resolved/empty-rotated-logs.md)), and a `CompactionJob::Archives` behind it — which is
[O9](#o9-every-intent-log-rotation-walks-the-entire-on-disk-partition-set)'s full walk of the
on-disk partition set, the expensive part by some margin.

Skipping the rotation when the log is empty is not a local change, which is why item 14 cleaned up
after the rotation instead of preventing it: the generation counter, `FlushProgress.rotated`, and
the `MarkEvictable` that advances a table's compacted generation are all keyed off the rotation
happening. Suppressing the `Archives` job alone — queue it only when a rotation had changes to
compact — is the cheap two thirds of this and does not touch generations at all.

Startup cost only, not per write, which is why it is here rather than in Known Issues.

---

## Routing and memory

### ~~O6. The ring is a 1000×N `BTreeMap` answering a question arithmetic would answer~~ — done

**Taken, with [items 11, 12 and 37](resolved/tablet-ring.md).** The ring was replaced by a tablet
map: `find_shard` is now a shift and two indexed loads into a 4096-entry `Vec<u16>` — 8 KiB,
against a 16,000-entry `BTreeMap` — and there is no search at all.

The original entry read:

> `ring.rs:26-44` builds it, `ring.rs:51-68` searches it, and `find_shard` runs once per partition
> key per query. At 16 shards that is a 16,000-entry `BTreeMap` — pointer-chasing, one allocation
> per node — consulted on the hot path.
>
> As built it does not need to be a search structure at all. Every shard uses the same fixed stride
> `RING_JUMP`, so the ring is exactly periodic and the owning shard is computable directly; that is
> the same property that makes the vnodes useless in item 12. Once item 12 is fixed and positions
> become independent, a search is needed again — but a sorted `Vec<(u64, usize)>` with
> `partition_point` is still strictly better than a `BTreeMap` for a structure that is built once
> and then only read.
>
> The two items should be done together: fixing item 12 without touching this doubles down on the
> structure that costs the most.

It was right that the two had to move together, and right that arithmetic could answer the
question — but it framed the choice as *periodic ring, so compute* versus *independent positions,
so search*. Tablets are neither: an explicit assignment table is a third option that is both O(1)
*and* evenly balanced, which is the combination the entry assumed was unavailable. The reason to
prefer it is not speed, though — it is that a stored assignment can be **moved**, which a computed
one cannot, and that is what a distributed Shoal needs.

Still unmeasured, as everything on this page is. The array is small enough to stay cache resident
where the `BTreeMap` was not, but that is an argument, not a profile.

### O14. Fixed thousand-element preallocations on per-call paths

- `evict_data` allocates a `Vec::with_capacity(1000)` per table it touches, to hold however many
  victims that table has (`shard.rs:693-696`).
- `write_partition` allocates `to_mark` at 1000 per call (`.../fs/compactor.rs:219`).
- ~~The per-record `HashSet` in [O7](#o7-startup-reads-the-same-archive-once-per-update-intent).~~
  Gone — it is allocated once per recovery now.

### O15. One partition load costs a `dup` and a `close`

`read_partition_helper` closes the handle the map just handed it (`.../fs/loader.rs:19-28`), even
though `ArchiveMap` caches open handles in `loaded_archives` specifically so it does not have to
reopen (`.../fs/map.rs:332`, `:455-475`). Borrowing the cached handle rather than duplicating it
would remove both syscalls from every partition read.

The cache has the opposite problem at the other end: nothing evicts from `loaded_archives` except
`remove_archive` (`.../fs/map.rs:502-510`), so a table with many archives holds a file descriptor
per archive for the life of the process.

---

## Suggested order

1. **O3**, then **O1** — both remove work from every read, neither changes an on-disk or wire
   format. O3 is the smaller change and the larger win; do it first, and read
   [archive checksums](todos.md#archive-checksums) before dropping validation.
2. **O4** — retires a real correctness wart
   ([item 22](known-issues.md#22-size-accounting-inconsistencies)) with the same edit that removes
   the cost, which makes it the easiest one to justify.
3. **O9**, then **O8** — the only entries whose cost scales with total data on disk rather than
   with request rate. Everything else gets worse under load; these get worse just by existing
   longer.
4. **O5** and **O14** — near-free, and worth doing whenever the surrounding code is open.

**O6** has been taken, together with items 11, 12 and 37 as this list said it had to be.

**O2** is deliberately not on this list. It is the largest single win available on the read path
and also the largest change, because it needs `ResponseAction::Get` to hold something other than
`Vec<T>`, which reaches the wire format and the client. It is worth its own design pass rather
than a slot in an ordering.
