# Partitions

A partition is the unit of storage, of caching, of eviction, and of IO. Everything in Shoal is
addressed by partition key first.

## MaybeLoaded

The central type. A partition in memory is in one of two states:

```rust
pub enum MaybeLoaded<P: PartitionSupport, B = ReadResult> {
    /// A fully loaded partition
    Loaded { partition: P, generation: u64 },
    /// An accessible but not fully loaded partition
    Accessible(ValidatedArchive<P, B>),
}
```

`shoal-core/src/server/tables/partitions.rs`

```
     read from archive              first mutation
  ─────────────────────▶ Accessible ──────────────▶ Loaded
                    validated once here             (deserialized)
                         (raw bytes)                      │
                              │                           │ evictable only once
                              │ filters/reads run         │ generation <= flushed
                              │ directly on the archive   │
                              ▼                           ▼
                          always evictable            eviction
```

**`Accessible` is the interesting one.** It holds the bytes straight off disk — no deserialization
has happened. rkyv lets Shoal read that buffer in place, so a partition can be searched, filtered,
and answered from without ever being turned into Rust structs:

```rust
MaybeLoaded::Accessible(read) => {
    let access = read.archived();
    for row in access.live_row_values() {
        if params.limit_reached(found) { break; }
        if let Some(filter) = &params.filters {
            if !R::is_filtered_archived(filter, row) { continue; }
        }
        found.push(P::from_archived(row));
    }
}
```

`.../tables/partitions.rs`

Note `is_filtered_archived` — the derive macro generates a filter that operates on
`<R as Archive>::Archived`, so rows that fail the filter are never deserialized. A selective
query over a large partition deserializes only what it returns. This is the payoff for
choosing rkyv as the on-disk format.

`P::from_archived` is where that payoff is taken further. `P` is what this get asked to be answered
with: for a get that named no projection it is the row itself, whose `from_archived` is the
`R::deserialize` this line used to be. For a get that named a projection it reads only the fields
that projection declared straight out of the archive, so the rest of the row stays where it is
([F2](../features/projections.md)). The scan is monomorphised over `P`, so neither case pays for
the other.

**`read.archived()` used to be `SortedPartition::<R>::access(read).unwrap()`**, and the difference
is the whole of [F4](../features/validated-archives.md). `access` is rkyv's *checked* entry point:
it ran `bytecheck` over the entire buffer before the scan above could start, on **every query**, so
a get naming one row of a 4,096-row partition spent 29.89 µs of 30.43 µs validating rows it was not
going to look at. The bytes are validated once now, when the read that produced them lands, and
`Accessible` holds a `ValidatedArchive` that carries that fact rather than a bare buffer. Nothing is
validated less often in total — a corrupt archive fails the read instead of the query.

The second type parameter is why any of the archived path can be tested at all: a glommio
`ReadResult` can only come from a live reactor, so before F4 the `Accessible` arm could not be
constructed outside a running server. It defaults to `ReadResult` and production never names it.

### The scan lives on the partition

Both arms are covered by one method per table kind — `MaybeLoaded<SortedPartition<R>>::get` and
`MaybeLoaded<UnsortedPartition<R>>::get` — so the resident and archived paths cannot drift apart.
The sorted one was missing for a long time, and the table inlined a copy of each arm instead;
neither copy had a limit check, which is how `LIMIT` came to be accepted and discarded
([`limit` was ignored by persistent sorted tables](../appendix/resolved/sorted-limit.md)). The
sorted side has a matching pair for `exists`, added when it stopped answering about the partition
instead of the row ([Sort keys were accepted and ignored](../appendix/resolved/sort-keys.md)).

Each of those methods has three shapes inside it on the sorted side, one per `SortSelect` arm:

| `SortSelect` | Resident | Archive read in place |
| --- | --- | --- |
| `All` | walk `live_row_values()` | walk `live_row_values()` |
| `Keys([..])` | `BTreeMap::get` per key | `ArchivedBTreeMap::get` per key |
| `Range(..)` | `BTreeMap::range` | `ArchivedBTreeMap::range` |

A seek that lands on a `MaybeRow::Tombstone` has found a deleted row, so it is a miss and not a
reason to look further; a range skips tombstones the way an unnarrowed walk does. **A range is
checked for emptiness before either seek**, because `BTreeMap::range` panics on one whose start is
past its end and on one whose ends meet on a key neither includes
([F1](../features/sort-key-ranges.md#invariants-to-uphold)).

What the three arms do *not* each have is their own copy of the filter, the limit check, and the
push. Those live once in `collect_rows` and `any_row` on the resident side and in
`collect_archived` and `any_archived` on the archived one; an arm builds an iterator and hands it
over. Adding a fourth way to select rows should not be a fourth place to check the limit in the
wrong order.

A key or a bound being sought in an *archive* has to be in the archived form first, which
`SeekBytes` builds — at most once per query execution, and only when a partition of it is actually
being read in place, so an all-resident get pays nothing for it.

`found` is the accumulator the whole get shares, not a per-partition buffer. That is why the
limit is checked against it rather than against a local count, and why it is checked *before* the
push: a scan can be handed a vec that is already full, either by an earlier partition in the same
get or by an earlier execution of a get that parked on a disk read.

Deserialization happens lazily, on the first *mutation*: insert, update, and delete all
convert `Accessible` into `Loaded`, then keep it that way "to avoid future deserialization
costs" (`.../persistent/sorted.rs:999-1005`).

The `generation` on `Loaded` records when the partition was last modified, and gates eviction
([Compaction](../storage/compaction.md#generations)).

```rust
pub fn is_evictable(&self, flushed_generation: u64) -> bool {
    match self {
        Self::Loaded { generation, .. } => *generation <= flushed_generation,
        Self::Accessible(_) => true,
    }
}
```

`.../tables/partitions.rs`

`Accessible` is unconditionally evictable — it is a read-only mirror of bytes already on
disk, so dropping it loses nothing.

## Sizes

```rust
pub trait PartitionSupport: DeepSizeOf {
    fn size(&self) -> usize { self.deep_size_of() }
}

impl<P: PartitionSupport, B> MaybeLoaded<P, B> {
    pub fn size(&self) -> usize {
        match self {
            Self::Loaded { partition, .. } => partition.size(),
            Self::Accessible(archive) => archive.len(),
        }
    }
}
```

`.../tables/partitions.rs`

`ValidatedArchive::len` answers from the length recorded when the archive was validated rather
than from the buffer, which is what keeps this impl block free of the bounds reading an archive
needs ([F4](../features/validated-archives.md#invariants-to-uphold)).

`deepsize2` walks the structure including heap allocations, so a `String` field counts its
buffer. Both concrete partitions override `size()` to return a cached field rather than
re-walking on every query.

Keeping those cached fields correct is fiddly, and they are not:

- `UnsortedPartition::new` sets `size = row.deep_size_of() + 17` — the row plus a fixed
  overhead, commented as "8 for key, 8 for size, 1 for evictable"
  (`.../tables/partitions.rs:88-90`).
- `UnsortedPartition::update` sets `size = self.deep_size_of()` — the whole partition
  (`.../tables/partitions.rs:120`).

Two different bases for the same field, so an update shifts a partition's accounted size for
reasons unrelated to the data. See
[Known Issues](../appendix/known-issues.md#22-size-accounting-inconsistencies).

`SortedPartition` maintains its size incrementally by delta on every mutation
(`.../tables/partitions.rs:308-330`, `:373-425`, `:466-487`), which avoids re-walking a large
`BTreeMap` but accumulates drift and undercounts tombstones (see below).

`merge_from_disk` (`.../tables/partitions.rs:442-462`) is the one exception: it sums the live
rows it ended up holding. Neither input's size describes their union, and inheriting the disk
copy's used to make a partition that had just grown report that it shrank
([Resolved #6](../appendix/resolved/memory-accounting.md)).

## Tombstones

```rust
pub enum MaybeRow<R> {
    Row(R),
    Tombstone,
}
```

`.../tables/partitions.rs:57-63`

A delete in a sorted table does not remove the entry — it replaces it with a tombstone:

```rust
pub fn remove(&mut self, sort: &T::Sort) -> Option<(usize, T)> {
    if !matches!(self.rows.get(sort), Some(MaybeRow::Row(_))) { return None; }
    match self.rows.insert(sort.clone(), MaybeRow::Tombstone) {
        Some(MaybeRow::Row(removed)) => {
            let row_size = removed.deep_size_of();
            self.size = self.size.saturating_sub(row_size);
            Some((row_size, removed))
        }
        _ => unreachable!(),
    }
}
```

`.../tables/partitions.rs:373-390`

**Why a tombstone is necessary.** A partition in memory may be only part of the story — the
rest may still be in an archive that has not been read. If a delete simply removed the row,
a later `load_partition` would merge the archive copy back in and resurrect it. The tombstone
is the record that says "this row is gone", and it survives the merge:

```rust
pub fn merge_from_disk(&mut self, disk: Self) {
    // keep our in memory rows to the side and make the disk copy our base
    let memory = std::mem::replace(self, disk);
    // replay our in memory rows ontop of the disk copy
    self.rows.extend(memory.rows.into_iter());
    // archives never contain tombstones so only our in memory ones survived
    self.tombstones = memory.tombstones;
    // we just merged in the full disk copy so there is nothing left to load
    self.check_disk = false;
}
```

`.../tables/partitions.rs:437-446`

The disk copy is installed as the base and the in-memory rows — tombstones included — are
extended over it. `BTreeMap::extend` overwrites on key collision, so memory wins. That is the
correct precedence: memory is newer.

There are two entry points, deliberately different:

| Method | Behaviour | Used by |
| --- | --- | --- |
| `remove` | Tombstones **only if** a live row exists; returns `None` otherwise | Live deletes, so a delete of a nonexistent row reports `false` |
| `tombstone` | Tombstones unconditionally | Intent replay, where the row may be on disk and not yet read (`.../tables/partitions.rs:400-425`) |

Reads skip tombstones through dedicated iterators:

```rust
pub fn live_rows(&self) -> impl Iterator<Item = (&T::Sort, &T)> { ... }
pub fn live_row_values(&self) -> impl Iterator<Item = &T> { ... }
```

`.../tables/partitions.rs:490-510`, with archived equivalents at `:517-540`.

**Tombstones die at compaction**, where the rewritten archive simply omits the row:

```rust
SortedIntents::Delete { sort_key, .. } => {
    // truly remove during compaction - no tombstone needed since
    // the new archive won't contain the deleted row
    entry.rows.remove(&sort_key);
}
```

`.../persistent/sorted.rs:1383-1386`

Until then a tombstone occupies a `BTreeMap` slot while contributing nothing to the
partition's accounted `size` — the row's bytes were subtracted on delete and the tombstone
adds none back. A delete-heavy partition therefore reports a smaller size than it occupies,
and eviction under-accounts it.

**Until then, and no longer.** Once the log holding a delete has been compacted, the archive
behind the partition no longer contains the row and the tombstone has nothing left to shadow.
That moment is exactly when the compactor reports the partition evictable, so the sweep happens
there:

```rust
pub fn drop_tombstones(&mut self) -> usize {
    // bail out early if we have nothing to sweep
    if self.tombstones == 0 {
        return 0;
    }
    // only keep rows that still hold data
    self.rows.retain(|_, row| matches!(row, MaybeRow::Row(_)));
    // our tombstones are all gone now
    std::mem::take(&mut self.tombstones)
}
```

`.../tables/partitions.rs:454-463`

The count exists so that sweeping does not have to walk every row of every marked partition to
discover there is nothing to sweep; `mark_evictable` can be handed a thousand keys at a time.
It is never written to an archive (`#[rkyv(with = Skip)]`), because a partition read back from
disk has no tombstones by construction.

Dropping one early is the whole of
[Resolved Issues #5](../appendix/resolved/resurrected-deletes.md): the row it was hiding comes
straight back on the next read. The generation gate is what makes the sweep safe, and nothing
else may be substituted for it — not a size threshold, not an age, not LRU pressure.

**Unsorted tables tombstone too**, for the same reason and at a coarser grain. An unsorted
partition holds one row, so it does not tombstone an entry inside itself — it *becomes* a
tombstone:

```rust
pub fn tombstone(key: u64) -> Self {
    // a tombstone carries no row data so it only costs its fixed overhead
    UnsortedPartition {
        key,
        row: MaybeRow::Tombstone,
        size: 17,
    }
}
```

A delete installs that in place of the partition rather than removing the key, and every read
path treats it as absent. Removing the key was the old behaviour, and it was only safe while
unsorted deletes could not reach a partition that existed on disk — once they could, dropping
the key left nothing to shadow the archive copy with, and the next read faulted the deleted
row back in
([Resolved Issues #4](../appendix/resolved/unsorted-disk-consultation.md)).

The unsorted tombstone's lifetime is bounded by the generation machinery rather than by a
merge: it is created at the current generation, so it becomes evictable exactly when the log
holding its `Delete` intent has been compacted — which is the same moment the compactor drops
the partition's `ArchiveEntry` ([Compaction](../storage/compaction.md#3-apply)). After that
there is nothing left to shadow and 17 bytes to reclaim.

## check_disk

```rust
pub struct SortedPartition<T: ShoalSortedTable> {
    key: u64,
    pub rows: BTreeMap<T::Sort, MaybeRow<T>>,
    size: usize,
    pub check_disk: bool,
    #[rkyv(with = Skip)]
    tombstones: usize,
}
```

`.../tables/partitions.rs:270-287`

`check_disk` answers: *might there be more of this partition on disk?*

It starts `true` for a newly created partition (`.../tables/partitions.rs:294-303`), because a
partition created by an insert may be shadowing an archive copy. It is set to `false` in three
kinds of place, and the third of them is the newest:

- once the full archive copy has been merged in (`SortedPartition::merge_from_disk`);
- when a partition has been deserialized from an `Accessible` read, in `insert`, `update` and
  `delete` — an accessible partition **is** the copy from disk, so the loaded partition built out
  of it inherits nothing to check. `insert` and `update` did not do this until
  [Resolved #80](../appendix/resolved/never-flushed-partitions.md), so an update to a partition
  read from disk could send it back to consulting disk once more;
- when storage answers that there is no archive at all (`mark_absent_from_disk`, reached from the
  one place either sorted path asks). This is the answer the flag used to throw away, which meant
  a partition that had only ever been written to asked on every single query for the rest of its
  life — and, because a partition that might have rows on disk may not be answered in place, was
  refused [F27](../features/grouped-responses.md)'s borrowing path permanently
  ([Resolved #80](../appendix/resolved/never-flushed-partitions.md)).

**Clearing it on an absent archive is only sound because an archive can only be built from this
shard's own intent log over this partition's previous archive**, and every one of those intents was
applied to the copy in memory when it was accepted. A compaction that could pull rows in from
anywhere else would have to set the flag back.

Every sorted read consults it before answering:

```rust
MaybeLoaded::Loaded { partition, .. } => {
    if partition.check_disk {
        // parks this query and returns true if this partition has to be read first
        if self.block_on_load(*partition_key, &meta, blocked_query).await { continue; }
    }
    for row in partition.live_row_values() { ... }
}
```

`.../persistent/sorted.rs`, the get path

**A read that fails leaves `check_disk` true.** Setting it false there would say memory holds
everything, about a partition that was never read — which is why the flag is deliberately left
alone when a read gives up, and why a later query tries the archive again
([Resolved #16, 51](../appendix/resolved/partition-load-failure.md#invariants-to-uphold)).

Without it, an insert into a partition that also exists on disk would make subsequent reads
return only the newly inserted rows. `load_partition` on the storage engine is cheap when
there is nothing to load — one hash lookup in the archive map, no IO
([Storage Overview](../storage/overview.md#the-archive-map)) — but it is asked **once per
partition** now rather than once per query, which is what makes the flag's other job, deciding
whether a get can be answered in place, reachable at all.

`UnsortedPartition` has no `check_disk` because a partition holds exactly one row: if it is in
memory, it is complete.

## PartitionSupport and RkyvSupport

Both partition types implement:

- `PartitionSupport` — sizing (`.../tables/partitions.rs:262-266`, `:546-553`).
- `RkyvSupport` — `serialize`/`access`/`deserialize` (`.../tables/partitions.rs`). `access` is
  still the checked entry point, and is still what validates an archive - it is now called once
  per read from disk, by `ValidatedArchive::new`, rather than once per query
  ([F4](../features/validated-archives.md)).
- `IntentReadSupport<T>` — the recovery and compaction hooks: `scan_keys`, `replay`,
  `apply_intents`, `partition_key_and_intent` (`.../server/tables/storage.rs`).

`scan_keys` only *names* the partitions an intent needs loaded; the loading itself belongs to
the storage engine, which does it for every log at once before replaying any of them. It used to
be a `scan` that loaded as it went, and that cost committed data
([item 31](../appendix/resolved/multi-log-recovery.md)). `replay` and `apply_intents` each take a
`&mut RecoveryStats` and count anything they cannot apply
([Recovery](../storage/recovery.md#what-recovery-discards)).

`IntentReadSupport` is where each table type defines what its intents mean. It is implemented
on the *partition* type rather than the table, because compaction works on partitions without
a table around ([Compaction](../storage/compaction.md)).

## Design notes

**Lazy deserialization as a first-class state.** Rather than a cache of deserialized
partitions over a store of serialized ones, `MaybeLoaded` makes "still bytes" a normal state
that reads work against directly. Deserialization is triggered by mutation, not by access, so
read-mostly workloads may never pay it.

**Tombstones because reads are lazy.** In a system that always loaded a whole partition before
mutating it, a delete could just remove the row. Shoal wants to accept a delete without
reading the partition first, which forces a tombstone.

**Cached sizes because eviction needs them cheaply.** Memory accounting is checked once per
shard loop iteration, so `size()` has to be O(1). The cost is the incremental-update bookkeeping
being easy to get wrong, and it is wrong in the ways noted above.

## Limitations

- Tombstones occupy memory but are not accounted for in partition size. Their lifetime is
  bounded — they are swept when their partition is marked evictable — but a partition that is
  written to continuously is never marked, so its tombstones are never swept either.
- `UnsortedPartition` computes its size two different ways.
- Sorted partition sizes drift, since they are maintained by delta rather than recomputed.
- `access(...).unwrap()` on `Accessible` partitions in many places
  (`.../persistent/sorted.rs:245`, `:355`, `:453`, `:599`, `:734`, `:890`) — a corrupt archive
  panics the shard rather than surfacing an error.
- No partition-level checksum; corruption in an archive is caught only if rkyv validation
  happens to reject it.
