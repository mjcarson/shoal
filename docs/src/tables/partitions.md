# Partitions

A partition is the unit of storage, of caching, of eviction, and of IO. Everything in Shoal is
addressed by partition key first.

## MaybeLoaded

The central type. A partition in memory is in one of two states:

```rust
pub enum MaybeLoaded<P: PartitionSupport> {
    /// A fully loaded partition
    Loaded { partition: P, generation: u64 },
    /// An accessible but not fully loaded partition
    Accessible(ReadResult),
}
```

`shoal-core/src/server/tables/partitions.rs:28-34`

```
     read from archive              first mutation
  ─────────────────────▶ Accessible ──────────────▶ Loaded
                         (raw bytes)                (deserialized)
                              │                          │
                              │ filters/reads run        │ evictable only once
                              │ directly on the archive  │ generation <= flushed
                              ▼                          ▼
                          always evictable            eviction
```

**`Accessible` is the interesting one.** It holds the raw `ReadResult` straight off disk — no
deserialization has happened. rkyv lets Shoal read that buffer in place, so a partition can be
searched, filtered, and answered from without ever being turned into Rust structs:

```rust
MaybeLoaded::Accessible(read) => {
    let partition = SortedPartition::<R>::access(&read).unwrap();
    for row in partition.live_row_values() {
        if let Some(filters) = &get.filters {
            if !R::is_filtered_archived(filters, row) { continue; }
        }
        let loaded_row = R::deserialize(row).unwrap();
        data.push(loaded_row);
    }
}
```

`.../persistent/sorted.rs:450-469`

Note `is_filtered_archived` — the derive macro generates a filter that operates on
`<R as Archive>::Archived`, so rows that fail the filter are never deserialized. A selective
query over a large partition deserializes only what it returns. This is the payoff for
choosing rkyv as the on-disk format.

Deserialization happens lazily, on the first *mutation*: insert, update, and delete all
convert `Accessible` into `Loaded`, then keep it that way "to avoid future deserialization
costs" (`.../persistent/sorted.rs:917-923`).

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

`.../tables/partitions.rs:46-51`

`Accessible` is unconditionally evictable — it is a read-only mirror of bytes already on
disk, so dropping it loses nothing.

## Sizes

```rust
pub trait PartitionSupport: DeepSizeOf {
    fn size(&self) -> usize { self.deep_size_of() }
}

impl<P: PartitionSupport> MaybeLoaded<P> {
    pub fn size(&self) -> usize {
        match self {
            Self::Loaded { partition, .. } => partition.size(),
            Self::Accessible(read) => read.len(),
        }
    }
}
```

`.../tables/partitions.rs:20-43`

`deepsize2` walks the structure including heap allocations, so a `String` field counts its
buffer. Both concrete partitions override `size()` to return a cached field rather than
re-walking on every query.

Keeping those cached fields correct is fiddly, and they are not:

- `UnsortedPartition::new` sets `size = row.deep_size_of() + 17` — the row plus a fixed
  overhead, commented as "8 for key, 8 for size, 1 for evictable"
  (`.../tables/partitions.rs:80-82`).
- `UnsortedPartition::update` sets `size = self.deep_size_of()` — the whole partition
  (`.../tables/partitions.rs:112`).

Two different bases for the same field, so an update shifts a partition's accounted size for
reasons unrelated to the data. See
[Known Issues](../appendix/known-issues.md#22-size-accounting-inconsistencies).

`SortedPartition` maintains its size incrementally by delta on every mutation
(`.../tables/partitions.rs:236-257`, `:301-337`, `:343-362`), which avoids re-walking a large
`BTreeMap` but accumulates drift and undercounts tombstones (see below).

## Tombstones

```rust
pub enum MaybeRow<R> {
    Row(R),
    Tombstone,
}
```

`.../tables/partitions.rs:55-61`

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

`.../tables/partitions.rs:301-318`

**Why a tombstone is necessary.** A partition in memory may be only part of the story — the
rest may still be in an archive that has not been read. If a delete simply removed the row,
a later `load_partition` would merge the archive copy back in and resurrect it. The tombstone
is the record that says "this row is gone", and it survives the merge:

```rust
std::mem::swap(&mut new, partition);              // disk copy becomes the base
partition.rows.extend(new.rows.into_iter());      // in-memory rows overlay it
partition.check_disk = false;
```

`.../persistent/sorted.rs:247-253`

The disk copy is installed as the base and the in-memory rows — tombstones included — are
extended over it. `BTreeMap::extend` overwrites on key collision, so memory wins. That is the
correct precedence: memory is newer.

There are two entry points, deliberately different:

| Method | Behaviour | Used by |
| --- | --- | --- |
| `remove` | Tombstones **only if** a live row exists; returns `None` otherwise | Live deletes, so a delete of a nonexistent row reports `false` |
| `tombstone` | Tombstones unconditionally | Intent replay, where the row may be on disk and not yet read (`.../tables/partitions.rs:328-337`) |

Reads skip tombstones through dedicated iterators:

```rust
pub fn live_rows(&self) -> impl Iterator<Item = (&T::Sort, &T)> { ... }
pub fn live_row_values(&self) -> impl Iterator<Item = &T> { ... }
```

`.../tables/partitions.rs:365-378`, with archived equivalents at `:387-413`.

**Tombstones die at compaction**, where the rewritten archive simply omits the row:

```rust
SortedIntents::Delete { sort_key, .. } => {
    // truly remove during compaction - no tombstone needed since
    // the new archive won't contain the deleted row
    entry.rows.remove(&sort_key);
}
```

`.../persistent/sorted.rs:1288-1292`

Until then a tombstone occupies a `BTreeMap` slot while contributing nothing to the
partition's accounted `size` — the row's bytes were subtracted on delete and the tombstone
adds none back. A delete-heavy partition therefore reports a smaller size than it occupies,
and eviction under-accounts it.

Unsorted tables have no tombstones. A delete removes the map entry
(`.../persistent/unsorted.rs:540`) — which, combined with the fact that unsorted deletes never
consult disk, is why a delete against an on-disk-only partition silently does nothing
([Table Types](table-types.md#the-asymmetry-that-matters)).

## check_disk

```rust
pub struct SortedPartition<T: ShoalSortedTable> {
    key: u64,
    pub rows: BTreeMap<T::Sort, MaybeRow<T>>,
    size: usize,
    pub check_disk: bool,
}
```

`.../tables/partitions.rs:204-214`

`check_disk` answers: *might there be more of this partition on disk?*

It starts `true` for a newly created partition (`.../tables/partitions.rs:222-229`), because a
partition created by an insert may be shadowing an archive copy. It is set to `false` once the
full archive copy has been merged in (`.../persistent/sorted.rs:252`) or a partition has been
deserialized from an `Accessible` read (`.../persistent/sorted.rs:739`, `:1164`, `:1210`,
`:1250`).

Every sorted read consults it before answering:

```rust
MaybeLoaded::Loaded { partition, .. } => {
    if partition.check_disk {
        let will_load = self.storage.load_partition(self.table_name, *partition_key, &self.loader_tx).await.unwrap();
        if will_load { /* park the query and wait */ continue; }
    }
    for row in partition.live_row_values() { ... }
}
```

`.../persistent/sorted.rs:407-448`

Without it, an insert into a partition that also exists on disk would make subsequent reads
return only the newly inserted rows. `load_partition` on the storage engine is cheap when
there is nothing to load — one hash lookup in the archive map, no IO
([Storage Overview](../storage/overview.md#the-archive-map)).

`UnsortedPartition` has no `check_disk` because a partition holds exactly one row: if it is in
memory, it is complete.

## PartitionSupport and RkyvSupport

Both partition types implement:

- `PartitionSupport` — sizing (`.../tables/partitions.rs:197-201`, `:420-427`).
- `RkyvSupport` — `serialize`/`access`/`deserialize` (`.../tables/partitions.rs:195`, `:415`).
- `IntentReadSupport<T>` — the recovery and compaction hooks: `scan`, `replay`,
  `apply_intents`, `partition_key_and_intent` (`.../server/tables/storage.rs:122-165`).

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

- Tombstones occupy memory but are not accounted for in partition size.
- `UnsortedPartition` computes its size two different ways.
- Sorted partition sizes drift, since they are maintained by delta rather than recomputed.
- `access(...).unwrap()` on `Accessible` partitions in many places
  (`.../persistent/sorted.rs:245`, `:355`, `:453`, `:599`, `:734`, `:890`) — a corrupt archive
  panics the shard rather than surfacing an error.
- No partition-level checksum; corruption in an archive is caught only if rkyv validation
  happens to reject it.
