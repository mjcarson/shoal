# Table Types

Shoal has three table implementations. Two are usable; one is effectively orphaned.

| Type | Partition holds | Persistent | Reachable from `#[db]` |
| --- | --- | --- | --- |
| `PersistentSortedTable<R, S, N>` | A `BTreeMap<R::Sort, MaybeRow<R>>` | Yes | Yes |
| `PersistentUnsortedTable<R, S, N>` | Exactly one row | Yes | Yes |
| `EphemeralTable<T>` | A `SortedPartition<T>` | No | **No** |

## PersistentSortedTable

`shoal-core/src/server/tables/persistent/sorted.rs:90-116`

```rust
pub struct PersistentSortedTable<R: ShoalSortedTable, S: StorageSupport, N: TableNameSupport> {
    table_name: N,
    pub partitions: HashMap<u64, MaybeLoaded<SortedPartition<R>>>,
    storage: S,
    generation: u64,
    pending: PendingResponse<R>,
    pending_data: HashMap<(Uuid, usize), (Vec<R>, Vec<u64>)>,
    flushed: Vec<(Uuid, Uuid, Span, Response<R>)>,
    loader_tx: AsyncSender<LoaderMsg<N>>,
    blocked: HashMap<u64, Vec<(QueryMetadata, SortedQuery<R>)>>,
    memory_usage: Arc<RefCell<usize>>,
    lru: Arc<RefCell<LruCache<(N, u64), usize, BuildHasherDefault<GxHasher>>>>,
}
```

Many rows per partition, ordered by a sort key:

```rust
pub struct SortedPartition<T: ShoalSortedTable> {
    key: u64,
    pub rows: BTreeMap<T::Sort, MaybeRow<T>>,
    size: usize,
    pub check_disk: bool,
}
```

`.../tables/partitions.rs:204-214`

Requires at least one `#[shoal(partition)]` and at least one `#[shoal(sort)]` field; the
derive macro panics otherwise (`shoal-derive/src/lib.rs:62-69`).

Three fields do the asynchrony bookkeeping — `pending` (awaiting durability), `blocked`
(awaiting a disk read), `pending_data` (partial results for a multi-partition get) — all
described in [Query Execution](query-execution.md).

### What "sorted" buys you

A sorted table gives you:

- multiple rows per partition key,
- deterministic iteration order — a partition answers in sort-key order,
- per-row deletes and updates addressed by sort key,
- **point lookups by sort key.** `SortedGet` and `SortedExists` both carry
  `sort_keys: Vec<R::Sort>`, and a query naming any of them is answered by seeking each key in the
  `BTreeMap` rather than walking the partition. The same is true of a partition being read in
  place from an archive, which is sought through `ArchivedBTreeMap::get`. An empty list still means
  "every row in this partition".

The sort keys used to be carried to the table and thrown away, which is
[item 8](../appendix/resolved/sort-keys.md) — worth reading before changing either scan, since it
records what a seek is allowed to skip and what it is not.

What a sorted table still does not give you is a **range** predicate: `title >= 'M'`, or a cursor
to page through a large partition with. That is [TODOs](../appendix/todos.md#sort-key-range-predicates),
and a composite sort key cannot be named from SHQL at all
([item 42](../appendix/known-issues.md#42-shql-cannot-express-a-composite-sort-key)).

## PersistentUnsortedTable

`shoal-core/src/server/tables/persistent/unsorted.rs:87-108`

Structurally the same minus `pending_data` — an unsorted get targets exactly one partition, so
there are no partial results to accumulate.

```rust
pub struct UnsortedPartition<R: ShoalUnsortedTable> {
    pub key: u64,
    pub row: MaybeRow<R>,
    pub size: usize,
}
```

`.../tables/partitions.rs:63-71`

The `MaybeRow` is what lets a deleted partition stay in memory as a tombstone shadowing its
archive copy ([Partitions](partitions.md#tombstones)).

One row per partition. Inserting to an existing key replaces the row outright
(`.../persistent/unsorted.rs:349`) — there is no merge, since there is nothing to merge with.

Requires a partition key and forbids sort keys; the macro panics on
`#[shoal(sort)]` (`shoal-derive/src/lib.rs:149`).

### Both types consult disk on every operation

| Operation | Sorted | Unsorted |
| --- | --- | --- |
| `get` | Loads from disk if needed | Loads from disk if needed |
| `exists` | Loads from disk if needed | Loads from disk if needed |
| `delete` | Loads from disk if needed | Loads from disk if needed |
| `update` | Loads from disk if needed | Loads from disk if needed |

They get there differently, because of what "not resident" can mean for each. A sorted
partition can be *partially* resident — some rows in memory, more on disk — so it carries a
`check_disk` flag and a mutation that misses in memory has to check it before answering
(`.../persistent/sorted.rs:698-720`, `:857-876`). An unsorted partition holds exactly one row,
so residency is all or nothing: the only case that needs disk is a key missing from
`self.partitions` entirely.

```rust
None => {
    // this partition may still be on disk so check there before
    // telling our client it doesn't exist
    if self
        .block_on_load(key, &meta, UnsortedQuery::Delete { key })
        .await
    {
        // wait for this partition to be loaded and this query replayed
        return None;
    }
    /* respond Delete(false) */
}
```

`block_on_load` is shared by all four unsorted operations. It parks the query on `blocked` and
returns `true`, or returns `false` when the archive map has no entry for the key — which is
the only case where "the row does not exist" is a truthful answer. It also queues behind an
existing `blocked` entry rather than requesting a second read of a partition already in
flight.

This asymmetry used to be a real one: unsorted `delete` and `update` consulted memory only and
reported `false` for anything evicted or not yet faulted in. See
[Resolved Issues #4](../appendix/resolved/unsorted-disk-consultation.md)
for what that cost and what fixing it dragged in with it.

## EphemeralTable

`shoal-core/src/server/tables/ephemeral.rs`

```rust
pub struct EphemeralTable<T: ShoalSortedTable> {
    pub partitions: BTreeMap<u64, SortedPartition<T>>,
    memory_usage: usize,
}
```

`.../ephemeral.rs:16-22`

In-memory only. No storage engine, no intent log, no eviction, no `MaybeLoaded` — partitions
are always fully resident. Its `handle` returns a `Response<T>` directly rather than an
`Option`, since nothing is ever deferred (`.../ephemeral.rs:47-77`).

It is exported (`shoal/src/lib.rs`) and documented in the CLAUDE.md table list, but the `#[db]`
macro's generated `ShoalDatabase` impl calls methods `EphemeralTable` does not have —
`new(shard_name, table_name, ..., loader_channels, ...)`, `loader_kind`, `spawn_loader`,
`get_flushed`, `mark_evictable`, `evict`, `load_partition`, `shutdown`
(`shoal-derive/src/traits/db.rs:22-185`). Putting an `EphemeralTable` in a `#[db]` struct will
not compile.

It is also `BTreeMap`-keyed on a hash, so its ordering is by hash value — arbitrary.

Treat it as dead code pending either a `#[db]` integration or removal.

## Shared surface

Both persistent tables implement the same informal interface, called by generated code:

| Method | Called from | Purpose |
| --- | --- | --- |
| `new` | `ShoalDatabase::new` | Construct, replay intents, force a compaction |
| `handle` | `ShoalDatabase::handle` | Execute one query |
| `load_partition` | `ShoalDatabase::load_partition` | Install a faulted-in partition, return unblocked queries |
| `flush` | `ShoalDatabase::flush` | Push staged intent bytes toward disk |
| `get_flushed` | `ShoalDatabase::handle_flushed` | Compact if needed; release acknowledgeable responses |
| `mark_evictable` | `ServerMsg::MarkEvictable` | Offer partitions to the LRU |
| `evict` | Shard memory pressure | Drop partitions |
| `shutdown` | `ShoalDatabase::shutdown` | Flush and close storage |

There is no trait here — the derive macro generates direct method calls, so the two tables
agree by convention. That is why the delete/update asymmetry above compiles: nothing requires
the two implementations to behave alike.

## Design notes

**Two table types, not one generic one.** A sorted table pays for a `BTreeMap` and a sort key
per partition; an unsorted table stores a row inline. Making unsorted a special case of sorted
would impose that cost on the common key-value shape, so they are separate types with
duplicated logic. The duplication is real — `sorted.rs` and `unsorted.rs` share their overall
shape and diverge in details, which is exactly how the delete/update asymmetry arose.

**Tables own their storage engine.** Each table has its own `FileSystem<D>`, hence its own
intent log, compactor, and archive map. Tables do not interfere with one another, at the cost
of one background compactor task per table per shard.

**No trait for the table interface.** Generated code calls methods by name. This dodges the
generic-parameter explosion visible in the `where` clauses (`.../persistent/sorted.rs:118-149`
is a 30-line bound list) but removes the compiler's ability to enforce that the two
implementations agree.

## Limitations

- Sort keys are not usable as a query predicate.
- Unsorted updates and deletes ignore data on disk.
- `EphemeralTable` cannot be used in a `#[db]` database.
- No trait unifies the table implementations, so behavioural divergence is silent.
- Partition keys are hashes; for unsorted tables a hash collision silently overwrites a row
  ([Partitioning](../architecture/partitioning.md#from-field-values-to-a-partition-key)).
