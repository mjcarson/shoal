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

### What "sorted" currently buys you

Less than the name suggests. Rows are stored in sort-key order and iterate in that order, but
**no query predicate uses the sort key**. `SortedGet` and `SortedExists` both carry a
`sort_keys: Vec<R::Sort>` field (`shared/queries/sorted.rs:74-83`, `:102-110`) that the server
never reads: `PersistentSortedTable::get` iterates every live row in the partition and applies
only the filters (`.../persistent/sorted.rs:436-448`).

So today a sorted table gives you:

- multiple rows per partition key,
- deterministic iteration order,
- per-row deletes and updates addressed by sort key (`Delete` and `Update` *do* use it),

but not point lookups by sort key, and not range scans. See
[Known Issues](../appendix/known-issues.md#8-sort-keys-are-accepted-and-ignored).

## PersistentUnsortedTable

`shoal-core/src/server/tables/persistent/unsorted.rs:87-108`

Structurally the same minus `pending_data` — an unsorted get targets exactly one partition, so
there are no partial results to accumulate.

```rust
pub struct UnsortedPartition<R: ShoalUnsortedTable> {
    pub key: u64,
    pub row: R,
    pub size: usize,
}
```

`.../tables/partitions.rs:63-71`

One row per partition. Inserting to an existing key replaces the row outright
(`.../persistent/unsorted.rs:349`) — there is no merge and no tombstone, because a delete just
removes the entry.

Requires a partition key and forbids sort keys; the macro panics on
`#[shoal(sort)]` (`shoal-derive/src/lib.rs:149`).

### The asymmetry that matters

Sorted and unsorted tables disagree about whether a mutation should consult disk.

| Operation | Sorted | Unsorted |
| --- | --- | --- |
| `get` | Loads from disk if needed | Loads from disk if needed |
| `exists` | Loads from disk if needed | Loads from disk if needed |
| `delete` | Loads from disk if needed | **Memory only** |
| `update` | Loads from disk if needed | **Memory only** |

`PersistentUnsortedTable::delete` is the whole story:

```rust
match self.partitions.remove(&key) {
    Some(old) => { /* write delete intent, ack */ }
    None => { /* respond Delete(false) */ }
}
```

`.../persistent/unsorted.rs:540-570`

No `load_partition`, no `blocked` entry. `update` has the same shape
(`.../persistent/unsorted.rs:591-637`). So an update or delete against a partition that is on
disk but not in memory — because it was evicted, or because the process restarted and the
partition has not been faulted in — silently reports `false` and does nothing.

The sorted implementations handle this correctly, checking disk and parking the query
(`.../persistent/sorted.rs:698-720`, `:857-876`). See
[Known Issues](../appendix/known-issues.md#4-unsorted-updates-and-deletes-never-consult-disk).

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
