# Table Types

Shoal has **two** table implementations and four names for them. The ephemeral pair are type
aliases for the persistent pair with a storage engine that writes nothing
([F9](../features/ephemeral-tables.md)).

| Type | Partition holds | Persistent | Reachable from `#[db]` |
| --- | --- | --- | --- |
| `PersistentSortedTable<R, S, N>` | A `BTreeMap<R::Sort, MaybeRow<R>>` | Yes | Yes |
| `PersistentUnsortedTable<R, S, N>` | Exactly one row | Yes | Yes |
| `EphemeralSortedTable<R, D, N>` | A `BTreeMap<R::Sort, MaybeRow<R>>` | No | Yes |
| `EphemeralUnsortedTable<R, D, N>` | Exactly one row | No | Yes |

> ~~`EphemeralTable<T>` is a third implementation and cannot be used in a `#[db]` database.~~
> Superseded by [F9](../features/ephemeral-tables.md). The old struct held a
> `BTreeMap<u64, SortedPartition<T>>` directly, had no storage engine and no `MaybeLoaded`, and
> was unreachable from a database struct for two years. It was deleted rather than integrated:
> making it reachable meant giving it the twelve methods the `#[db]` macro generates calls
> against, which is a second implementation of the persistent table's whole interface with
> nothing but convention keeping the two alike.

## PersistentSortedTable

`shoal-core/src/server/tables/persistent/sorted.rs:90-116`

```rust
pub struct PersistentSortedTable<R: ShoalSortedTable, S: StorageSupport, N: TableNameSupport> {
    table_name: N,
    pub partitions: HashMap<u64, MaybeLoaded<SortedPartition<R>>>,
    storage: S,
    generation: u64,
    pending: PendingResponse<R>,
    pending_data: PendingGets,
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
described in [Query Execution](query-execution.md). `pending_data` holds its rows type-erased,
because a parked get can be waiting for whole rows or for any of this table's projections
([F2](../features/projections.md#design-choices)).

### What "sorted" buys you

A sorted table gives you:

- multiple rows per partition key,
- deterministic iteration order — a partition answers in sort-key order,
- per-row deletes and updates addressed by sort key,
- **point lookups by sort key.** `SortedGet` and `SortedExists` both carry
  `sort_select: SortSelect<R::Sort>`, and a query naming keys is answered by seeking each of them
  in the `BTreeMap` rather than walking the partition. The same is true of a partition being read
  in place from an archive, which is sought through `ArchivedBTreeMap::get`.
- **range scans and paging by sort key.** `SortSelect::Range` bounds the rows a get or exists
  wants, and both partition forms seek to the lower bound and stop past the upper one —
  `BTreeMap::range` in memory and `ArchivedBTreeMap::range` in an archive. An exclusive lower
  bound is therefore a cursor: the sort key of the last row of a page names where the next page
  begins, so page *n* costs a seek plus its own rows rather than every row before it.

`SortSelect::All` is what a query that narrowed itself in neither way carries, and is the only
arm that means every row in the partition.

The sort keys used to be carried to the table and thrown away, which is
[item 8](../appendix/resolved/sort-keys.md) — worth reading before changing either scan, since it
records what a seek is allowed to skip and what it is not. Ranges were added by
[F1](../features/sort-key-ranges.md), which records the same for a span.

What a sorted table still does not give you is a range over a *prefix* of a composite sort key,
and a composite sort key cannot be named from SHQL at all
([item 42](../appendix/known-issues.md#42-shql-cannot-express-a-composite-sort-key)). Nor does a
range reduce I/O: a cold partition is read whole either way.

## PersistentUnsortedTable

`shoal-core/src/server/tables/persistent/unsorted.rs:87-108`

Structurally the same. An unsorted get can name several partitions, so it accumulates partial
results in a `pending_data` of its own, one slot per partition
([Query Execution](query-execution.md#slots-not-an-accumulator)).

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

## The ephemeral pair

`shoal-core/src/server/tables/ephemeral.rs`

```rust
pub type EphemeralSortedTable<R, D, N> = PersistentSortedTable<R, NoStorage<D>, N>;
pub type EphemeralUnsortedTable<R, D, N> = PersistentUnsortedTable<R, NoStorage<D>, N>;
```

Aliases, not implementations. A table is generic over its storage engine, so an in-memory table
is the same table with `NoStorage` underneath it instead of `FileSystem`
(`shoal-core/src/server/tables/storage/none.rs`). A schema writes one generic and the `#[db]`
macro fills the other two in:

```rust
#[shoal::db]
pub struct MyDb {
    pub cache: EphemeralUnsortedTable<Session>,
}
```

Everything above the engine — routing, partitions, filters, sort key selections, ranges,
projections, SHQL — is shared with the persistent tables and cannot drift from them.

**What is removed:** the intent log write, the durability barrier, compaction, archive reads,
and eviction. **What is not:** an insert is still wrapped in an intent, still parked in
`PendingResponse`, and still released on a shard sweep rather than answered inline; partitions
are still held behind a `MaybeLoaded` that can only ever be the loaded arm. See
[F9](../features/ephemeral-tables.md) for why, and for the numbers the two halves are worth.

Nothing an ephemeral table holds is ever evicted. A partition can only be evicted after being
marked evictable, and the only two things that send a `MarkEvictable` are the filesystem
compactor and the partition load path — neither of which `NoStorage` reaches. That is a safety
property (an evicted ephemeral partition would be gone rather than re-readable) and a cost:
memory pressure cannot reclaim ephemeral data.

## Shared surface

Both persistent tables implement the same informal interface, called by generated code:

| Method | Called from | Purpose |
| --- | --- | --- |
| `new` | `ShoalDatabase::new` | Construct, replay intents, force a compaction |
| `handle` | `ShoalDatabase::handle` | Execute one query |
| `load_partition` | `ShoalDatabase::load_partition` | Install a faulted-in partition, return unblocked queries |
| `flush` | `ShoalDatabase::flush` | Push staged intent bytes toward disk |
| `compaction_due` | `ShoalDatabase::compaction_due` | Answer whether this log has grown past its rotation size — synchronous, so the shard can ask per message |
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
of one background compactor task per table per shard. It is also what makes an ephemeral table
possible without a second table implementation: the engine is a generic, so a table that stores
nothing is the same table with a different one ([F9](../features/ephemeral-tables.md)).

**No trait for the table interface.** Generated code calls methods by name. This dodges the
generic-parameter explosion visible in the `where` clauses (`.../persistent/sorted.rs:118-149`
is a 30-line bound list) but removes the compiler's ability to enforce that the two
implementations agree.

## Limitations

- A range over a *prefix* of a composite sort key is not expressible, and a range never reduces
  the I/O of a cold partition ([F1](../features/sort-key-ranges.md#limitations)).
- Unsorted updates and deletes ignore data on disk.
- No trait unifies the table implementations, so behavioural divergence is silent.
- An ephemeral table still needs a storage directory. `ShoalPool::start` claims one before any
  shard is spawned (`shoal-core/src/server.rs:87`), whatever its tables are made of
  ([F9](../features/ephemeral-tables.md#limitations)).
- Partition keys are hashes; for unsorted tables a hash collision silently overwrites a row
  ([Partitioning](../architecture/partitioning.md#from-field-values-to-a-partition-key)).
