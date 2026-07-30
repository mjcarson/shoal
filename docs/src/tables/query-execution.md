# Query Execution

A query reaching its owning shard has three possible outcomes: answer now, wait for
durability, or wait for a disk read. All three are expressed as one return type:

```rust
async fn handle(&mut self, meta: QueryMetadata, query: SortedQuery<R>)
    -> Option<(Uuid, Uuid, Response<R>)>
```

`shoal-core/src/server/tables/persistent/sorted.rs:302-320`

`Some` answers immediately. `None` means "later", and the two kinds of "later" are tracked in
different places.

```
                        table.handle(meta, query)
                                  │
             ┌────────────────────┼────────────────────┐
             ▼                    ▼                    ▼
     Some(response)      None + pending.add()   None + blocked.entry()
       read hit           write, awaiting        needs a disk read
             │            durability                   │
             │                    │                    │
             ▼                    ▼                    ▼
        reply now       ServerMsg::DataFlushed   ServerMsg::Partition
                          → handle_flushed         → load_partition
                          → get_flushed            → replay query
                          → reply                  → (may now answer)
```

## Reads

`PersistentSortedTable::get` (`.../persistent/sorted.rs:387-525`) walks the requested
partition keys, accumulating rows:

```rust
let (mut data, mut blocked) = match self.pending_data.remove(&(meta.id, meta.index)) {
    Some((data, blocked)) => (data, blocked),
    None => (Vec::with_capacity(get.partition_keys.len()), Vec::default()),
};
for partition_key in &get.partition_keys {
    match self.partitions.get(partition_key) {
        Some(MaybeLoaded::Loaded { partition, .. }) => { /* maybe load; else scan */ }
        Some(MaybeLoaded::Accessible(read)) => { /* scan the archive in place */ }
        None => { /* maybe load from disk */ }
    }
}
if !blocked.is_empty() {
    self.pending_data.insert((meta.id, meta.index), (data, blocked));
    None
} else {
    /* build ResponseAction::Get and answer */
}
```

The first line is the key to the whole design: **a get can execute several times.** Each run
picks up whatever the previous run accumulated, resolves what it can, and re-parks if
partitions are still missing. It terminates when `blocked` is empty.

`pending_data` is keyed by `(query id, index)` — the pair that uniquely identifies one query
within one bundle.

An unsorted get is simpler: one partition key, so there is nothing partial to accumulate
(`.../persistent/unsorted.rs:374-437`).

### What a get actually filters on

```rust
for row in partition.live_row_values() {
    if let Some(filters) = &get.filters {
        if !R::is_filtered(filters, row) { continue; }
    }
    data.push(row.clone());
}
```

`.../persistent/sorted.rs:436-448`

A full scan of every live row in the partition, filtered by the generated filter predicate.
Two things are conspicuously absent:

- **`get.sort_keys` is never read.** The field exists on `SortedGet`
  (`shared/queries/sorted.rs:74-83`) and is carried through `to_blocked`, but no code path
  consults it. There is no point lookup by sort key and no range scan.
- **`get.limit` is never applied.** `SortedPartition::get` does honour it
  (`.../tables/partitions.rs:278-284`), but that method is not what runs here — this loop is
  inlined in the table and has no limit check. So SHQL's `LIMIT` parses, type-checks, travels
  the wire, and is discarded.

See [Known Issues](../appendix/known-issues.md#7-limit-is-ignored-by-persistent-sorted-tables)
and [#8](../appendix/known-issues.md#8-sort-keys-are-accepted-and-ignored).

Rows are `clone()`d into the response. For an `Accessible` partition they are deserialized
instead, but only after passing the filter
([Partitions](partitions.md#maybeloaded)).

## Blocking on a disk read

```rust
let will_load = self.storage
    .load_partition(self.table_name, *partition_key, &self.loader_tx)
    .await
    .unwrap();
if will_load {
    let entry = self.blocked.entry(*partition_key).or_default();
    let blocked_get = get.to_blocked(*partition_key);
    entry.push((meta.clone(), SortedQuery::Get(blocked_get)));
    blocked.push(*partition_key);
    continue;
}
```

`.../persistent/sorted.rs:410-434`

`load_partition` returns whether a read was started
([Storage Overview](../storage/overview.md#the-archive-map)) — `false` means the archive map
has no entry, so the partition does not exist and the loop moves on with no IO.

`to_blocked` narrows the query to the single partition being waited on
(`shared/queries/sorted.rs:91-98`), so when it resumes it does not redo work already
accumulated in `pending_data`.

Note the query is parked under `blocked[partition_key]`, keyed by partition rather than by
query. Several queries waiting on the same partition share one entry and are all released by
one read.

### Resumption

When the loader finishes, `ServerMsg::Partition` reaches the shard and the generated dispatch
calls `load_partition` on the table (`shoal-derive/src/traits/db.rs:144-176`):

```rust
if let Some((unblocked, generation)) = self.#field_ident.load_partition(loaded_kinds.loaded).await {
    let mark_evict_msg = ServerMsg::MarkEvictable { generation, table, partitions: vec![id] };
    for (meta, unwrapped) in unblocked {
        let query = #query_ident::#variant_ident(unwrapped);
        shard_local_tx.send(ServerMsg::Query { meta, query }).await.unwrap();
    }
    shard_local_tx.send(mark_evict_msg).await.unwrap();
}
```

Unblocked queries are re-injected as ordinary `ServerMsg::Query` messages — they take the
normal path again, and this time the partition is resident.

The `MarkEvictable` is deliberately sent *after* the replayed queries:

> build a mark evictable message for this partition so we don't mark this as evictable until
> we have completed all blocked queries to prevent load/reloading the same partition over and
> over again

`shoal-derive/src/traits/db.rs:158-160`

Since the shard processes its queue in order, the partition cannot be evicted out from under
the very queries that faulted it in. The table's `load_partition` also pops it from the LRU
on arrival (`.../persistent/sorted.rs:267-270`) for the same reason.

### Merging the loaded partition

```rust
hash_map::Entry::Occupied(mut entry) => {
    if let MaybeLoaded::Loaded { partition, .. } = entry.get_mut() {
        let old_size = partition.size();
        let accessed = SortedPartition::<R>::access(&loaded.data).unwrap();
        let mut new = SortedPartition::<R>::deserialize(&accessed).unwrap();
        std::mem::swap(&mut new, partition);
        partition.rows.extend(new.rows.into_iter());
        partition.check_disk = false;
        ...
    }
}
hash_map::Entry::Vacant(entry) => {
    entry.insert(MaybeLoaded::Accessible(loaded.data));
    ...
}
```

`.../persistent/sorted.rs:238-288`

If nothing is in memory, the raw bytes are installed as `Accessible` — no deserialization. If
something *is* in memory, the disk copy becomes the base and the in-memory rows (including
tombstones) are extended over it, so memory wins on conflict
([Partitions](partitions.md#tombstones)).

Note the `if let` covers only the `Loaded` case. An `Occupied` entry holding `Accessible` is
left untouched and the freshly read data is dropped — correct, since both are copies of the
same archive extent.

The memory-usage adjustment in the shrinking branch is wrong:

```rust
let new_mem_usage = self.memory_usage.borrow().saturating_sub(diff as usize);
```

`.../persistent/sorted.rs:262-263`

`diff` is a negative `isize` here, so `diff as usize` is an enormous number and the saturating
subtraction floors shard memory usage at 0. See
[Known Issues](../appendix/known-issues.md#6-negative-isize-cast-collapses-memory-accounting).

The unsorted variant does not merge at all — it overwrites `Accessible` with `Accessible` and
leaves `Loaded` alone (`.../persistent/unsorted.rs:229-262`), which is right for a
one-row-per-partition table where memory is always complete.

## Writes

```rust
let intent = SortedIntents::Insert(row);
let pos = self.storage.commit(&intent).await.unwrap();
let row = match intent {
    SortedIntents::Insert(row) => row,
    _ => unsafe { std::hint::unreachable_unchecked() },
};
/* apply to the in-memory partition */
self.pending.add(meta, pos, action);
self.lru.borrow_mut().pop(&(self.table_name, key));
None
```

`.../persistent/sorted.rs:329-378`

Order: log first, then memory, then park the response. Popping from the LRU marks the
partition non-evictable — it now holds changes not yet compacted.

`PendingResponse` is a FIFO of `(pos, meta, action)`:

```rust
pub fn get(&mut self, flushed_pos: u64, flushed: &mut Vec<(Uuid, Uuid, Span, Response<T>)>) {
    while !self.pending.is_empty() {
        let is_flushed = match self.pending.front() {
            Some((pending_pos, _, _)) => flushed_pos >= *pending_pos,
            None => break,
        };
        if is_flushed { /* pop and emit */ } else { break; }
    }
}
```

`.../server/tables/storage.rs:72-100`

A `VecDeque` popped from the front works because positions are monotonically increasing, so
the first unflushed entry ends the scan.

The watermark it is compared against is `synced_pos` — the offset below which everything has
been fdatasynced — so popping an entry means the record really is on disk. At an intent log
rotation the queue is drained wholesale instead, since positions restart at 0 in the new file
and the old ones are all durable by then. See
[Durability model](../storage/overview.md#durability-model).

### Updates and deletes

Sorted updates and deletes follow the same pattern with a twist: they must first establish
that the row exists. All three states are handled — resident and found, resident but
`check_disk` set, and not resident — and the middle case parks the query for a disk read
(`.../persistent/sorted.rs:667-813` for delete, `:826-963` for update).

Unsorted updates and deletes do not do this. They consult memory only and report `false` if
the partition is not resident ([Table Types](table-types.md#the-asymmetry-that-matters)).

## exists

`exists` short-circuits on the first matching row rather than collecting
(`.../persistent/sorted.rs:534-653`). It reuses `pending_data` for its blocked set but stores
an empty `Vec` in the data slot, since there is nothing to accumulate.

This function still contains six debug `println!`s
(`.../persistent/sorted.rs:546`, `:553`, `:593`, `:594`, `:613`, `:639`), one of which
pretty-prints the entire partition on every hit. On a hot path, in a database. See
[Known Issues](../appendix/known-issues.md#17-leftover-debug-printlns).

## Design notes

**Re-execution instead of continuations.** A blocked query is stored as a *query*, not as a
suspended future. Resuming means running `handle` again with accumulated state from
`pending_data`. This keeps the table free of self-referential async state, keeps everything on
one `ServerMsg` queue, and makes the blocked path debuggable — you can print a blocked query.
The cost is that partial results must be threaded through an explicit map, and that a
multi-partition get faulting in five partitions runs `get` six times.

**Blocking keyed by partition, not by query.** Many queries waiting on one partition share one
read.

**Ordering the resumption.** Replayed queries are queued before the `MarkEvictable` that would
make the partition a candidate again, using the shard queue's FIFO ordering as the
synchronisation mechanism. Simple, and dependent on nothing reordering that queue.

## Limitations

- `limit` is ignored; `sort_keys` is ignored.
- Gets scan every live row in a partition — no index within a partition.
- Rows are cloned into responses; no zero-copy read path server-side.
- No timeout on blocked queries. If a `ServerMsg::Partition` never arrives — a loader error,
  for instance, which hits a `todo!()` (`.../fs/loader.rs:128`) — the query is parked
  forever, with no way for the client to learn that.
- `pending_data` and `blocked` are unbounded.
- Memory accounting corrupts on the partition-shrinks path.
- Debug `println!`s in `exists`.
