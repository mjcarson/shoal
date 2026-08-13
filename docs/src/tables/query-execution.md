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

`PersistentSortedTable::get` (`.../persistent/sorted.rs`) walks the requested partition keys,
filling a slot per partition:

```rust
let mut pending = self
    .pending_data
    .resume::<P>(&(meta.id, meta.index), &get.partition_keys, get.limit);
for partition_key in &get.partition_keys {
    let Some(rank) = pending.rank(*partition_key) else { continue };
    if pending.filled_before(rank) { pending.fill(rank, Vec::new()); continue }
    /* maybe read from disk, otherwise scan and fill this slot */
}
if pending.is_pending() {
    self.pending_data.park((meta.id, meta.index), pending);
    None
} else {
    /* flatten the slots in order, truncate, and answer */
}
```

The first line is the key to the whole design: **a get can execute several times.** Each run
picks up whatever the previous run accumulated, resolves what it can, and re-parks if partitions
are still missing. It terminates when every slot is filled.

`pending_data` is keyed by `(query id, index)` — the pair that uniquely identifies one query
within one bundle. What it holds is type-erased, because a parked get can be waiting for whole rows
or for any of its table's projections. `resume::<P>` downcasts back to the type the query named, and
panics rather than silently starting fresh, which would discard the rows already found
([F2](../features/projections.md#invariants-to-uphold)). The box is only ever allocated on this
path: a get whose partitions are all resident finishes in one execution and never parks.

### Slots, not an accumulator

Rows are not appended to one shared vec. `PendingGet` (`.../tables/persistent.rs`) gives each
partition the get named its own slot, in the order the query named them:

```rust
struct PendingGet<P> {
    keys: Vec<u64>,
    slots: Vec<Option<Vec<P>>>,
    limit: Option<usize>,
}
```

`P` is what this get asked to be answered with, which is the row type unless it named a projection.

That is what makes the answer's order a function of the query. A partition read back from disk is
replayed long after the ones already resident, and with a shared accumulator its rows landed
wherever the read happened to finish. With slots the replay fills the place the query asked for.
`None` doubles as "not read yet", which is what `is_pending` gates the response on — so the
blocked list the old code kept alongside the rows is no longer a separate thing to keep in sync.

Partition keys are deduplicated by `group_by_shard` before a get reaches a table
(`shared/queries.rs`), so a key names at most one slot and `rank` is an unambiguous lookup.

An unsorted get uses the same structure. It used to name one partition and so had nothing partial
to accumulate; it now names as many as a sorted one
([26, 39](../appendix/resolved/partition-order.md)).

### What a get actually filters on

```rust
partition.get(get, &mut data);
```

The scan itself lives on the partition, in `MaybeLoaded<SortedPartition<R>>::get`
(`.../tables/partitions.rs`), which covers both a resident partition and an archive being read
in place. A get selects its rows one of three ways, and `SortedGet::sort_select` says which:

| `SortSelect` | How the rows are visited |
| --- | --- |
| `All` | walk every live row of the partition |
| `Keys([..])` | seek each named key in the tree |
| `Range(..)` | seek the lower bound, walk in sort order until the upper one |

All three then run the same loop — `collect_rows`, which every arm shares so that the filtering,
the limit check, and the push exist once rather than once per arm:

```rust
for row in rows {
    if params.limit_reached(found) { break; }
    if let Some(filter) = &params.filters {
        if !T::is_filtered(filter, row) { continue; }
    }
    found.push(P::from_row(row));
}
```

`P` is what this get asked to be answered with. A get that named no projection asks for the whole
row, whose `from_row` is a clone, so this is exactly what the loop did before
[F2](../features/projections.md) — the scan is monomorphised per projection, so an unprojected get
has no branch here at all.

`found` here is this partition's own slot, so `limit_reached` caps each partition at `limit` rows
of its own. A partition can never contribute more than that to the first `limit` rows of the
whole answer, so nothing correct is lost and the scan still stops early. Checking before the push
rather than after is what makes `LIMIT 0` read nothing at all.

The limit spanning the whole get is applied when the slots are flattened, and deciding whether a
partition is worth reading at all is `PendingGet::filled_before`:

```rust
if pending.filled_before(rank) {
    pending.fill(rank, Vec::new());
    continue;
}
```

It is true only when every partition named *before* this one has been read and they already hold
the whole limit. The "before" is the part that matters. The old code compared the limit against
whatever rows it happened to hold, so a get whose *first* partition was on disk filled up from
its second and then dropped the first — answering out of the partition that was quicker rather
than the one it was asked for. An unread partition earlier in the query can still supply rows
that come first, so nothing may be skipped past it.

**`get.sort_select` narrows which rows are read, and nothing else.** A named key is sought in the
`BTreeMap` — or, for a partition being read in place, in the archived one — so a partition of *n*
rows asked for *k* of them costs `k log n` comparisons rather than *n* visits. A range seeks to its
lower bound and stops past its upper one, in both forms, so reading a page costs `log n` plus the
page. What neither may do is change which *partitions* are read: a key or a bound that matches
nothing in the copy in memory says nothing about the copy in an archive, so a partition marked
`check_disk` is read before it is answered about however narrow the get is.

Keys arrive sorted and deduplicated, which `SortSelect::normalized` does inside
`SortedQuery::split_by_shard` once as the query enters the server, so seeking them in order
produces sort order. A range needs no normalizing — it is already an ordered pair — but it *is*
checked for emptiness before either scan seeks with it, because `BTreeMap::range` panics on a range
whose start is past its end.

`SortSelect::All` is the only arm that means every row. An empty `Keys` list selects nothing, which
is a change from the bare `sort_keys: Vec<Sort>` this replaced. See
[item 8](../appendix/resolved/sort-keys.md) and [F1](../features/sort-key-ranges.md).

Rows are `clone()`d into the response. For an `Accessible` partition they are deserialized
instead, but only after passing the filter
([Partitions](partitions.md#maybeloaded)). Both of those are the *identity* projection; a get that
named a projection copies only the fields that projection declared, which for an archived partition
means the rest of each row is never deserialized at all
([F2](../features/projections.md#performance)).

### A limit across shards

A get naming partitions on several shards is split across them, and each shard applies the limit
to its own share as it scans. Their union can still be over the limit, so the shard that split
the query merges the shares, puts their rows back into the order the query named its partitions
in, and trims the union before replying — see
[Request Lifecycle](../architecture/request-lifecycle.md). The limit a client sees is therefore
global, not per shard.

Keeping each shard's own first `limit` rows is enough to be sure the globally first `limit` are
among them: a row in the global first `limit` has at most `limit - 1` rows before it anywhere,
and so at most `limit - 1` on its own shard.

### The order rows come back in

A get answers with its partitions in the order the query named them, and with each partition's
rows in sort-key order. Nothing is interleaved across partitions — this is a defined order, not
an `ORDER BY`. Both halves are needed for it to hold: the slots above give the shard-local order,
and the coordinator's reorder gives the cross-shard one. See
[26, 39](../appendix/resolved/partition-order.md).

## Blocking on a disk read

```rust
// build a query for just this blocked partition
let blocked_get = SortedQuery::Get(get.to_blocked(*partition_key));
// park this get if this partition has to be read from disk first
if self.block_on_load(*partition_key, &meta, blocked_get).await {
    // leave this slot empty for the replay to fill
    continue;
}
```

`.../persistent/sorted.rs`, the get path

`block_on_load` answers whether this query was parked. It returns `false` — meaning the caller
should answer now — in two cases, and they are different in kind:

- the archive map has no entry for the key, so the partition does not exist and there is no IO
  to do ([Storage Overview](../storage/overview.md#the-archive-map));
- this query is a replay released by a read that *failed*, and is carrying `meta.skip_disk` for
  this partition. Asking for that read again would park it on the same failure without end
  ([Resolved #16, 51](../appendix/resolved/partition-load-failure.md)).

`to_blocked` narrows the query to the single partition being waited on
(`shared/queries/sorted.rs:91-98`), so when it resumes it does not redo work already
accumulated in `pending_data`.

Note the query is parked under `blocked[partition_key]`, keyed by partition rather than by
query. Several queries waiting on the same partition share one entry and are all released by
one read — and, because a non-empty entry means a read is already in flight, they no longer
each ask the loader for it.

**Both table kinds express this as one `block_on_load`.** The sorted table used to inline it at
each of its blocking sites, on the reasoning that the surrounding match already held a borrow of
`self.partitions`; six copies of a rule is how half of it gets updated, which is what a single
`skip_disk` check spread over six sites would have been.

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
on arrival (`.../persistent/sorted.rs:279-281`) for the same reason.

The `generation` in that message is the table's newest **compacted** generation, not the one it
is currently writing in. Passing the open generation instead lets a query that has just been
released mark its own uncompacted write as evictable
([Resolved Issues #5](../appendix/resolved/resurrected-deletes.md)).

### Merging the loaded partition

```rust
hash_map::Entry::Occupied(mut entry) => {
    if let MaybeLoaded::Loaded { partition, .. } = entry.get_mut() {
        let old_size = partition.size();
        let accessed = SortedPartition::<R>::access(&loaded.data).unwrap();
        let new = SortedPartition::<R>::deserialize(&accessed).unwrap();
        partition.merge_from_disk(new);
        ...
    }
}
hash_map::Entry::Vacant(entry) => {
    entry.insert(MaybeLoaded::Accessible(ValidatedArchive::new(loaded.data)?));
    ...
}
```

`.../persistent/sorted.rs`

**The `Vacant` arm is where an archive is validated**, once, and it is the only place it happens
now ([F4](../features/validated-archives.md)). That is also why `load_partition` returns a
`Result`: a corrupt archive fails the read that produced it rather than the first query to touch
it. The `Occupied` arm above still uses the checked `access`, because those bytes are never
wrapped — that call is the only validation they get.

If nothing is in memory, the raw bytes are installed as `Accessible` — no deserialization. If
something *is* in memory, the disk copy becomes the base and the in-memory rows (including
tombstones) are extended over it, so memory wins on conflict
([Partitions](partitions.md#tombstones)).

Note the `if let` covers only the `Loaded` case. An `Occupied` entry holding `Accessible` is
left untouched and the freshly read data is dropped — correct, since both are copies of the
same archive extent.

The merge recomputes the partition's size from the rows it ended up holding, and shard memory
usage is moved by that signed difference:

```rust
let diff = partition.size().cast_signed() - old_size.cast_signed();
adjust_memory_usage(&self.memory_usage, diff);
```

`.../persistent/sorted.rs:269-273`

The diff cannot be negative — in-memory rows win every collision in the `extend`, so the merged
partition is a superset of what was resident. It is applied signed anyway because the sizes
being subtracted are maintained by delta elsewhere and can drift. Both of those used to be
wrong, and between them they floored shard memory usage at 0
([Resolved #6](../appendix/resolved/memory-accounting.md)).

The unsorted variant does not merge at all — it overwrites `Accessible` with `Accessible` and
leaves `Loaded` alone (`.../persistent/unsorted.rs:229-262`), which is right for a
one-row-per-partition table where memory is always complete. Leaving `Loaded` alone is also
what keeps a tombstone from being clobbered by a read that was already in flight when the
delete landed.

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

Unsorted updates and deletes do the same through `block_on_load`, with only two states to
handle rather than three: an unsorted partition is either resident in full or not resident at
all ([Table Types](table-types.md#both-types-consult-disk-on-every-operation)).

An unsorted delete does not remove the key. It replaces the partition with
`UnsortedPartition::tombstone(key)`, because the pre-delete copy may still be sitting in an
archive and dropping the key outright would let the next read fault it back in. The tombstone
costs 17 bytes, answers `Get(None)` / `Exists(false)` / `Delete(false)` / `Update(false)`
without touching disk, and is evicted once compaction has pruned the archive entry it shadows
([Partitions](partitions.md#tombstones)).

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

- A range bounds which rows are returned but not what is read: a cold partition is read whole
  either way, since there is no index within a partition on disk. The win is CPU and rows on the
  wire ([F1](../features/sort-key-ranges.md#limitations)).
- A range applies to the whole `Sort` value. A range over a *prefix* of a composite sort key is
  not expressible.
- A get selecting every row scans every live row in a partition — there is no index within a
  partition on anything but the sort key, so a filter on any other field is a scan. A `limit`
  bounds how much of that scan runs, but only because it stops early.
- Rows are grouped by partition rather than merged by sort key, so a get spanning partitions is
  not globally sorted. Interleaving them would mean reading every named partition even under a
  small limit.
- A gather entry for a query split across shards is only released when every shard has reported.
  A shard that dies mid-query leaks it and the client waits forever, since there are no timeouts.
- Rows are cloned into responses; no zero-copy read path server-side. A projection narrows what is
  cloned or deserialized to the fields it names, but it is still a copy
  ([F2](../features/projections.md#performance)).
- A projection changes what is deserialized, not what is read: a cold partition is read whole
  either way, the same caveat a range carries
  ([F2](../features/projections.md#limitations)).
- No timeout on blocked queries. A read that *fails* now releases the queries parked on it
  ([Resolved #16, 51](../appendix/resolved/partition-load-failure.md)), so the loader is no
  longer a way to reach this. A read that neither completes nor fails still parks them forever,
  with no way for the client to learn that.
- A read that failed is answered exactly as an empty partition is. The server logs the failure at
  `ERROR`; the response has no variant that can carry one
  ([item 56](../appendix/known-issues.md#56-a-response-cannot-say-that-a-read-failed)).
- `pending_data` and `blocked` are unbounded.
- Memory accounting corrupts on the partition-shrinks path.
