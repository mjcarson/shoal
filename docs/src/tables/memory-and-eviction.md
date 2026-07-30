# Memory and Eviction

Shoal uses direct IO, so the kernel page cache holds nothing. Every cached byte is a byte
Shoal itself is holding, and the only thing stopping a shard from consuming all of RAM is its
own eviction loop.

## Accounting

One counter per shard, shared with all of its tables:

```rust
memory_usage: Arc<RefCell<usize>>,
```

`shoal-core/src/server/shard.rs:260`

`Arc<RefCell<_>>` because everything sharing it lives on one thread
([Thread per Core](../architecture/thread-per-core.md#what-a-shard-owns)).

Every operation that changes resident data adjusts it. The typical shape:

```rust
let new_size = self.memory_usage.borrow().saturating_add_signed(size_diff);
*self.memory_usage.borrow_mut() = new_size;
```

`.../persistent/sorted.rs:371-373`

Read the borrow, compute, then write — rather than `*x.borrow_mut() += d` — to avoid holding
a mutable borrow across the computation. `saturating_add_signed` prevents underflow when a
partition shrinks.

Sizes come from `deepsize2`, which walks heap allocations, so a row's `String` and `Vec`
contents are counted ([Partitions](partitions.md#sizes)).

The counter is an estimate and drifts, for reasons documented in
[Partitions](partitions.md#sizes) and [Known Issues](../appendix/known-issues.md#22-size-accounting-inconsistencies):
tombstones are uncounted, `UnsortedPartition` uses two different size bases, and sorted
partitions maintain their size by delta.

One path corrupts it outright. In `load_partition`, when a merged partition ends up smaller:

```rust
let new_mem_usage = self.memory_usage.borrow().saturating_sub(diff as usize);
```

`.../persistent/sorted.rs:262-263`

`diff` is negative here, so `diff as usize` is astronomically large and the result saturates
to **0**. The shard then believes it is using no memory and stops evicting until the counter
climbs back above the limit. See
[Known Issues](../appendix/known-issues.md#6-negative-isize-cast-collapses-memory-accounting).

## The LRU

```rust
lru: Arc<RefCell<LruCache<(D::TableNames, u64), usize, BuildHasherDefault<GxHasher>>>>,
```

`shoal-core/src/server/shard.rs:262`

Keyed by `(table, partition key)`, valued by size, built `unbounded_with_hasher`
(`shard.rs:314`) — capacity is not the eviction trigger; the memory counter is.

**The LRU holds candidates, not contents.** A partition is in it only if it is currently
evictable. Membership changes on three events:

| Event | Effect |
| --- | --- |
| Insert / update / delete | `lru.pop(...)` — no longer evictable, it has uncompacted changes |
| Partition faulted in from disk | `lru.pop(...)` — protect it until blocked queries finish |
| Get / exists hit | `lru.promote(...)` — mark recently used |
| Compaction reports it durable | `lru.put(...)` — now a candidate |

`.../persistent/sorted.rs:375`, `:268`, `.../persistent/unsorted.rs:388-390`, `:653-655`

Note `promote` is called on unsorted reads (`.../persistent/unsorted.rs:388`, `:482`) but
**not** on sorted reads — `PersistentSortedTable::get` has no `promote` call anywhere in
`.../persistent/sorted.rs:387-525`. So for sorted tables the "recently used" ordering only
reflects when a partition became evictable, not when it was last read. Frequently read sorted
partitions are evicted as readily as cold ones.

## Becoming evictable

A partition may only be dropped once its changes are on disk. That is the generation check:

```rust
pub fn mark_evictable(&mut self, generation: u64, partitions: Vec<u64>) {
    let mut marked = 0;
    for partition in partitions {
        if let Some(maybe_loaded) = self.partitions.get(&partition) {
            if maybe_loaded.is_evictable(generation) {
                let size = maybe_loaded.size();
                self.lru.borrow_mut().put((self.table_name, partition), size);
                marked += size;
            }
        }
    }
    event!(Level::INFO, marked);
}
```

`.../persistent/sorted.rs:966-986`

Driven by `ServerMsg::MarkEvictable`, sent by the compactor after it writes and syncs a batch
of partitions ([Compaction](../storage/compaction.md#5-mark-evictable)), and by the
`load_partition` path once blocked queries have been queued.

```
   write ──▶ partition generation = current gen
                    │
                    │  (not evictable: gen > flushed gen)
                    ▼
   log rotation ──▶ compaction writes it to an archive
                    │
                    ▼
   MarkEvictable(gen) ──▶ is_evictable(gen)? ──▶ lru.put()
                                                     │
                    memory over limit ───────────────┤
                                                     ▼
                                              evict_data()
```

## Eviction

Checked once per shard loop iteration:

```rust
if *self.memory_usage.borrow() > self.conf.resources.memory {
    self.evict_data().await?;
}
```

`shoal-core/src/server/shard.rs:661-664`

Note this is a strict comparison against the configured limit — **not** the "60%" that
CLAUDE.md describes. The 40% figure in that document refers to how much is freed:

```rust
// we will always try to evict at least 40% of our cache when we hit memory pressure
let mut need = (*self.memory_usage.borrow() as f64 * 0.40).ceil() as usize;
let mut evictable = HashMap::with_capacity(10);
loop {
    match self.lru.borrow_mut().pop_lru() {
        Some(((table_name, key), size)) => {
            evictable.entry(table_name).or_insert_with(|| Vec::with_capacity(1000)).push(key);
            need = need.saturating_sub(size);
            if need == 0 { break; }
        }
        None => break,
    }
}
for (table_name, victims) in evictable {
    self.tables.evict(table_name, victims);
}
```

`shoal-core/src/server/shard.rs:554-589`

Evicting 40% rather than just enough to get under the limit avoids re-entering eviction on
every subsequent iteration — one bigger pass instead of a continuous trickle.

Victims are grouped by table before eviction so each table is called once with a batch.

If the LRU empties before 40% is found, the loop exits having freed whatever it could. The
shard then remains over its limit and will try again next iteration, every iteration.

Dropping is straightforward:

```rust
pub fn evict(&mut self, victims: Vec<u64>) {
    let pre = *self.memory_usage.borrow();
    for victim in victims {
        if let Some(partition) = self.partitions.remove(&victim) {
            let decreased = self.memory_usage.borrow().saturating_sub(partition.size());
            *self.memory_usage.borrow_mut() = decreased;
        }
    }
    let post = *self.memory_usage.borrow();
    event!(Level::INFO, pre, post, diff = pre - post, ...);
}
```

`.../persistent/sorted.rs:990-1014`

`partitions.remove` drops the partition. Its data is already in an archive, so a later read
faults it back in ([Query Execution](query-execution.md#blocking-on-a-disk-read)).

`diff = pre - post` is a plain subtraction on `usize`. Any accounting bug that leaves `post >
pre` panics here — in the logging statement, not the logic. See
[Known Issues](../appendix/known-issues.md#13-eviction-logging-can-underflow).

## The generation trap

The generation rule is correct, and it has a consequence worth stating plainly:
**a continuously written partition can never be evicted.**

A partition written in generation *N* becomes evictable only after generation *N* is
compacted. If it is written again before then, its generation advances and the clock restarts.
Under a sustained write workload where the working set is larger than the memory limit, the
LRU stays empty, `evict_data` finds nothing, and memory grows past the configured limit
unchecked.

The escape valve is log rotation, driven by `intent_log_size` (default 10 MiB per table per
shard). More frequent rotation means more frequent generations, hence more eviction
opportunities — at the cost of more compaction. That trade-off is not documented anywhere in
the config, and `intent_log_size` reads like a purely IO-related setting.

## Interaction with the memory setting

`resources.memory` is a **per-shard** limit compared against a **per-shard** counter
(`shard.rs:661`), but it is configured once, globally. With 16 shards and `memory: "4Gi"`, the
process ceiling is 64 GiB, not 4.

And if the `resources` block is omitted entirely, `memory` defaults to `0`
([Configuration](../getting-started/configuration.md#resources)), so eviction triggers on the
first byte and runs forever.

## Design notes

**The database manages its own memory.** Direct IO means there is no page cache to fall back
on, so Shoal must decide what stays resident. The upside is that eviction is aware of what a
partition costs and whether it is safe to drop; the downside is that everything above depends
on an approximate byte counter that several code paths get wrong.

**Durability gates eviction, not the other way round.** Rather than forcing a flush to make
room, Shoal only evicts what compaction has already persisted. That keeps eviction free of IO
and keeps the eviction path synchronous — at the cost of the generation trap.

**Evict in bulk.** Freeing 40% and batching victims per table amortises the work.

## Limitations

- Memory accounting collapses to zero on the partition-shrink path.
- Sorted reads never `promote`, so sorted LRU ordering does not reflect reads.
- Under sustained writes the limit is unenforceable.
- The limit is per shard but configured globally.
- Tombstones and drifting cached sizes make the counter approximate.
- `diff = pre - post` can panic.
- No metrics beyond two `INFO` events; no way to observe resident bytes, LRU depth, or
  eviction rate other than by reading logs.
