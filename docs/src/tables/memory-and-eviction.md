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

`.../persistent/sorted.rs:392-394`

Read the borrow, compute, then write — rather than `*x.borrow_mut() += d` — to avoid holding
a mutable borrow across the computation. `saturating_add_signed` prevents underflow when a
partition shrinks. `adjust_memory_usage` (`.../tables/persistent.rs:130-135`) is that shape as a
function; a signed change must go through it or spell it out, never through a cast to `usize`
([Resolved #6](../appendix/resolved/memory-accounting.md)).

**Reading the counter is subject to the same rule as writing it.** `eviction_totals`
(`.../tables/persistent.rs:144-166`) is its counterpart for the eviction log, and saturates for
the same reason: the counter is an estimate, so arithmetic on it may not assume an ordering, and
arithmetic in a log statement may not be able to fail
([Resolved #13](../appendix/resolved/eviction-log-underflow.md)).

Sizes come from `deepsize2`, which walks heap allocations, so a row's `String` and `Vec`
contents are counted ([Partitions](partitions.md#sizes)).

The counter is an estimate and drifts, for reasons documented in
[Partitions](partitions.md#sizes) and [Known Issues](../appendix/known-issues.md#22-size-accounting-inconsistencies):
sorted tombstones are uncounted, `UnsortedPartition` uses two different size bases, and sorted
partitions maintain their size by delta. An unsorted tombstone does account for its 17 bytes,
since it replaces the whole partition rather than one entry inside it. The sorted undercount is
at least bounded now: tombstones are swept when their partition is marked evictable, so they
accumulate for one generation rather than forever.

One path used to corrupt it outright rather than drift: `load_partition` sized a merged
partition from the archive extent it merged in, so a partition that had just grown reported that
it shrank, and the shrink was applied with a cast that floored the counter at **0**. Both halves
are fixed — the merge recomputes its size and the adjustment is signed
([Resolved #6](../appendix/resolved/memory-accounting.md)). The merge is now the one place a
sorted partition's size is recomputed instead of maintained by delta.

What remains is drift, not collapse. Note that the two arms of `load_partition` still count
different things: a `Vacant` entry is accounted by the raw archive bytes it installs
(`.../persistent/sorted.rs:289-295`), while a merge is accounted by `deep_size_of` over the
rows.

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

`.../persistent/sorted.rs:396`, `:262-264`, `.../persistent/unsorted.rs:423`, `:450-452`

Note `promote` is called on unsorted reads (`.../persistent/unsorted.rs:450-452`, `:542-544`) but
**not** on sorted reads — `PersistentSortedTable::get` has no `promote` call anywhere in
`.../persistent/sorted.rs:408-563`. So for sorted tables the "recently used" ordering only
reflects when a partition became evictable, not when it was last read. Frequently read sorted
partitions are evicted as readily as cold ones.

## Becoming evictable

A partition may only be dropped once its changes are on disk. That is the generation check:

```rust
pub fn mark_evictable(&mut self, generation: u64, partitions: Vec<u64>) {
    let mut marked = 0;
    let mut swept = 0;
    // track how far our data has been compacted
    self.flushed_generation = self.flushed_generation.max(generation);
    for partition in partitions {
        if let Some(maybe_loaded) = self.partitions.get_mut(&partition) {
            if maybe_loaded.is_evictable(generation) {
                if let MaybeLoaded::Loaded { partition, .. } = maybe_loaded {
                    swept += partition.drop_tombstones();
                }
                let size = maybe_loaded.size();
                self.lru.borrow_mut().put((self.table_name, partition), size);
                marked += size;
            }
        }
    }
    event!(Level::INFO, marked, swept, flushed_generation = self.flushed_generation);
}
```

`.../persistent/sorted.rs:1049-1081`

Driven by `ServerMsg::MarkEvictable`, sent by the compactor after it writes and syncs a batch
of partitions ([Compaction](../storage/compaction.md#6-mark-evictable)), and by the
`load_partition` path once blocked queries have been queued.

**A read that failed sends none.** `fail_partition` releases the same queries `load_partition`
would have, but nothing was read: no partition entered `partitions` and none came out of the LRU
that has to be put back, and a read that never happened has no generation to advance
`flushed_generation` with ([Resolved #16, 51](../appendix/resolved/partition-load-failure.md)).

**Which generation is passed matters more than it looks.** The compactor's message carries the
generation of the log it has just sealed *and compacted*, so `gen <= flushed` genuinely means
"already in an archive". The load path has no such generation to hand — it is releasing queries
that are about to write — so both tables track the newest compacted generation separately and
pass that:

```rust
/// The newest generation whose intent log has been compacted into an archive
flushed_generation: u64,
```

`.../persistent/sorted.rs:100-107`, `.../persistent/unsorted.rs:94-101`

It is advanced only from a compactor message, and counters start at 1 so that 0 can mean
"nothing has been compacted yet". Passing the *open* generation here instead — which is what
the load path used to do — makes the check compare a generation against itself, marks a
partition evictable while its own delete is still in an open log, and resurrects the row it
deleted ([Resolved Issues #5](../appendix/resolved/resurrected-deletes.md)).

**Marking is also when sorted tombstones die.** A tombstone shadows a row that may still be in
an archive; once the partition's generation is compacted, the archive behind it no longer holds
those rows and there is nothing left to shadow. `drop_tombstones` sweeps them at exactly that
moment, which is the only point where dropping one is safe.

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
    let mut removed = 0;
    for victim in victims {
        if let Some(partition) = self.partitions.remove(&victim) {
            let size = partition.size();
            let decreased = self.memory_usage.borrow().saturating_sub(size);
            *self.memory_usage.borrow_mut() = decreased;
            removed += size;
        }
    }
    let post = *self.memory_usage.borrow();
    let (reclaimed, drift) = eviction_totals(pre, post, removed);
    event!(Level::INFO, pre, post, removed, reclaimed, drift, ...);
}
```

`.../persistent/sorted.rs:1059-1092`

`partitions.remove` drops the partition. Its data is already in an archive, so a later read
faults it back in ([Query Execution](query-execution.md#blocking-on-a-disk-read)).

**The event reports two independent numbers, not one.** `removed` is summed from the partitions
that were actually dropped; `reclaimed` is how far the shard counter moved; `drift` is the gap
between them, and is non-zero only when the counter had already drifted low enough to floor at 0.
Since this event is the only window onto the counter, a drifted counter used to make the window
report the drift as though it were the truth.

The subtractions live in `eviction_totals` (`.../tables/persistent.rs:144-166`) and both saturate.
That line used to read `diff = pre - post` — a plain `usize` subtraction inside a log statement,
which panics the shard on any counter state where `post > pre`, from the logging rather than the
logic ([Resolved #13](../appendix/resolved/eviction-log-underflow.md)). That state turned out not
to be reachable through this loop, since every mutation in it saturates; the fix is what keeps it
unreachable regardless of what the loop is changed into, and the drift reading is what it bought
along the way.

## The generation trap

The generation rule is correct, and it has a consequence worth stating plainly:
**a continuously written partition can never be evicted.**

A partition written in generation *N* becomes evictable only after generation *N* is
compacted. If it is written again before then, its generation advances and the clock restarts.
Under a sustained write workload where the working set is larger than the memory limit, the
LRU stays empty, `evict_data` finds nothing, and memory grows past the configured limit
unchecked.

Sorted tables used to appear to escape this, because their in-place mutations never refreshed
the partition's generation — the clock never restarted, so partitions became evictable on
schedule regardless of what was still in the log. That was not an escape, it was the bug behind
[Resolved Issues #5](../appendix/resolved/resurrected-deletes.md): acknowledged writes
disappeared from reads until their log was compacted. Now that sorted partitions are pinned as
correctly as unsorted ones, a sorted write-heavy workload feels this trap the same way.

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

- ~~Memory accounting collapses to zero on the partition-shrink path.~~ Fixed: a merge recomputes
  its size and the adjustment is signed ([Resolved #6](../appendix/resolved/memory-accounting.md)).
  What remains is drift, not collapse.
- Sorted reads never `promote`, so sorted LRU ordering does not reflect reads.
- Under sustained writes the limit is unenforceable.
- The limit is per shard but configured globally.
- Tombstones and drifting cached sizes make the counter approximate, though sorted tombstones
  no longer accumulate past the generation that compacts them.
- ~~`diff = pre - post` can panic.~~ Fixed: the subtractions saturate inside `eviction_totals`
  ([Resolved #13](../appendix/resolved/eviction-log-underflow.md)). The event now also reports
  `drift`, so a floored counter is visible rather than silently reported as the truth — but drift
  is only measured, not repaired.
- No metrics beyond two `INFO` events; no way to observe resident bytes, LRU depth, or
  eviction rate other than by reading logs.
- **An ephemeral table is outside all of this.** Its partitions are never marked evictable, so they
  never enter the LRU and no eviction pass can choose them, and the memory they hold is not
  counted toward the limit. That is a safety property — there is no disk to re-read an evicted
  ephemeral partition from — and it means `resources.memory` does not bound an ephemeral table at
  all ([F9](../features/ephemeral-tables.md#limitations)).
- **Eviction is not what makes a wide row slow**, which is worth saying because this is the first
  page people look at when throughput falls as rows grow. The row-size sweep's widest arms hold
  256 MiB against a `4Gi` limit and never evict anything, and the memory sweep is flat across six
  rungs for the same reason. The cost of a wide row is in the copies and in the intent log's
  staging buffer, not here — see [Row size and what it costs](row-size.md).
