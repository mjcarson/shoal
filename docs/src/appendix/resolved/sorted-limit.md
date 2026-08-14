# 7. `limit` was ignored by persistent sorted tables

Filed as *`limit` is ignored by persistent sorted tables*, which was the reachable half. Making
the limit real inside one shard is a small change; making it *mean* anything is not, because a get
naming partitions on several shards was already being answered several times over, and a per-shard
limit would have multiplied by the number of shards. The two had to be fixed together.

## Symptom

A `LIMIT 10` against a million-row partition returned a million rows. The limit was parsed by
SHQL, type-checked, bound into the generated `*Get`, serialized, sent over the wire, and delivered
to the table, which discarded it.

Underneath that, a get naming partitions on several shards returned only whichever shard answered
first — the other shards' rows were dropped by the client, silently.

## Cause

### The table did not call the code that implemented the limit

`SortedPartition::get` (`.../tables/partitions.rs`) honoured `limit`. It was not the method that
ran. `PersistentSortedTable::get` inlined two scan loops of its own, one for a resident partition
and one for an archive being read in place, and neither mentioned `limit`:

```rust
for row in partition.live_row_values() {
    if let Some(filters) = &get.filters {
        if !R::is_filtered(filters, row) { continue; }
    }
    data.push(row.clone());
}
```

`grep limit` over the whole file had no hits. The duplication is why: the `Accessible` arm has to
filter the archived form and deserialize, so there was no shared method covering both arms — the
unsorted side had one (`MaybeLoaded<UnsortedPartition<R>>::get`), the sorted side did not.

### The limit that did exist was applied one row too late

`SortedPartition::get` pushed the row and *then* checked:

```rust
found.push(row.clone());
if let Some(limit) = params.limit {
    if found.len() >= limit { break; }
}
```

`found` is shared by every partition a get touches, so a scan handed an already-full vec still
appended a row before noticing. One extra row per partition past the one that filled the limit.

### Each shard was answering the whole query

`SortedQuery::find_shard` pushed one `ShardInfo` **per partition key with no dedup**, and
`send_to_shard` cloned the **full, un-narrowed** query to each of them. So a get naming keys on
three shards produced three responses carrying the same `(id, index)`, each having scanned keys it
did not own, and two keys on one shard made that shard execute the whole query twice.

Nothing merged them. `ShoalResultStream` advances `next_index += 1` per response and parks
out-of-order ones in a `BTreeMap` keyed by index, so a second response at an index it has already
passed is unreachable forever.

### The index a gather would key on was not stable

`index += queries.base_index` sat **inside** the fan-out loop, so the same query was tagged index
10 and then index 20 for a streamed bundle with `base_index = 10`. `end_index` was computed
without `base_index` at all, and `queries.queries.len() - 1` underflowed on an empty bundle.

## Evidence

Read from the source, then reproduced. Every test below was run against the unfixed tree first and
recorded failing, rather than written afterwards and assumed to cover it.

The overshoot, two partitions of three rows scanned into one vec with `limit: Some(3)`:

```
assertion `left == right` failed
  left: 4
 right: 3
```

The cross-shard loss, 20 partitions of three rows over a two-shard ring, no limit:

```
assertion `left == right` failed
  left: 21
 right: 60
```

21 is one shard's seven partitions. The other 39 rows were dropped by the client, which is the
data-loss half of this item and was not what it was filed for.

## The fix

**One limit-aware scan covers both ways a partition can be held.**
`MaybeLoaded<SortedPartition<R>>::get` (`.../tables/partitions.rs`) is the sorted twin of the
unsorted method that already existed. `PersistentSortedTable::get` calls it and no longer inlines
anything.

**The limit is counted against accumulated rows, before the push.**
`SortedGet::limit_reached` (`shared/queries/sorted.rs`) is the one predicate, asked about the rows
found so far rather than the rows any one scan produced:

```rust
pub fn limit_reached(&self, found: &[R]) -> bool {
    match self.limit {
        Some(limit) => found.len() >= limit,
        None => false,
    }
}
```

That is what makes it survive a get that parks on a disk read and resumes with its earlier rows
handed back out of `pending_data`.

**A full get stops scanning and stops reading.** The guard sits at the top of the partition loop
in `PersistentSortedTable::get`, so a `LIMIT 2` naming ten cold partitions issues at most the
reads it needs, and a `LIMIT 0` issues none:

```rust
if get.limit_reached(&data) {
    // stop waiting on a partition we are done with
    blocked.retain(|key| key != partition_key);
    // keep walking our keys so the rest come off our blocked list too
    continue;
}
```

The `continue` and the `retain` are load-bearing — see the invariants below.

**A query is narrowed to each shard rather than broadcast whole.**
`ShoalQuerySupport::split_by_shard` replaces `find_shard`. It groups the partition keys by owning
shard, deduplicates the shards, and emits one query per shard naming only that shard's keys. Keys
keep their original relative order within each group, so a narrowed get scans its partitions in
the order the whole get would have — which matters for a limit, since it takes the first rows it
finds.

**The shard that split a query collects the shares and answers once.**
`QueryMetadata.gather` carries the contact of the shard that split the query; `ServerMsg::Gathered`
carries a share back to it; `Shard.gathering` holds the partial answer keyed by `(id, index)`.
When the last outstanding share arrives, `handle_gathered` merges them, applies the limit to their
union, and replies to the client once. A query answered by one shard alone sets `gather: None` and
keeps the direct shard-to-socket reply, so the common path pays nothing.

Merging is `ResponseAction::merge` — a get is the union of the rows each shard found, an exists is
true if any shard found it, and nothing else can be split because every other query names a single
partition. `ResponseAction::truncate` trims the union, mapping an emptied get back to
`Get(None)` so the empty-result convention holds.

**The index is computed once per query.** `send_to_shard` hoists `index += queries.base_index` out
of the fan-out loop, compares `end` against an absolute `end_index`, and returns early on an empty
bundle. This is the whole of what was [item 10](../known-issues.md); it is not optional here,
because a gather keyed on `(id, index)` cannot work while one query carries two different indexes.

## Alternatives rejected

**A per-shard limit, with the over-return documented.** The smallest change that makes `LIMIT`
do something. Rejected because it makes the guarantee depend on how the keys happen to hash: the
same query returns 10 rows or 40 depending on the ring. A limit that is only sometimes a limit is
worse than one that is honestly ignored, because it looks fixed.

**Trimming in the client.** `ShoalResultStream` could cap what it hands back. But it keys pending
responses in a `BTreeMap` by index and advances `next_index` once per response, so it already
loses duplicate-index responses — trimming would have sat on top of a broken invariant instead of
fixing it, and the rows it trimmed away would have been an arbitrary shard's, not the first rows
of the query.

**Chaining the query shard to shard**, each one passing the residual limit and the accumulated
rows to the next, with the last replying to the client. It needs no new response path and gives an
exact limit, but latency becomes linear in the number of shards for a query whose whole point is
that the shards work in parallel.

**Returning a signal from the scan so the caller can `break`.** It only covers the case where
*this* scan filled the limit. A replayed get enters with `data` already full and must be caught
before the partition lookup and before `load_partition`. A guard at the top of the loop covers
both with one check, so the scan methods stay `-> ()`.

## Invariants to uphold

- **The limit is counted against accumulated rows, not against one scan.** `found` is shared by
  every partition a get touches and is handed back across disk-load replays. Checking a per-scan
  counter instead reintroduces the bug in a form that only shows up on multi-partition gets.
- **Check the limit before the push, not after.** Checking after appends one row per partition
  past the one that filled the limit, and appends a row at all to a vec that was already full.
- **A key skipped for being past the limit must still come off `blocked`.** A replayed get carries
  only the key that just loaded, while `blocked` still holds the others; its registration in
  `self.blocked` has already been consumed, so nothing will replay it again. `break`ing out of that
  loop instead of `continue`-with-`retain` leaves `blocked` non-empty forever and **the client
  never gets a response at all**.
- **A get with partitions still parked may not answer early, even when its limit is full.**
  Dropping `pending_data` would let the in-flight load's replay build a fresh accumulator and emit
  a *second* response for the same `(id, index)`. The cost is waiting on reads that are no longer
  needed; deregistering our own entries from `self.blocked` is the real fix and is not done here.
- **A gathered query replies exactly once, on the `outstanding == 0` transition.** The entry is
  removed before the reply. A share arriving after that is warned about and dropped, because there
  is nothing left to merge it into.
- **`ServerMsg::Gathered` is never broadcast.** Its `Clone` arm panics. It travels to exactly one
  shard, and `Comms::broadcast` clones — the same hazard the `Partition` variant lives under.
- **Every shard applies the limit to its own share before sending it.** The gather trims the union,
  but the per-shard limit is what bounds how much crosses a channel. Removing it would still be
  correct and would still be a regression.

## Still open

**A shard that dies mid-query leaks its gather entry** and the client waits forever. There are no
timeouts anywhere ([item 15](../known-issues.md#15-no-backpressure-anywhere)), so this is the
existing failure mode rather than a new one, but the gather adds an instance of it.

**`LIMIT 0` is indistinguishable from a miss.** It answers `Get(None)`, which `send_one` reports as
`QueryDidNotSucceed` — the same thing a get that found nothing reports. ~~Telling them apart needs
the error channel on `ResponseAction` that [TODOs](../todos.md) already calls for.~~ **The error
channel landed ([F11](../../features/error-channel.md)) and does not close this**, which is worth
saying because this page expected it to. `LIMIT 0` and an empty get are both queries that *worked*;
the error channel separates a query that worked from one that did not. What separates these two is
[item 55](../known-issues.md#55-a-get-that-found-nothing-is-reported-as-a-query-that-failed) —
`send_one` taking a `QuerySuceededOpts`.

**`SortedExists` still inlines its own dual-arm scans.** This change touched only the get path;
giving `exists` the same `MaybeLoaded` treatment is a clean follow-up. *Done* — it moved onto
`MaybeLoaded::exists` with [item 8](sort-keys.md).

## Tests

| Test | Fails without |
| --- | --- |
| `a_limit_is_shared_across_partitions` (`.../tables/partitions.rs`) | The check-before-push. Two partitions of three rows with `limit: Some(3)` returns 4 |
| `a_full_found_vec_is_left_alone` (`.../tables/partitions.rs`) | The same. A scan handed an already-full vec appends a row, which is the disk-replay shape |
| `a_zero_limit_finds_no_rows` (`.../tables/partitions.rs`) | The guard. A limit of zero scans the partition anyway |
| `a_limit_stops_a_partition_scan`, `a_get_with_no_limit_returns_every_row` (`.../tables/partitions.rs`) | Nothing — they pin the cases that were already right, so a future guard cannot short-circuit an unlimited get |
| `get_stops_at_its_limit` (`shoal/tests/persistent_sorted_table.rs`) | The table calling the limit-aware scan. Five rows come back for a `LIMIT 2` |
| `get_stops_at_its_limit_when_loaded_from_disk` (same) | The same, through the blocked-replay path and the `Accessible` archived scan — the only test that covers that arm |
| `get_stops_at_its_limit_under_eviction` (same) | The same, for a partition being dropped and read back under memory pressure |
| `get_spreads_its_limit_across_partitions` (same) | The limit spanning partitions rather than resetting. Six rows for a `LIMIT 4` over two partitions on one shard |
| `get_across_shards_returns_every_row` (same) | The gather. 21 of 60 rows, because the client keeps one response per index and drops the rest |
| `get_applies_its_limit_across_shards` (same) | The gather applying the limit globally. 21 rows for a `LIMIT 50` |
| `get_with_a_zero_limit_returns_nothing` (same, and `persistent_unsorted_table.rs`) | The zero-limit guard on each table kind |

## Related

- [Query Execution](../../tables/query-execution.md) — the get loop and the blocked-replay model
- [Partitions](../../tables/partitions.md) — `MaybeLoaded` and the scan methods
- [SHQL](../../api/shql.md) — where `LIMIT` is parsed and bound
- [Partitioning and the Tablet Map](../../architecture/partitioning.md) — how keys are routed to shards
- [Request Lifecycle](../../architecture/request-lifecycle.md) — the fan-out and the reply path
- [Memory accounting collapsed to zero on a partition load](memory-accounting.md) — the other defect on the load path
