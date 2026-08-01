# 8. Sort keys were accepted and ignored

The last piece of a get that was accepted and thrown away. `limit` was the other one
([item 7](sorted-limit.md)), and the two failed the same way: parsed, type checked, bound into the
generated query, serialized, sent, delivered to the table, and dropped on the floor. A sorted
table that cannot be asked for a row is a table whose sort key only decides what order you are
handed everything in.

## Symptom

Against the TMDB example, whose `MovieByKeyword` is a partition per keyword sorted by title:

```sql
SELECT * FROM MovieByKeyword WHERE keyword = 'alien' AND title = 'Aliens'
```

returned every movie carrying the keyword. The sort key narrowed nothing. The same was true of
the typed API — `MovieByKeywordGet::new(keys).sort_keys(vec!["Aliens".into()])` returned the whole
partition — so there was no spelling, in any of the three front ends, that asked for a row.

`exists` was worse, because its answer is a single boolean. A `SortedExists` naming a sort key
answered "does this partition hold anything at all", so it was true for every row name a caller
could invent as long as the partition was not empty, and a caller could not tell the two apart
from the response.

## Cause

### The field was carried everywhere and read nowhere

`SortedGet::sort_keys` and `SortedExists::sort_keys` (`shared/queries/sorted.rs`) were populated by
the generated builders and by the SHQL binding arm (`shoal-derive/src/structs/client.rs`), copied
through `for_partitions` and `to_blocked`, and never consulted. `grep sort_keys` over the whole
server found the two struct definitions, the two copies, and nothing else.

`PersistentSortedTable::get` handed each partition to `MaybeLoaded<SortedPartition<R>>::get`, which
walked every live row and applied only the filters:

```rust
for row in self.live_row_values() {
    if params.limit_reached(found) { break; }
    if let Some(filter) = &params.filters {
        if !T::is_filtered(filter, row) { continue; }
    }
    found.push(row.clone());
}
```

That the rows are in a `BTreeMap<T::Sort, MaybeRow<T>>` — a structure whose whole purpose is to be
sought in — made no difference, because nothing sought.

### `exists` had its own two scans, and neither looked at a key

`PersistentSortedTable::exists` did not go through `MaybeLoaded` at all. It matched on the two arms
itself and inlined a walk into each, both of which returned `true` on the first live row that
survived the filters. The get path had been given a shared dual-arm scan by item 7; the exists path
was explicitly left behind by it, and this is the follow-up that page called for.

The two inlined scans had also picked up six `println!` debug lines, which went to a server's
stdout on every exists — one of them dumping a whole partition with `{:#?}`.

## Evidence

Read from the source, then reproduced. Every test below was run against the unfixed tree first and
recorded failing, rather than written afterwards and assumed to cover it.

A four-row partition asked for one row (`SortedPartition::get`, `.../tables/partitions.rs`):

```
assertion `left == right` failed
  left: ["a", "b", "c", "d"]
 right: ["c"]
```

The same thing end to end through a server, five rows in one partition
(`get_selects_a_named_sort_key`):

```
assertion `left == right` failed
  left: [("partition_key", "a"), ("partition_key", "b"), ("partition_key", "c"),
         ("partition_key", "d"), ("partition_key", "e")]
 right: [("partition_key", "c")]
```

And the exists half, asking a three-row partition about a row that was never written
(`exists_by_sort_key_is_false_for_a_missing_row`):

```
a row that was never written exists
```

## The fix

**A sort key names a row, and a get returns the rows it named.** `sort_keys` is a set — an `IN`
list — and not a range. An empty list keeps meaning "every row in this partition", which is what a
get that never mentions the sort key produces.

**Both scans seek instead of walking.** `SortedPartition::get` and the `Accessible` arm of
`MaybeLoaded<SortedPartition<R>>::get` (`.../tables/partitions.rs`) branch on whether any sort key
was named. When one was, each key is sought:

```rust
// a tombstone is a row that was deleted, so only a live row is returned
let Some(MaybeRow::Row(row)) = self.rows.get(sort_key) else {
    continue;
};
```

A partition of *n* rows asked for *k* of them costs `k log n` comparisons instead of *n* row
visits, and — for a partition being read in place — *k* deserializations instead of *n*.

**An archive is sought too, not walked.** `MaybeLoaded::seek_archived` puts the wanted key into the
form an archive's keys are in and asks `ArchivedBTreeMap::get` for it:

```rust
let raw = <R::Sort as RkyvSupport>::serialize(sort_key);
let wanted = <R::Sort as RkyvSupport>::access(&raw).unwrap();
match access.rows.get(wanted) { .. }
```

The bounds this needs — `Archived<Sort>: Ord` and its `CheckBytes` bound — were already on that
impl block, because the archived form of a partition was already being iterated there.

**`exists` shares the get path's shape.** `SortedPartition::exists` and
`MaybeLoaded<SortedPartition<R>>::exists` are the twins of the two gets, and
`PersistentSortedTable::exists` calls the second of them instead of matching on the arms itself.
What is left in the table is the part that is actually its own: deciding which partitions to read,
in the same `(resident, check_disk)` shape `get` uses. The six `println!`s went with the scans.

**Sort keys are normalized once, where a query enters the server.**
`normalize_sort_keys` (`shared/queries.rs`) sorts and deduplicates, and
`SortedQuery::split_by_shard` calls it in the `Get` and `Exists` arms — beside `group_by_shard`,
which deduplicates partition keys for the same reason. Sorting there is what makes a seek-in-order
produce sort order; deduplicating is what stops a key named twice from returning its row twice.

**A get naming sort keys still reads disk.** No part of this touches the `check_disk` block in
`PersistentSortedTable::get` or its counterpart in `exists`. That is deliberate and is the
invariant most likely to be optimized away by someone who has not read this page — see below.

## Alternatives rejected

**A membership filter over the existing walk.** Keep both scans as they are and skip rows whose key
was not named. It is the smaller diff and it is correct, and it was rejected because it fixes only
half of what the item was about: the get still visits every row, so "no point lookup by sort key"
would have stayed true in the sense that matters for a large partition. A `BTreeMap` that is only
ever iterated is paying for an ordering it never uses.

**Returning the rows in the order the query named its keys.** `IN ('c', 'a')` would answer `c`
then `a`. Rejected because it makes two spellings of the same set two different queries, and
because rows within a partition coming back in sort-key order is the invariant the cross-partition
ordering was built on ([item 26/39](partition-order.md)): the coordinator's reorder is a *stable*
sort precisely so that it does not disturb the order the shards produced.

**Skipping the disk read when every named key is already resolved in memory.** A row or a tombstone
in memory shadows whatever the archive holds for that key, so a get whose named keys are all
resolved in memory could answer without reading. It is sound, and it was rejected anyway: it makes
the answer's cost — and the code's reasoning — depend on what happens to be resident, for a saving
only available when the whole key set hits. The rule that a partition marked `check_disk` is read
before it is answered about is worth more than the read it saves. It is recorded in
[Optimizations](../optimizations.md) rather than lost.

**Normalizing in the generated client builders.** The keys could be sorted where the query is
built, saving the server the work. Rejected because a query arriving over the wire is deserialized
straight into its struct and never passes through a constructor, so the server would have been
trusting a client to have done something it has no way to check. `split_by_shard` is the one place
every query passes through, whoever built it.

**Range predicates in the same change.** `title >= 'M' AND title < 'N'` is what would make paging
inside a partition possible, and it was the larger half of what TODOs asked for. It was kept out
because it is a feature with a grammar design attached — a new query shape, new SHQL operators, and
a rework of the one-condition-per-field rule item 26/39 introduced — while this was a field that
was accepted and ignored. The set is the prerequisite either way: the seek machinery on both arms
is what a range scan reuses, which is exactly what [F1](../../features/sort-key-ranges.md) went on
to do.

## Invariants to uphold

- ~~**An empty `sort_keys` means every row.**~~ **Superseded by
  [F1](../../features/sort-key-ranges.md).** This was how a get that never mentioned the sort key
  reached the table, and how `exists` asked about a partition rather than a row — because a bare
  `Vec<Sort>` had no other way to say "unnarrowed". `SortSelect::All` says it now, and
  `SortSelect::Keys(vec![])` is a set with nothing in it and matches nothing. The obligation the
  rule existed for did not go away, it moved: **a query that never mentioned its sort key must
  produce `All`**, and both the generated builders and the SHQL binding arm do.
- **Sort keys reaching a table are sorted and deduplicated.** The scans seek in the order they are
  given and do not check for repeats, because `normalize_sort_keys` has already run. A new path
  that hands a get to a table without going through `split_by_shard` has to normalize for itself —
  the same obligation `group_by_shard` already imposes for partition keys.
- **`check_disk` stays the only thing that decides whether a partition is read.** A named key
  missing from the copy in memory says nothing about the copy in an archive, and a named key
  *found* in memory says nothing about the other keys the same query named. Sort keys may not
  shorten, skip, or reorder a read.
- **A tombstone is a miss, not a fallthrough.** A seek that lands on `MaybeRow::Tombstone` has
  found a deleted row. Treating it as "not here, try disk" resurrects it, which is
  [item 5](resurrected-deletes.md) by another route.
- **`Archived<Sort>` must order the way `Sort` does.** An archived seek binary-searches by the
  archived ordering, so a hand-written `Ord` on a sort key that disagrees with its derived archived
  `Ord` now silently misses rows the old walk would have found. The archive already depended on
  this — it is written in `Sort` order — but nothing searched it before, so nothing depended on it
  sharply.
- **The limit is still checked before the push.** A seek loop is not exempt: `found` is shared by
  every partition a get touches, and a get replayed after a disk load enters with it already
  filled.

## Still open

**An exists may answer early while its own reads are still in flight.** An exists spanning two
partitions, the first parked on a read and the second holding a match, replies `true` at once and
then replies again — `false` — when the parked partition's replay finds nothing, because the entry
it would have carried was already taken out of `pending_exists`. The client drops the second, since
`ShoalResultStream` has already advanced past that index — so the fault is invisible, not absent.
It is the exists twin of the gap [item 7](sorted-limit.md#invariants-to-uphold) left open on the
get path and wants the same fix: deregistering the query's own entries from `self.blocked`. It
predates this change and the early return was kept exactly as it was, so naming sort keys neither
causes it nor makes it likelier.

**Range predicates — built.** [F1](../../features/sort-key-ranges.md). Read its invariants
alongside the ones above before touching either scan: it kept every rule this page set except one,
and the one it changed is the empty-list rule below.

**A composite sort key cannot be named from SHQL.** Several `#[shoal(sort)]` fields make a tuple
`Sort`, and no SHQL literal is a tuple. That was harmless while sort keys did nothing and is a real
gap now — filed as [item 42](../known-issues.md#42-shql-cannot-express-a-composite-sort-key).

**The wanted keys are re-archived per archived partition.** A get naming *k* keys across *p*
partitions being read in place serializes those keys *k × p* times rather than once. Recorded in
[Optimizations](../optimizations.md).

## Tests

| Test | Fails without |
| --- | --- |
| `a_named_sort_key_selects_one_row`, `named_sort_keys_select_their_rows` (`.../tables/partitions.rs`) | The seek. A four-row partition asked for one row answers with four |
| `a_missing_sort_key_finds_nothing` (same) | The same, in the shape that matters most: a named row this partition does not hold used to answer with every row it does |
| `a_tombstoned_sort_key_is_not_found` (same) | The tombstone arm of the seek. A deleted row comes back |
| `selecting_every_row_returns_every_row`, `an_empty_sort_key_selection_returns_no_rows` (same) | The unnarrowed get, so a future selection cannot narrow a get that never asked to be narrowed. These replaced `an_empty_sort_key_list_returns_every_row` when [F1](../../features/sort-key-ranges.md) made "unnarrowed" and "an empty set" two different questions |
| `a_limit_bounds_a_sort_key_selection` (same) | Nothing today, since it passes either way. It pins the limit check inside the seek loop |
| `exists_answers_for_a_named_sort_key`, `exists_is_true_for_any_named_sort_key`, `exists_is_false_for_a_tombstoned_sort_key` (same) | `SortedPartition::exists`, which did not exist. The unfixed behaviour of the path it replaced is pinned by the integration tests below |
| `exists_selecting_every_row_asks_about_the_partition` (same) | Nothing — it pins the question `exists` used to answer for every query, which is still the right answer for this one |
| `sort_keys_are_put_in_sort_order`, `a_repeated_sort_key_is_only_kept_once`, `no_sort_keys_stay_no_sort_keys`, `a_key_selection_is_normalized` (`shared/queries.rs`) | `normalize_sort_keys`. Rows come back in the order the keys were typed, and a key named twice returns its row twice |
| `get_selects_a_named_sort_key` (`shoal/tests/persistent_sorted_table.rs`) | The whole of the fix, end to end. Five rows for a get naming one |
| `get_selects_several_sort_keys_in_sort_order` (same) | The normalization. The keys are named in reverse and the rows still come back in sort order |
| `get_by_sort_key_misses_return_nothing` (same) | The seek. A miss answers with the whole partition |
| `get_by_sort_key_reads_from_disk` (same) | The archived seek — the only test that covers that arm, since a `ReadResult` cannot be built outside a running server |
| `get_by_sort_key_spans_memory_and_disk` (same) | The read that sort keys are not allowed to suppress. One named row resident, one only in an archive, both returned in sort order |
| `get_by_sort_key_stops_at_its_limit` (same) | The limit check inside the seek loop, across two partitions |
| `exists_by_sort_key_is_false_for_a_missing_row` (same) | The exists half. A row that was never written exists, because the partition holds other rows |
| `exists_by_sort_key_survives_a_disk_load` (same) | The archived arm of the exists, and the read before the answer |
| `binds_sort_keys_from_an_in_list`, `binds_no_sort_keys_when_none_are_named` (`shoal/tests/shql.rs`) | Nothing — they pin the binding that feeds all of the above, which was already right. The second now also pins that an unnarrowed query binds to `SortSelect::All` and not to an empty set |

## Related

- [Query Execution](../../tables/query-execution.md) — the get loop, the blocked-replay model, and
  where a seek sits in it
- [Partitions](../../tables/partitions.md) — `MaybeLoaded` and the scan methods
- [Table Types](../../tables/table-types.md) — what a sorted table buys you, which this is most of
- [SHQL](../../api/shql.md) — where a sort-key condition is parsed and bound
- [`limit` was ignored by persistent sorted tables](sorted-limit.md) — the other half of the pair,
  and the page that left `exists` as a follow-up
- [A multi-partition get answered in an arbitrary order](partition-order.md) — the ordering this
  had to stay inside
