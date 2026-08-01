# 26, 39. A multi-partition get answered in an arbitrary order

Filed as two things that turned out to be one. *SHQL silently drops duplicate conditions on one
field* (26) was about a spelling that lied: `WHERE keyword = 'a' AND keyword = 'b'` reads as an
intersection and was answered as a union. *A multi-partition get answered in an arbitrary order*
(39) was about what came back once it had been: the same query, run twice, returned different
rows. Both had to be fixed together, because a spelling nobody could rely on and an order nobody
could rely on add up to a query whose answer is not a function of the query.

## Symptom

Against the TMDB example:

```sql
SELECT * FROM MovieByKeyword WHERE keyword = 'giant worm' AND keyword = 'alien' LIMIT 2;
```

returned movies carrying *either* keyword, not both. Repeating the query returned two rows from
the `alien` partition on one run and two from `giant worm` on the next. Within a partition the
order was stable; across partitions it was not.

The consequence is worse than the surprise. Paging over several partitions was impossible: with
no order to page along, a client had to fetch every row and sort them itself, which is the work
the database exists to do.

The unsorted side had the mirror-image bug. `WHERE id = 1 AND id = 2` bound `id = 1` and dropped
the second value with no warning, because an unsorted get could only ever name one partition.

## Cause

### The connective had no meaning of its own

`AND` was the only connective the grammar had, and it was applied by position rather than by what
it joined. `ParsedSelect` produced a flat `Vec<WhereClause>` and the generated binding arms
bucketed those by field role, which gave one keyword three different meanings:

| Query | What it did |
| --- | --- |
| `WHERE id = 1 AND id = 2` on an **unsorted** table | `id = 1`. The second was dropped by `.find(...)`. |
| `WHERE movie = 'a' AND movie = 'b'` on a **sorted** table | *Both*, as two partitions to read — a union. |
| `WHERE id = 1 AND title = 'a' AND title = 'b'` (a filter) | `title = 'a'`. The second was dropped. |

Each was confirmed by running it against a real schema. Only the middle row was useful, and it
was useful by accident: the sorted arm used `filter` where the other two used `find`.

### Rows were ordered only inside a partition

A `SortedPartition` holds its rows in a `BTreeMap<T::Sort, _>`, so a single partition has always
answered in sort-key order. Nothing ordered the partitions themselves, and two separate places
scrambled them.

**On one shard,** `PersistentSortedTable::get` accumulated every partition's rows into one shared
`Vec`. A partition that had to be read from disk could not answer during that pass — it was
parked in `pending_data` and replayed once the read finished, appending its rows *after* the
partitions that were already resident. So the order depended on which partitions happened to be
in memory.

**Across shards,** `Shard::handle_gathered` merged the shares as they arrived and
`ResponseAction::merge` was a plain `ours.extend(theirs)`. The order was therefore whichever
shard replied first, which is a race.

### The limit was applied to that arbitrary order

Worse than cosmetic, because `LIMIT` decides *which* rows come back. The per-shard loop stopped
as soon as the rows it already held filled the limit, and then dropped the partitions it was
still waiting on:

```rust
if get.limit_reached(&data) {
    // stop waiting on a partition we are done with
    blocked.retain(|key| key != partition_key);
    continue;
}
```

With the first partition a query named on disk and the second in memory, `LIMIT 2` answered
entirely out of the second one and threw away the first — the one the query asked for first.

## The fix

**The connective now means what it says.** `IN (v1, v2)` is the spelling for giving a field
several values, and `OR` between two conditions on the same field folds into the same clause.
`AND` keeps its real meaning of joining conditions on *different* fields, and constraining one
field twice with `AND` is a parse error naming the `IN` list that was meant. `OR` across two
fields is rejected too, since one side of it would name rows in no partition the query could
read. See [SHQL](../../api/shql.md#and-or-and-in).

`WhereClause` changed shape to make this structural rather than a check: it holds
`values: Vec<WhereValue>` and the parser guarantees one clause per field, so the binding arms are
a lookup instead of a search and cannot disagree with each other.

**Rows are slotted by partition.** `pending_data` holds a `PendingGet<R>`
(`server/tables/persistent.rs`) with one slot per partition the get named. A partition read back
from disk fills *its own* slot rather than appending to the end, so the answer is assembled in
the order the query named its partitions in whatever order the reads finish.

**The limit follows the order.** The shared-accumulator early exit is replaced by
`PendingGet::filled_before`, which lets a partition be skipped only when every partition named
*before* it has been read and they already hold the whole limit. An unread partition earlier in
the query can still supply rows that come first, so nothing may be skipped past it.

**The coordinator reorders before truncating.** `Gather` carries the partition order from the
unsplit query, and `handle_gathered` calls `ShoalResponseSupport::order_by_partitions` — a stable
sort by where each row's partition was named — before applying the limit. Stable, so rows within
one partition keep the sort-key order their shard produced.

**Unsorted gets became multi-partition.** `UnsortedGet` carries `partition_keys: Vec<u64>` like
`SortedGet` does and splits across shards through the same `group_by_shard` helper, which also
deduplicates keys so no partition is read — or returned — twice.

## Alternatives rejected

**A global sort-key merge.** Interleaving rows from every partition by sort key, so `LIMIT 3`
over two keywords returns the three first titles overall. This is a real `ORDER BY` and is what
most people picture. It was rejected because it forces every named partition to be read even
under a small limit: a partition cannot be skipped until you know its smallest key, and the
cheapest way to know that is to read it. Grouping by partition keeps the early exit, and a
caller who wants a global sort can still get one by asking for all the rows.

**Sorting the merged rows instead of slotting them.** Simpler on the shard, but it does not fix
the limit: rows already dropped by a wrong early exit cannot be sorted back into existence. The
slots exist because the order has to be right *before* the limit is applied, not after.

**Making `AND` a synonym for `IN`.** It would not have broken anything and would have kept the
old queries working. It was rejected because the query would still read as an intersection and
answer as a union — the defect being fixed.

## Invariants to uphold

- **Partition keys reaching a table are unique.** `group_by_shard` deduplicates, and
  `PendingGet::rank` relies on a key naming at most one slot. A new path that hands a get to a
  table without going through `split_by_shard` has to deduplicate for itself.
- **A slot is filled exactly once, and every slot is filled before answering.** `is_pending`
  gates the response on it. A path that leaves a slot unfilled without registering a read for it
  hangs the query; one that fills a slot whose read is still in flight answers twice.
- **`order_by_partitions` runs before `truncate`.** Reversing them keeps the first rows to
  arrive rather than the first rows asked for.
- **The reorder is a stable sort.** An unstable one would scramble the sort-key order inside each
  partition, which is the half of the ordering the shards are responsible for.
- **A partition may only be skipped on a resolved prefix.** `filled_before` returns false as soon
  as it meets an unread slot. Weakening that to "we have enough rows" reintroduces the original
  defect exactly.

## Tests

`shoal/tests/persistent_sorted_table.rs`:

- `get_returns_partitions_in_query_order` — the rows of a get spanning several shards, in full,
  and reversed when the query names its partitions the other way round.
- `get_partition_order_is_stable_across_repeats` — the same query run twenty times. One run
  proves nothing here, since the order it replaced depended on a race and could match by luck.
- `get_partition_order_survives_disk_loads` — a get spanning a resident partition and one that
  has to be read from disk, with the read one named first.
- `get_limit_takes_the_first_partitions` — which rows a limit keeps, not just how many.
- `get_limit_prefers_a_blocked_partition` — the regression `filled_before` exists for: first
  partition on disk, second resident, `LIMIT 2` answers out of the first.

`shoal/tests/persistent_unsorted_table.rs` covers the same three properties for multi-partition
unsorted gets, which could not be expressed at all before.

The grammar half is covered by `shoal-core/src/shared/queries/parser/tests.rs` (`IN` lists, `OR`
folding, and both new errors) and `shoal/tests/shql.rs` (what those bind to).
