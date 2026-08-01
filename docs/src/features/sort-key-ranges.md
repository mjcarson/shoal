# F1. Sort-key range predicates

A sorted partition is a `BTreeMap` keyed by sort key. Until this, the only two questions you could
ask one were "every row" and "these exact rows" — so the structure whose whole purpose is to be
sought in a span was never asked for one.

## Context

[Item 8](../appendix/resolved/sort-keys.md) made a sort key select rows, which was the half of this
that was a defect. What was left was the half that is a feature, and
[TODOs](../appendix/todos.md) carried its design.

The gap was sharper than it sounds. A `LIMIT 20` over a partition of a few thousand titles always
answered with the *first* twenty, and there was no way to ask for the next twenty — the only
spelling that could was an `IN` list of the exact keys you wanted, which you would have to already
know. Paging over a large partition meant fetching all of it. With a lower bound it is a seek plus
twenty rows, whatever page you are on:

```sql
SELECT * FROM MovieByKeyword WHERE keyword = 'alien' AND title > 'Gravity' LIMIT 20
```

That query is the whole feature. The last row of a page, fed back as an exclusive lower bound, is a
cursor onto the next one — which is why there is no cursor type anywhere in this change.

## What it does

**A sorted get or exists selects its rows one of three ways**, and the type says which:

```rust
pub enum SortSelect<S> {
    /// Every row in the partition
    All,
    /// The rows named by these sort keys, in sort order and without repeats
    Keys(Vec<S>),
    /// The rows whose sort key falls inside this range
    Range(SortRange<S>),
}

pub struct SortRange<S> {
    pub start: Bound<S>,
    pub end: Bound<S>,
}
```

`shared/queries/sorted.rs`. `SortedGet::sort_select` and `SortedExists::sort_select` replaced the
`sort_keys: Vec<Sort>` both used to carry.

**SHQL grew four operators.** `<`, `<=`, `>`, `>=`, on a sort key:

```sql
SELECT * FROM MovieByKeyword WHERE keyword = 'alien' AND title >= 'G' AND title < 'H'
```

The two conditions on `title` are the one place `AND` may name a field twice; they are folded into
a single clause while the `WHERE` clause is read, so everything downstream still sees one clause
per field.

**The typed API grew a builder**, and `SortRange` carries the vocabulary rather than each generated
query repeating it:

```rust
MovieByKeywordGet::new(keywords)
    .sort_range(SortRange::after("Gravity".to_string()))   // the cursor
    .limit(20)
```

`SortRange::after`, `starting_at`, `before`, `ending_at`, `new`, `with_start`, `with_end`.
`MovieByKeywordExists::sort_range` is the same on the exists side.

**The completion menu offers the operators on a sort key and nowhere else**, since binding refuses
a range on anything but one and the menu should not walk a user into an error it can see coming.

## Design choices

**The three ways to select rows are one enum, not three fields.** A query arriving over the wire is
deserialized straight into its struct and never passes through a constructor, so the type is the
only place a nonsensical combination can be refused. "These keys *and* this range" is not a state
the server has an opinion about, because it is not a state that can be built.

This changed one rule. An empty `sort_keys` list used to mean *every row*, because it was the only
way a get could say it had not narrowed itself. `SortSelect::All` says that now, so `Keys(vec![])`
is a set with nothing in it and selects nothing. That is what "these rows" has always meant for
every other set, but it is the opposite of what
[item 8](../appendix/resolved/sort-keys.md#invariants-to-uphold) documents, so it is called out
below and pinned by a test.

**Both partition forms seek; neither walks.** A resident partition uses `BTreeMap::range`, and an
archive uses `ArchivedBTreeMap::range` (rkyv `collections/btree/map/iter.rs`), which descends to
the lower bound and stops at the first key past the upper one rather than scanning from the start.
The two arms had already been made twins by item 8 and stayed twins here.

**A range is checked for emptiness before it is used.** `BTreeMap::range` **panics** on a range
whose start is past its end, and on one whose ends are equal with either of them excluding.
`SortRange::is_empty` catches both, and every scan asks it first. `title > 'm' AND title < 'm'` is
one typo away from a valid query, and without the guard it takes the shard down.

**The two halves of a range are folded in the parser, not in the binder.** `merge_field_clauses`
(`shared/queries/parser.rs`) allows a field to be named twice only when the two conditions bound
opposite ends, and merges them into one `WhereClause`. Every other repeat is still refused with the
error it always had. This is what lets the three generated `.find(..)` lookups — partition key,
sort key, and `shql_build_filters` — stay lookups.

**The three scan arms share one loop.** `SortedPartition::collect_rows` and `::any_row` take an
iterator of rows, so the filtering, the limit check, and the push exist once rather than once per
arm; `MaybeLoaded::collect_archived` and `::any_archived` are their archived counterparts. The
three arms differ only in which iterator they build. Adding a fourth way to select rows should not
be a fourth place the limit can be checked in the wrong order.

**The archived form of a query's keys is built once per execution.** `SeekBytes`
(`server/tables/partitions.rs`) owns the serialized keys and bounds, and
`PersistentSortedTable::get`/`exists` build it lazily on the first partition being read in place.
A get every one of whose partitions is resident builds none of it. This closed **O19**, which was
about the same waste on the sort-key path.

## Alternatives rejected

**`BETWEEN`.** `title BETWEEN 'M' AND 'N'` is sugar over `>= AND <=`, and its inner `AND` collides
with the `AND` that joins clauses — `where_conditions` and the completion state machine would both
need lookahead states to tell the two apart. Recorded in [TODOs](../appendix/todos.md) rather than
built.

**An additive `Option<SortRange>` field beside `sort_keys`.** Smaller diff, and it keeps the
generated builders exactly as they were. Rejected because it makes "keys and a range" representable
and therefore something the scan has to have a precedence rule about — a rule with no meaning that
every future reader has to look up.

**Relaxing the one-clause-per-field rule globally instead of folding.** Letting a field be named
twice and leaving the two clauses in the list is less parser code. It was rejected because the rule
is load-bearing in three separate places in generated code, none of which would fail loudly: a
second clause on a filter field would simply be ignored by `shql_build_filters`, which finds the
first one. Folding keeps the guarantee that made the binding stage a lookup.

**Letting a range decide which partitions are read.** A range that matches nothing resident could
skip the disk read. It is the same idea as **O20** and it is wrong for the same reason: what a
partition holds in memory says nothing about what its archive holds. See below.

**A cursor token in the protocol.** An opaque cursor returned with a page and handed back for the
next one is what most databases expose. Rejected because it is a second thing to version, a second
thing to validate, and it buys nothing an exclusive lower bound does not — a sort key already
identifies a row, and the client already has it. A stateless cursor cannot go stale against a
partition that was written to in between; it simply resumes from wherever that key now sits.

**Prefix ranges over composite sort keys.** Several `#[shoal(sort)]` fields make a tuple `Sort`,
and a range over a *prefix* of that tuple needs synthesized minimum and maximum values for the
remaining elements. The typed API can already range over whole tuples, since a tuple is `Ord`.
SHQL cannot name one at all ([item 42](../appendix/known-issues.md#42-shql-cannot-express-a-composite-sort-key)).

## Limitations

- **A range does not reduce I/O.** A cold partition is read whole either way, because there is no
  index within a partition on disk — an archive entry is an offset and a size. The win is CPU,
  deserialization, and rows on the wire.
- **Ranges bind on a sort key only.** A range on a partition key is refused (`a partition is
  located by its exact key`) and so is a range on a filter (`a filter checks a row against the
  values it may take`). Both parse and then fail during binding, so the error names the field.
- **A range applies to the whole `Sort` value.** No prefix ranges over a composite sort key.
- **No `BETWEEN`, `!=`, or `LIKE`.**
- **`OR` cannot join a range.** Two ranges are a union, and there is no access path that reads one.
- **Rows are still grouped by partition rather than merged by sort key.** A range spanning several
  partitions answers partition by partition, in the order the query named them — so paging across
  partitions pages each of them in turn, and is not a global ordering. This is the same limitation
  a get has always had ([26, 39](../appendix/resolved/partition-order.md)).
- **A range is not validated against the sort key's type beyond deserialization.** `title > 5` on a
  `String` sort key fails to bind, which is right, but an inverted range is accepted and answers
  with nothing rather than being called out.

## Invariants to uphold

- **`SortSelect::All` is the only thing that means every row.** This is the one behaviour the
  feature changed. `Keys(vec![])` names no rows and matches none, and a query that never mentioned
  its sort key must produce `All` — the generated SHQL arm and `#nameGet::new` both do. A branch
  that treats `All` as an empty set turns every unnarrowed get into a miss, and one that treats an
  empty set as `All` turns a narrow miss into a whole partition.
- **A range is checked for emptiness before it is used.** `is_empty` is not an optimization. Both
  the resident and the archived scan call it before their seek, in `get` and in `exists`, because
  `BTreeMap::range` panics on the two shapes it catches.
- **`check_disk` stays the only thing that decides whether a partition is read.** A range matching
  nothing resident says nothing about what an archive holds, and one matching everything resident
  says nothing about the rows only the archive has. Bounds may not shorten, skip, or reorder a
  read. This is item 8's invariant, and a range makes it easier to break because a bounded scan
  looks so much more like a decision than a point lookup does.
- **`Archived<Sort>` must order the way `Sort` does.** An archived range descends by the archived
  ordering across a whole span rather than at one key, so a hand-written `Ord` on a sort key that
  disagrees with its derived archived `Ord` now silently returns the wrong rows rather than the
  wrong row. The archive already depended on this — it is written in `Sort` order.
- **The limit is checked before the push, inside the range walk.** `found` is shared by every
  partition a get touches, and a get replayed after a disk load enters with it already filled.
  `collect_rows` is the one place this happens for all three arms; keep it that way.
- **A tombstone inside a range is skipped, not returned.** A range walks rows rather than seeking
  each of them, so it meets tombstones the way an unnarrowed scan does. Returning one is
  [item 5](../appendix/resolved/resurrected-deletes.md) by another route.
- **Downstream still gets one `WhereClause` per field.** The two halves of a range are folded in
  `merge_field_clauses` precisely so this holds. A change that leaves two clauses for one field in
  the list breaks three `.find(..)` lookups quietly.
- **A bound's inclusivity belongs to the query, not to the archive.** `MaybeLoaded::archived_bound`
  archives only the *value* an end holds and carries `Included`/`Excluded`/`Unbounded` over as it
  stands. Archiving the `Bound` itself would work and is a wire format change for nothing.
- **`SeekBytes` is built at most once per execution, and only when an archive is scanned.** It is
  threaded as `&mut Option<SeekBytes>` for that reason. Building it eagerly makes every all-resident
  get pay for serialization it never uses, which is the regression this shape exists to avoid.

## Performance

| Access | Before | After |
| --- | --- | --- |
| A page of a resident partition of *n* rows | `O(n)` visits, every row cloned or filtered | `O(log n)` seek plus the page |
| A page of an archived partition | `O(n)` visits, `O(n)` filter evaluations | `O(log n)` descent plus the page |
| Archiving a get's wanted keys, *k* keys over *p* archived partitions | *k × p* serializations and validations | *k* serializations, *p* validations — **O19 closed** |
| Archiving a get's wanted keys, every partition resident | *0* | *0*, unchanged |
| Reading a cold partition | whole partition | whole partition, **unchanged** |

The last row is the one to remember. A range changes what is deserialized and what crosses the
wire; it does not change what is read off disk.

## Tests

| Test | Fails without |
| --- | --- |
| `a_range_selects_its_rows` (`.../tables/partitions.rs`) | The resident range arm. A five row partition asked for three of them answers with five |
| `an_excluded_lower_bound_skips_its_key`, `an_upper_bound_decides_whether_its_key_is_kept` (same) | Inclusivity. An off-by-one at either end, which for the lower bound means every page repeats its predecessor's last row |
| `an_unbounded_range_returns_every_row` (same) | Nothing today — it pins that a range bounding nothing is not a range matching nothing |
| `an_inverted_range_returns_nothing`, `an_empty_exclusive_range_returns_nothing` (same) | The `is_empty` guard. Both **panic** without it rather than answering wrongly |
| `a_range_skips_tombstones` (same) | The tombstone filter on the range walk. A deleted row comes back |
| `a_range_stops_at_its_limit`, `a_full_get_takes_no_rows_from_a_range` (same) | The limit check inside the range walk, and its position before the push |
| `exists_answers_for_a_range`, `exists_is_false_for_a_range_of_tombstones`, `exists_over_an_empty_range_is_false` (same) | The exists half of all three of the above |
| `selecting_every_row_returns_every_row`, `an_empty_sort_key_selection_returns_no_rows` (same) | The rule that changed. Together they pin that `All` and `Keys(vec![])` are different questions |
| `a_range_survives_normalization`, `all_rows_stay_all_rows`, `a_key_selection_is_normalized` (`shared/queries.rs`) | `SortSelect::normalized`. A range widened or dropped as a query enters the server |
| `an_inverted_range_is_empty`, `a_point_range_needs_both_ends_to_include_it`, `a_half_open_range_is_never_empty` (same) | `is_empty` itself, including the case where a missing bound must not read as a crossing |
| `a_range_knows_which_keys_it_holds` (same) | `SortRange::contains` |
| `parses_each_range_operator` (`.../parser/tests.rs`) | The grammar. Each operator landing on the wrong end, or the wrong inclusivity |
| `range_operators_prefer_their_longer_spelling` (same) | The operator scan order. `>=` read as `>` followed by a value starting `=` |
| `two_bounds_on_one_field_fold_together`, `bounds_fold_in_either_order` (same) | The fold. Two clauses for one field reach the binder, which finds only the first |
| `rejects_two_bounds_on_the_same_end`, `rejects_a_value_and_a_range_on_one_field`, `rejects_a_range_joined_by_or` (same) | The three refusals. Each of them silently answers half a query instead |
| `rejects_unsupported_operators` (same) | The operator error, and — for `!=` — that the operator scan runs before the equality one |
| `tracks_the_positions_of_range_values` (same) | The spans. A binding error on a bound points at the whole query instead of the literal |
| `expects_a_value_after_each_range_operator`, `a_two_character_operator_is_one_token` (`.../parser/complete/tests.rs`) | The completion tokenizer. `<` and `>` fell into the numeric fallback, so a bounded query offered nothing at all |
| `binds_a_sort_key_range`, `binds_every_range_operator`, `binds_a_sort_key_range_from_both_ends` (`shoal/tests/shql.rs`) | The binding arm, end to end from SHQL to a `SortSelect::Range` |
| `rejects_a_range_on_a_partition_key`, `rejects_a_range_on_a_filter`, `rejects_a_range_on_an_unsorted_table` (same) | The role refusals. A range on a partition key would be dropped and the query answered as if it were unbounded |
| `binds_no_sort_keys_when_none_are_named` (same) | That an unnarrowed query binds to `All` and not to an empty set of keys |
| `suggests_range_operators_only_for_a_sort_key` (same) | The role gate on the menu. Operators offered where they cannot bind |
| `get_by_range_selects_its_rows`, `get_by_range_honours_each_bound` (`shoal/tests/persistent_sorted_table.rs`) | The whole of the feature, end to end through a server |
| `get_by_range_reads_from_disk` (same) | The archived range — the only test that covers that arm, since a `ReadResult` cannot be built outside a running server |
| `get_by_range_spans_memory_and_disk` (same) | The read a range is not allowed to suppress. One row resident, two only in an archive, all three returned in sort order |
| `get_by_an_empty_range_returns_nothing` (same) | The `is_empty` guard through a real shard, and that the shard is still answering afterwards |
| `get_by_range_stops_at_its_limit` (same) | The limit across two partitions of one range |
| `a_partition_can_be_paged_by_its_sort_key` (same) | **The point of the feature.** Seven rows walked in pages of two, asserting the pages concatenate to the whole partition with no gap and no repeat |
| `exists_by_range_answers_for_its_rows`, `exists_by_range_survives_a_disk_load` (same) | The exists half end to end, including the disk read before a false |
| `shql_bounds_rows_by_a_sort_key_range` (same) | The typed path and the SHQL path landing on the same query |

## Related

- [SHQL](../api/shql.md) — the grammar, and where a range is parsed and bound
- [Query Execution](../tables/query-execution.md) — the get loop and where a seek sits in it
- [Partitions](../tables/partitions.md) — `MaybeLoaded` and the scan methods
- [Derive Macros](../api/derive-macros.md) — the generated `Get` and `Exists`
- [Sort keys were accepted and ignored](../appendix/resolved/sort-keys.md) — the half of this that
  was a defect, and the seek machinery a range reuses
- [`limit` was ignored by persistent sorted tables](../appendix/resolved/sorted-limit.md) — the
  limit that stops a range walk early
- [A multi-partition get answered in an arbitrary order](../appendix/resolved/partition-order.md) —
  the ordering a range had to stay inside
- [Optimizations](../appendix/optimizations.md) — O19, closed here, and O20, still open
