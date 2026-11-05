# 42, 207. A sort key of any type but `String` did not compile, and SHQL could not name a composite one

Two defects closed by one change. Item 207 kept a sorted table's sort key a `String`: an integer, a
`Uuid` or a key of two fields failed the table derive. Item 42 was its SHQL half: once a composite
sort key compiled, no SHQL query could name one. It was filed first, on the assumption that such a
table compiled, which it did not. W1 listed the first as an optional prerequisite of the scans plan,
"sort keys of any `Ord` type", since a scan of a sorted table hands rows out in whatever order its
sort key has ([W1](../../future-work/scans/prerequisites.md#optional)).

## Symptom

A sorted table whose `#[shoal(sort)]` field was anything but a `String` did not compile:

```text
error[E0277]: the trait bound `u64: shoal::traits::RkyvSupport` is not satisfied
  --> shoal/tests/sort_key_types.rs:56:13
   |
56 |     pub at: u64,
   |             ^^^ the trait `shoal::traits::RkyvSupport` is not implemented for `u64`
```

Nor did one of two sort fields, whose key is their tuple, nor a `Uuid`, which failed a second bound
too:

```text
error[E0277]: the trait bound `uuid::Uuid: DeepSizeOf` is not satisfied
   --> shoal/tests/sort_key_types.rs:111:13
    |
111 |     pub id: Uuid,
    |             ^^^^ the trait `DeepSizeOf` is not implemented for `uuid::Uuid`
```

So every schema that wanted rows ordered by a number padded it into a string: the bench's `Event`
pads `at` to twenty digits, and [F54](../../features/tmdb-dataset-deployment.md)'s
`MovieByKeyword` joins a title and a padded id into one key. A key padded short of its widest value,
or a negative number padded at all, orders wrongly without a word.

Once the key compiled, SHQL could not name a composite one. `WHERE stream = 7 AND seq = 2 AND name =
'b'` was refused:

```text
Failed to deserialize sort key: invalid type: integer `2`, expected a tuple of size 2
```

## Cause

### The bound named a trait one type implemented

`ShoalSortedTable::Sort` was bound by `Ord + Hash + RkyvSupport + Debug + From<Self::Sort> + Clone +
DeepSizeOf` (`shoal-proto/src/shared/traits/sorted.rs`). `RkyvSupport` is a trait of default
methods that a type opts into with an empty impl, and the only standard type ever opted in was
`String` (`traits.rs`, `impl RkyvSupport for String {}`). A schema could not opt `u64` in itself,
since neither the trait nor the type is its own. The engine under the bound was generic throughout.
Seeks, ranges, cursors, walk pieces, the builders and the dataset derive all take any `Sort`. The
todo that filed this called it "a missing impl rather than a design decision", and it was one,
apart from the order question below.

`DeepSizeOf` was on the bound because a sorted partition derived `DeepSizeOf`, and a partition's
charge for a key was `sort.deep_size_of()` less its slot. deepsize2 has no `uuid` feature, and
neither Shoal nor a schema may implement it for `Uuid`.

### An archived partition is sought in its keys' archived order

What made this more than a missing impl is the invariant
[Resolved #8](sort-keys.md#invariants-to-uphold) recorded: **`Archived<Sort>` must order the way
`Sort` does.** A partition read from disk is sought in place. `SeekBytes` archives the keys a get
named and asks `ArchivedBTreeMap::get` and `range` for them, and a walk's pieces compare archived
keys with each other. None of them deserializes a key. rkyv wrote the archive in the native order,
and it is searched in the archived order. For a `String` the two agree. Admitting another type is
a claim that they agree for it too, and a type whose two orders disagree answers an archived get
with the wrong rows, silently.

### SHQL bound one literal to the whole key

The sorted parse arm of `#[shoal::db]` found *the* condition naming a sort field and deserialized
its literal into `Sort` (`shoal-derive/src/structs/client.rs`). For a tuple there is no such
literal. The database macro could not have done better where it stood: it sees the table's type
and never its fields, so it knows neither the names nor the types a composite key is built from.

## Evidence

**Reproduced.** `shoal/tests/sort_key_types.rs` was written first, a table of each shape, and
compiled against the unfixed tree (`40bcbd4b`). It failed with ten errors, one for each shape and
nothing else:

```text
error[E0277]: the trait bound `u64: shoal::traits::RkyvSupport` is not satisfied            (x2)
error[E0277]: the trait bound `i64: shoal::traits::RkyvSupport` is not satisfied
error[E0277]: the trait bound `uuid::Uuid: shoal::traits::RkyvSupport` is not satisfied
error[E0277]: the trait bound `uuid::Uuid: DeepSizeOf` is not satisfied
error[E0277]: the trait bound `(u64, std::string::String): shoal::traits::RkyvSupport` is not satisfied   (x2)
error[E0277]: the trait bound `std::option::Option<u32>: shoal::traits::RkyvSupport` is not satisfied
error[E0432]: unresolved imports `shoal::shared::traits::check_sort_key_order`, `shoal::shared::traits::SortKey`
error[E0432]: unresolved import `shoal::shared::queries::parser::SORT_KEY_PRODUCT_LIMIT`
error: could not compile `shoal` (test "sort_key_types") due to 10 previous errors
```

Item 42 could not be reproduced on that tree, since its table did not compile. It was reproduced
on the tree with the trait in place and SHQL's binding untouched. There every row of the binary
passed but the two that name a composite key in SHQL: the reads and writes of every shape, resident
and archived, the integer-keyed walk, and SHQL's integer, negative, uuid and newtype keys. Those two
failed:

```text
thread 'shql_names_a_composite_sort_key' panicked at shoal/tests/sort_key_types.rs:906:5:
assertion `left == right` failed
  left: Err("Failed to deserialize sort key: invalid type: integer `2`, expected a tuple of size 2")
 right: Ok([(2, "b")])
```

Before any type was admitted, its archived order was checked in registry sources. rend 0.5.3 orders
every archived integer and `char` by `to_native()`. rkyv 0.8.12's `ArchivedTupleN` derives `Ord`
field by field. `ArchivedOption` puts `None` first, as `Option` does. `ArchivedString` orders as
`str` does. And `uuid-1` archives a `Uuid` as itself. `check_sort_key_order` then holds each of them
to it over its edges and two thousand seeded values.

## The fix

**A sort key is any `SortKey`** (`shoal-proto/src/shared/traits/sort_key.rs`):

```rust
pub trait SortKey:
    RkyvSupport<
        Archived: Ord + Debug + for<'a> CheckBytes<..> + rkyv::Deserialize<Self, Strategy<Pool, Error>>,
    > + Ord + Hash + Debug + Clone
{
    fn heap_bytes(&self) -> usize;
}
```

`type Sort: SortKey + From<Self::Sort>` replaces the old bound. The archived form's `Ord`,
validation and decode are bounds of the trait, so a type that lacks one fails where it is
implemented, not deep in the engine's generics. That every key a client sends is bytechecked is the
trait's bound, not a convention.

**Implemented for every type whose archived order provably is its own.** That is the integers from
8 to 128 bits signed and unsigned, `bool`, `char`, `String`, `Uuid`, `Option` of a key, and tuples
of keys from two to twelve wide, the widest std orders. `RkyvSupport` is implemented for each beside
it, the tuples' and `Option`'s generically over their members. **A schema's own type opts in** by
implementing `SortKey`. `check_sort_key_order` is public and is how its tests hold it to the trait's
promise: it sorts a set of values natively, archives each, and checks every adjacent pair orders
the same way archived. For two total orders adjacent pairs suffice. It also checks each archived
value decodes to itself. The test binary's `Day(u32)` is the worked example.

**A partition asks the key for its heap.** `SortedPartition::key_heap` calls
`SortKey::heap_bytes`, which is `capacity()` for a `String`. That is the figure deepsize2 counted,
so no partition's charge moved, and `a_keys_heap_is_what_deepsize_counts` holds it to the old
formula. `SortedPartition` no longer derives `DeepSizeOf`, and `PartitionSupport` no longer
requires it. Its default `size()`, a deep walk, was overridden by both partition kinds and called
by nothing.

**SHQL's selection moved to the table's derive** (item 42). `ShoalSortedTable`'s derive generates
`Row::shql_build_sort_select`, as the filter derive generates `shql_build_filters`. The database's
sorted arm calls it. A single sort field is read exactly as before, messages included. A composite
key is read field by field through three parser helpers (`shoal-proto/src/shared/queries/parser.rs`):

- `composite_sort_clauses` finds each field's clause in declaration order. A key named by none of
  its fields is `SortSelect::All`, and one named in part is refused by name: a prefix of it is a
  range, which SHQL cannot write yet.
- `composite_sort_points` reads a field's values as its own type, and refuses a range on any field
  of the key by name.
- `check_sort_key_product` multiplies the fields' value counts with `checked_mul` and refuses a
  product past `SORT_KEY_PRODUCT_LIMIT` (10,000) **before any key is built**. The generated code
  then crosses the sets into tuples with one nested loop a field.

The server sorts and deduplicates the keys, as it does every set (`normalize_sort_keys`). Completion
offers the range operators only on a table's sole sort field.

chrono's times, which a sort key of time was the point of, cannot archive under rkyv 0.8 at all.
They are [F131](../../features/time-types.md), built in the same change on this trait.

## Alternatives rejected

- **A blanket `impl<T: Archive + Serialize<..>> RkyvSupport for T`.** It is the shortest fix and
  does not compile: it overlaps every impl the derives emit for a row and its queries (E0119). It
  would also have admitted every archivable type as a sort key, the ones whose orders disagree
  among them.
- **A sealed set of key types.** Safe by construction, and it was the other option offered when the
  scope was decided. It was rejected because a newtype key, a day, an epoch or an id, is how a
  schema says what its key means. The open trait's promise is the same as `Ord`'s: a `BTreeMap`
  trusts a type's `Ord` as much as an archive trusts this one. The difference is that this promise
  is checkable, and the check is shipped.
- **Seeking an archive by deserializing its keys.** Comparing native against native would make any
  `Ord` type correct. It would also deserialize every key a seek passes, which is what the archived
  seek of [Resolved #8](sort-keys.md) exists not to do, on the read path of every get of an
  archived partition.
- **`DeepSizeOf` for `SortedPartition` by hand.** It would have kept `PartitionSupport` as it was,
  and it would have been a second sizing of a partition that nothing calls. `heap_bytes` is the
  only figure the partition uses, and it is the one a key type has to state.
- **A default `heap_bytes` of zero.** It would have let a schema's key holding a `String` report no
  heap, so a node would grow past its budget unseen, which is [Resolved #196](row-charge-undercount.md)
  again. An impl says what its key holds, even when that is nothing.
- **A range on a composite key's last field.** `a = 1 AND b > 5 AND b < 9` is expressible as a range
  of tuples, but `a = 1 AND b > 5` is not without a greatest `b`, which `Sort` does not name. Building
  the one-sided half's refusal beside the two-sided half's acceptance teaches a rule nobody would
  guess. Prefix ranges stay one open todo.
- **Fixing item 41 in the same change.** It is the same shape for the partition key and the helpers
  are written to serve it, but a composite partition key hashes rather than orders, its `IN` names
  partitions rather than rows, and it was not asked for. Its entry now names the shape.
- **Migrating the bench's and TMDB's padded keys.** The bench's `Event.at` would move the fingerprint
  of every workload and part the corpus from the frozen baseline. TMDB's is a deployed schema, whose
  changed row is a new lineage ([F93](../../features/operations-and-the-real-cluster.md)). An
  integer key priced beside a padded string is filed as
  [O156](../optimizations.md#o156-an-integer-sort-key-priced-beside-a-padded-string).

## Invariants to uphold

- **A `SortKey`'s archived form orders exactly as the type does.** Every archived seek, range, exists
  and walk piece depends on it. A new impl in `sort_key.rs` gets a row in its tests through
  `check_sort_key_order`. A schema's impl is that schema's to check, and the trait's docs and
  [Derive Macros](../../api/derive-macros.md#sort-key-types) say so. `usize` and `isize` stay out:
  rkyv archives them in 32 bits, so a key past four billion would not archive.
- **Validation and decode are the trait's bounds.** A key arrives in a client's bundle, and an
  archived key is read where it lies. A type whose archived form were not `CheckBytes` would be read
  unvalidated.
- **`heap_bytes` has no default, and a `String` reports its capacity.** It is what a partition
  charges a key to the node's budget.
- **A composite key's product is checked before anything is allocated.** The check counts the
  values each field named; the cross builds `count` keys with that capacity. Reordering them, or
  building the keys to count them, is the multiplication the limit exists to stop.
- **A composite key is named whole or not at all, and by points alone.** A prefix of it is a range,
  and the refusal names what is missing. When prefix ranges are built, this is the rule that moves.
- **A single sort field binds as it always did.** Its messages are the same strings, so a client or
  a test matching on them still matches.
- **No existing schema's fingerprint, archive or wire moved.** No row type changed, and the trait is
  not part of a fingerprint. `fingerprint.rs`'s frozen literals and `client_half`'s frozen ids passed
  unchanged.

## Still open

- **[Item 41](../known-issues.md#41-shql-cannot-express-a-composite-partition-key)**: SHQL cannot
  name a composite *partition* key. The helpers here are the shape of its fix.
- **Prefix ranges over a composite sort key**, in SHQL and the typed API alike
  ([todos](../todos.md#sort-key-range-predicates--built)). They need the greatest and least values
  of the fields a prefix leaves out.
- **[O156](../optimizations.md#o156-an-integer-sort-key-priced-beside-a-padded-string)**: what an
  integer key saves over the padded string the bench and TMDB still use is unmeasured.
- **`check_sort_key_order` is a test, not a guard.** Nothing stops a schema implementing `SortKey`
  for a type whose orders disagree and never running it. A derive that implemented `SortKey` only
  where both `Ord`s are derived over the same fields would close that, and is not built.
- **A row holding a `Uuid` still sizes itself by hand**, since deepsize2 has no `uuid` impl. Rows
  sized without deepsize2 are a todo.

## Tests

| Test | Fails without |
| --- | --- |
| `shoal` `sort_key_types::{unsigned,unsigned_in_memory,signed,uuid_key,composite,composite_in_memory,optional,schema_owned}::resident_rows_answer_in_key_order` | The binary does not compile: the `SortKey` bound. Each is a key shape whose order its strings would not keep, held to a get of every row, named keys, a range, paging by cursor, exists, an update and a delete |
| `shoal` `sort_key_types::*::archived_rows_answer_in_key_order` (six persistent shapes) | The same, on rows read back from an archive and sought by their archived keys, beside a row written over the archive, and across a restart. It is what holds each shape's archived order to its own |
| `shoal` `sort_key_types::days_order_as_they_archive` | The binary does not compile: `check_sort_key_order`, run as a schema would run it over its own key type |
| `shoal` `sort_key_types::shql_binds_an_integer_sort_key`, `shql_binds_a_negative_sort_key`, `shql_binds_a_uuid_and_a_newtype_sort_key` | The binary does not compile. Each binds a key that is not a string from SHQL's literal and answers in the key's own order |
| `shoal` `sort_key_types::shql_names_a_composite_sort_key` | Item 42: the arm deserializing one literal into a tuple refuses every point |
| `shoal` `sort_key_types::shql_refuses_what_a_composite_sort_key_cannot_name` | The refusals by name: a key named in part, a range, and a product past the limit |
| `shoal` `sort_key_types::an_integer_keyed_partition_walks_in_key_order` | The binary does not compile. A scan's page folds an archived partition of integer keys with a resident overlay a piece at a time, its cursors archived integers, and a driver's page folds it whole |
| `shoal` `shql::binds_a_composite_sort_key_by_every_field`, `binds_every_pairing_of_a_composite_keys_sets`, `binds_every_row_when_a_composite_key_is_left_out` | Item 42, bound without a server: the key built in declaration order whatever order the query names it in, the cross product, and `All` |
| `shoal` `shql::refuses_a_composite_sort_key_named_in_part`, `refuses_a_range_on_a_composite_sort_key`, `refuses_what_a_composite_keys_fields_cannot_bind` | The refusals, each naming the field it is about |
| `shoal` `shql::suggests_only_points_for_a_composite_sort_key` | Completion offers a range on a field of a composite key, which binding refuses |
| `shoal-proto` `sort_key::tests::{integers,scalars_strings_and_uuids,optional_keys,tuples}_order_as_they_archive` | An impl whose archived order disagrees with its own, at its edges or over two thousand seeded values |
| `shoal-proto` `sort_key::tests::a_key_whose_orders_disagree_is_reported` | The check itself: a key built to disagree is reported by name |
| `shoal-proto` `sort_key::tests::a_keys_heap_is_what_deepsize_counts` | `heap_bytes` moving a string key's charge from deepsize2's figure |
| `shoal-client-check` `client_half::shql_names_a_composite_sort_key_of_a_time` | A composite key of a time and a number, named by SHQL in a build with no engine |

## Related

- [F131. Times a row can hold](../../features/time-types.md), the time types built on this trait
- [Sort keys were accepted and ignored](sort-keys.md), whose archived-order invariant this keeps
- [F1. Sort key ranges](../../features/sort-key-ranges.md), whose ranges and cursors any key now takes
- [A partition key of two or more fields did not compile](composite-partition-key.md), the partition
  key's twin, which filed item 207
- [Derive Macros](../../api/derive-macros.md#sort-key-types), [SHQL](../../api/shql.md),
  [Table Types](../../tables/table-types.md)
- [W1](../../future-work/scans/prerequisites.md#optional), which listed this as an optional
  prerequisite of the scans plan
