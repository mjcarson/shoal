# 92, 198. A partition key of two or more fields did not compile

One defect, filed twice. [F37](../../features/node-identity-control-plane.md)'s golden key test
found it as item 92, and [F66](../../features/dataset-benchmarks.md)'s `dataset_rows` test found
it again as item 198, which was filed without a reference to the first. The object storage plan
read the two together and listed the fix as a required prerequisite of its M12
([S1](../../object-storage/prerequisites.md#required)): `StripeMeta` is keyed by a consumer id, an
object id and a stripe index. Packing the three into one field to get round the derive would have
frozen the workaround into the persisted key of every stripe.

## Symptom

A table with more than one `#[shoal(partition)]` field failed the table derive's own expansion,
so the schema declaring it did not build:

```text
error[E0308]: mismatched types
  --> shoal/tests/dataset_rows.rs:74:9
   |
74 |         ShoalUnsortedTable,
   |         ^^^^^^^^^^^^^^^^^^ expected `String`, found `&String`
```

Every table in the repository had a single partition field, so nothing else noticed. The
multi-field branches of the get, update, delete, exists, dataset and conditional derives build a
tuple key, and none of them had ever been compiled against a table.

## Cause

`shoal-derive/src/traits/partition_key.rs` gives a table with several partition fields the
key type `(A, B)`. `get_partition_key_from_values(values: &Self::PartitionKey)` hashes that
tuple's members one at a time, and it is correct.

`get_partition_key(&self)` hashes a row, and it was built by handing the row's fields to that
function:

```rust
Self::get_partition_key_from_values(&(&self.a, &self.b))
```

That expression is a `&(&A, &B)`, not a `&(A, B)`, so it is a type error for every composite
key. With one field the same branch produces `&self.a`, which is the right type, and that is the
only case anything had ever compiled.

## Evidence

**Reproduced.** The tests below were written first and run against the unfixed tree (`0a68d62`).
None of them compiled, and every error was this one site, once per partition field after the
first. There was no other error, which is what said the other multi-field branches were sound:

```text
error[E0308]: mismatched types
  --> shoal/tests/partition_keys.rs:50:52
   |     Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
   |                                                    ^^^^^^^^^^^^^^^^^^ expected `String`, found `&String`
error[E0308]: mismatched types
  --> shoal/tests/partition_keys.rs:68:52
   |                                                    ^^^^^^^^^^^^^^^^^^ expected `u64`, found `&u64`
error: could not compile `shoal` (test "partition_keys") due to 3 previous errors

error: could not compile `shoal` (test "composite_partition_keys") due to 6 previous errors
error: could not compile `shoal` (test "dataset_rows") due to 2 previous errors
error[E0308]: mismatched types
  --> shoal-client-check/src/lib.rs:77:52
   |                                                    ^^^^^^^^^^^^^^^^^^ expected `u64`, found `&u64`
```

**On the lab.** The fix was held to a deployed cluster as well, through a third table in the TMDB
schema, `MovieRelease`. It is keyed by release year, month and id: three integers, the stripe
row's shape. The cluster in `tmdb_cluster.yaml` (europa, titan and hyperion, a factor of three) was
destroyed and bootstrapped fresh on 2026-10-03, with europa running the loader. Then:

- **The load.** The full csv wrote 3,382,336 rows in 89.0s, 1,188,548 of them release rows. 231
  writes were retried after `NotLeader` while the groups' first elections settled.
- **The load's read back.** It found 10,073 of 10,073 sampled movies, and their 10,073 release
  rows by the composite key.
- **`verify --read quorum`.** It found all 1,188,009 distinct release keys, each holding a row
  the csv filed under that key. All 1,187,691 movies and 58,418 keyword partitions matched too.
- **`verify --read one --member N`.** Each of the three members, read alone, gave the same counts
  through its own copy.

A row is hashed by `get_partition_key` on every replica when the write is applied, and a get is
hashed by `get_partition_key_from_values` on the client. If the two disagreed, every one of those
reads would have missed.

## The fix

`get_partition_key` no longer goes through the tuple. It hashes the row's partition fields itself,
in declaration order, into one `GxHasher`:

```rust
let mut hasher = ::shoal::gxhash::GxHasher::default();
Self::hash_field(&mut hasher, &self.a);
Self::hash_field(&mut hasher, &self.b);
hasher.finish()
```

This writes the same bytes `get_partition_key_from_values` writes from `&(a, b)`. A tuple's
`Hash` hashes its members in order, with no prefix or length, and that function hashes each
member through `hash_field` too. The single-field case is generated the same way, and it is the
same bytes as before (`hash_field(&self.a)` is what `from_values(&self.a)` did). The eight frozen
single-field literals passed unchanged on the fixed tree, which is the proof.

Two composite shapes joined the golden key set in `partition_keys.rs`: a string and an integer,
and three integers. No earlier tree could produce their literals, since no such table compiled
before. They were printed by the fixed tree and held to a definition written without the derive:
a fresh `GxHasher` fed each value's own `Hash` in order (`composite_keys_hash_field_by_field`).

## Alternatives rejected

- **Cloning the fields into a tuple and calling `get_partition_key_from_values`.** It compiles and
  hashes the same bytes, but it copies the key on every insert, every replica's apply and every
  conditional write. A `String` key would cost an allocation each time, just to be hashed and
  dropped.
- **Changing `get_partition_key_from_values` to take a tuple of references.** That would change a
  signature every caller of a key uses, typed queries and the dataset driver included. It would
  also make a single-field key a different type from the field it is, to fix a function that only
  ever has a row to hand.
- **Packing the fields into one.** That is the workaround the object storage plan would not take.
  A key packed into a string or a wider integer is a format, and once a stripe row is written
  under it, it is a migration to undo.
- **Fixing item 41 in the same change.** Item 41 is SHQL's parse arm turning one literal into
  the whole key. It is now reachable, since a composite key compiles, but it is a parser change
  with its own design question: what `IN` over two fields means. A typed query is the path the
  object store uses. The refusal is pinned by a test instead, so a fix will be noticed.

## Invariants to uphold

- **A row and the key naming it hash the same bytes.** These are each partition field's own
  `Hash`, in declaration order, into one `GxHasher::default()`, with nothing between them.
  `get_partition_key` is how a row is placed when it is applied. `get_partition_key_from_values`
  is how a get, update, delete or exists finds it. A change to either that is not made to the
  other sends every read to a partition no insert wrote. `composite_keys_hash_field_by_field`
  holds both to the definition, not just to each other.
- **The order of a key's fields is part of the persisted key.** `(1, 2, 3)` and `(3, 2, 1)` are
  different partitions, and reordering the `#[shoal(partition)]` fields of a table with rows in it
  moves every row. It is a migration, the same as a hash change.
- **The sixteen literals in `partition_keys.rs` are a persistence format**, the composite eight
  included.
- **The archived hash is not frozen.** `get_partition_key_from_archived_insert` still writes a
  string without its terminator (item 93), for a composite key as for a single one. The day it
  gets a caller, it has to be made to agree first.

## Still open

- **[Item 41](../known-issues.md#41-shql-cannot-express-a-composite-partition-key): SHQL
  cannot name a composite key.** It used to be unreachable and now is reached:
  `shql_refuses_a_composite_key` pins that the parse is refused, not panicked on.
- **[Item 93](../known-issues.md#93-the-archived-partition-hash-disagrees-with-the-live-one-for-every-string-key):
  the archived hash of a string.** It is unchanged and still has no caller.
- **[Item 207](../known-issues.md#207-a-sorted-table-with-two-shoalsort-fields-does-not-compile):
  a composite sort key does not compile**, filed by this change. A sort key's type must implement
  `RkyvSupport`, which only `String` does. It is not the same defect, because a sort key has an
  order on disk as well as a hash. A sorted table with a composite partition key and one `String`
  sort key works, and the tests use exactly that. [Item 42](../known-issues.md#42-shql-cannot-express-a-composite-sort-key)
  is its SHQL half.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal` `partition_keys::composite_keys_hash_field_by_field` | The binary does not compile. A fix that hashed the row's fields in another order, or hashed one twice, fails here rather than on a read that misses |
| `shoal` `partition_keys::partition_keys_hash_to_frozen_values` | The binary does not compile; a change that moves a composite key's hash or tablet fails on the literal |
| `shoal` `composite_partition_keys::persistent_unsorted_round_trip` | The binary does not compile: insert, a three-key get, exists, update, a refused `if_absent` and delete on a persistent table keyed by three integers, with a reversed key and a neighbour kept apart |
| `shoal` `composite_partition_keys::ephemeral_unsorted_round_trip` | The same, on the ephemeral table |
| `shoal` `composite_partition_keys::persistent_sorted_round_trip` | The same on a persistent sorted table keyed by a string and an integer, with a sort key narrowing the partition |
| `shoal` `composite_partition_keys::ephemeral_sorted_round_trip` | The same, on the ephemeral sorted table |
| `shoal` `composite_partition_keys::persistent_rows_are_found_after_a_restart` | Rows compacted to disk under a composite key are not found by it after two restarts |
| `shoal` `dataset_rows::a_composite_keyed_row_builds_a_get_of_both_fields` | `Stock`, back in the catalog, does not compile, or its dataset read is not a get of the tuple |
| `shoal-client-check` `client_half::a_composite_key_hashes_the_same_from_a_row_and_from_its_values` | The client-only build of a composite key does not compile, or its row and key disagree |
| `shoal-client-check` `client_half::shql_refuses_a_composite_key` | Not reverted by this fix: it pins item 41's refusal, and fails when item 41 is fixed, which is when it should be rewritten |
| `tmdb-dataset` `load::tests::a_release_date_that_will_not_parse_is_zero` | `MovieRelease` does not compile, or a date that will not parse stops filing its movie under zero |

## Related

- [Resolved #65](gxhash-pin.md), whose golden key test found item 92 and froze the two shapes
  that built.
- [F66](../../features/dataset-benchmarks.md), whose `Stock` table found item 198.
- [F54](../../features/tmdb-dataset-deployment.md), whose schema now carries `MovieRelease`.
- [S1](../../object-storage/prerequisites.md#required) and
  [M12](../../object-storage/milestones.md#m12-tables-what-the-metadata-needs), which required this.
- [Derive Macros](../../api/derive-macros.md), on what a composite key's queries take.
