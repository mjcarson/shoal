# 93. The archived partition hash disagreed with the live one

`PartitionKeySupport` had three ways to hash a partition key: from a row, from a key's values,
and from an archived row. The third disagreed with the first two for most key types and had no
caller. [Resolved #65](gxhash-pin.md)'s golden key test found it by reading the derive, and the
object storage plan listed it as an optional prerequisite
([S1](../../object-storage/prerequisites.md#optional)): `ObjectMeta` is keyed by a path, a string,
which is the shape it got wrong. The item offered two fixes: delete the function, or make it agree
and freeze it beside the live hash. It was deleted, the fix the user chose.

## Symptom

None that anything saw, since nothing called it. A caller would have placed a row in a different
partition from the one its insert used, and almost always in a different tablet. The string key
`"shoal"` hashed to `0x7498_3e29_46f5_ce66` (tablet 1865) from a row and to
`0x1971_f478_ff20_1b37` (tablet 407) from the same row archived. That is the silent data loss the
item warned of: a read finds nothing, and no error says why.

It also limited what could be a key. A partition field whose archived type has no `to_native()`
failed to compile in this one generated method, though the live paths take any `Hash`. That
covers `Uuid`, which rkyv archives as itself, `bool`, and every `Vec`:

```text
error[E0599]: no method named `to_native` found for struct `Uuid` in the current scope
   --> shoal/tests/partition_keys.rs:121:52
    |
121 |     Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq,
    |                                                    ^^^^^^^^^^^^^^^^^^ method not found in `Uuid`
    |
    = note: this error originates in the derive macro `ShoalUnsortedTable` (in Nightly builds, run with -Z macro-backtrace for more info)
```

## Cause

The live paths hash each partition field through its own `Hash`, in declaration order, into one
`GxHasher::default()` ([Resolved #92, #198](composite-partition-key.md)). The archived path was
written by the derive from each field's type name instead:

| Field type | Archived path wrote | Live path writes |
| --- | --- | --- |
| `String` | `write(bytes)` | `Hash for str`: `write(bytes)`, then `write_u8(0xff)` |
| `u64` | `write_u64` | `write_u64` |
| `u32` | `write_u32` | `write_u32` |
| anything else | `write_u64(x.to_native() as u64)` | the type's own `Hash` |

Item 93 named the first row. The last is wider than a string. gxhash 2.3.1 gives every width its
own load (`write_u8` to `write_u128` and `write_i8` to `write_i128` in its `hasher.rs`, each a
broadcast of the value across the lane in `platform/x86_128.rs`), so a `u16` or an `i32` written
as a `u64` is a different input. A `usize` key went through std's default `write_usize`, the
value's bytes through `write`, which is different again. A `u128` written as a `u64` also lost its
upper half: the key 2^64 became 0, and hashed exactly as the `u64` key 0 does. X8's and X10's
stripe rows key an object by `object: u128`, so this was the stripe row's shape as well as the
path's. Integers agreed only as `u64`, `u32` and `i64`, the last because `load_i64` is the same
broadcast as `load_u64`. A composite key disagreed wherever one of its fields did.

Nothing noticed, because nothing hashed an archived row. [F26](../../features/archive-routed-requests.md)
routes an archived bundle by the `key: u64` its client hashed and put beside the row, and
[F68](../../features/conditional-writes.md)'s conditional insert carries its key the same way.
Every engine site hashes a row it has deserialized: an apply, a replay, a dataset read.
[Partitioning](../../architecture/partitioning.md) said the archived variant served intent log
replay, which deserializes.

## Evidence

**Reproduced.** A test was written first and run against the unfixed tree (`1e1c051`). It added a
table keyed by a `u128` to the golden key test's schema, archived one row of each key shape with
`rkyv::to_bytes`, and compared `get_partition_key_from_archived_insert` with `get_partition_key`:

```text
running 3 tests
int 42                 archived 0x3036ffe2cc259d89 live 0x3036ffe2cc259d89 agree
text "shoal"           archived 0x1971f478ff201b37 live 0x74983e2946f5ce66 DISAGREE
pair ("shoal", 42)     archived 0x386eed5531280a68 live 0x182ef82c353071b0 DISAGREE
triple (1, 2, 3)       archived 0x45915a4622421929 live 0x45915a4622421929 agree
wide 2^64              archived 0x20bfea6723380d78 live 0xac4dab108a447644 DISAGREE

thread 'archived_hash_agrees_with_live' (3050246) panicked at shoal/tests/partition_keys.rs:153:5:
assertion `left == right` failed: 3 of 5 key shapes hash differently archived
  left: 3
 right: 0
```

Three of five shapes disagreed. The live hashes are the frozen literals for `"shoal"` and
`("shoal", 42)`, so the live path is the right one. The archived hash of 2^64 is the frozen hash
of the `u64` key 0. The `Uuid` error above came from the same tree, with a table keyed by a
`Uuid` whose row sized itself by hand (see [Still open](#still-open)), so the error left is this
item's alone.

The reproduction is not in the tree, because it calls the function the fix deleted. The doctest
pair below stands in its place.

**On the lab.** The fix moves no persisted key, and it was held to a deployed cluster to show that.
The cluster in `tmdb_cluster.yaml` (europa, titan and hyperion, a factor of three) was destroyed
and bootstrapped fresh on 2026-10-11 at the unfixed tree, with europa running the loader:

- **The load.** The first 20,000 movies of the csv wrote 190,051 rows in 3.5s. These are movies
  keyed by id, release rows keyed by three integers, and 149,644 keyword rows under 19,995
  keywords, a string key each.
- **`verify --limit 20000 --read quorum`, before.** It found all 20,000 movies, all 20,000 release
  rows by their composite key, and all 19,995 keyword partitions. None was missing or different.
- **`upgrade` to the fixed build**, one node at a time with the leader last. Each node came
  back and caught up beside members still on the old build. The schema fingerprint covers a
  table's fields and roles, not its trait's methods, so the two builds are one schema.
- **`verify`, after.** At quorum, again all 20,000 movies, 20,000 release rows and 19,995 keyword
  partitions, none missing or different. Then each member read alone (`--read one --member N`)
  through its own copy gave the same counts. `status` showed 3 of 3 copies with writes admitted.

Nothing had been archived yet (every member held 0 B), so each node rebuilt its tables from its
WAL as it restarted, and the new build hashed every row again on apply. Every keyword partition
was written by a node that had the archived variant compiled in, and read back from a node that
did not. Both hash a key through the live path alone, which the fix did not touch.

## The fix

The function is gone:

- `PartitionKeySupport::get_partition_key_from_archived_insert` is off the trait
  (`shoal-proto/src/shared/traits.rs`). The trait gained a docstring stating the rule instead:
  a key is hashed from a row or from a key's values, and code holding an archived row
  deserializes it or reads the key its client hashed.
- The derive no longer builds `hash_archived_stmts` or emits the method
  (`shoal-derive/src/traits/partition_key.rs`). `is_u64_type` and `is_u32_type` in
  `shoal-derive/src/utils.rs` served nothing else and went with it.
- The two hand-written implementations in `shoal-core/src/server/tables/partitions.rs`'s tests
  lost their copies.

Two guards were added. `shoal::traits` carries a doctest pair in the style of `shoal::dataset`'s.
Both twins archive a string-keyed row. The compiling twin deserializes it and holds its hash to
the key's. The `compile_fail` twin is the same program calling the archived method. Its only error
on the fixed tree is the method not existing (`E0599`), which was checked by compiling it as a
plain doctest. The golden key set gained the two types an object's id could be keyed by: four
`u128` keys and three `Uuid` keys, so twenty-three literals now. A `Uuid` key could not compile
before. Every earlier literal passed unchanged on the fixed tree.

## Alternatives rejected

- **Make it agree, and freeze it.** The derive could hash each archived field through its own
  `Hash`. rkyv defines `ArchivedString`'s as its `str`'s, rend's integers' as their native
  value's, and `ArchivedVec`'s as its slice's, so the shapes in the golden set would agree. But
  agreement would hold type by type, only where rkyv's archived `Hash` happens to equal the native
  one, and only for the shapes a test names. A `Vec<u64>` key would still disagree: `[u64]`
  hashes its bytes in one write through `hash_slice`, and a slice of rend's `u64_le` writes each
  element through `write_u64`. Freezing it would also pin a value nothing reads, which
  [Resolved #65](gxhash-pin.md) declined to do for this reason. And it would keep a method on a
  trait every schema implements, for a caller that does not exist.
- **The narrow fix the item named, `write_str` for a string.** It mends the first row of the
  table above and leaves the last: a `u128` key truncated, every other width disagreeing.
- **Leaving it.** The S1 row was "listed so that nobody gives it a caller first", which a page
  cannot stop. Deleted, a caller has to write a hash of its own, and meets the doctest and this
  page on the way.

## Invariants to uphold

- **A partition key is hashed two ways and no third.** `get_partition_key` hashes a row and
  `get_partition_key_from_values` a key's values. Both feed each partition field's own `Hash`, in
  declaration order, into one `GxHasher::default()`. A third way has to be held to these by a test
  before it has a caller, not after.
- **Code holding an archived row and no key deserializes the row**, or reads the key its client
  hashed. F26's routing and F68's conditional insert carry `key: u64` beside the row for exactly
  this, and a new frame that carries a row should do the same.
- **The twenty-three literals in `partition_keys.rs` are a persistence format.** The seven new
  ones belong to the two types an object id could be keyed by. gxhash loads a `u64` into both
  halves of its 128-bit lane, so the `u128` keys 0 and `u128::MAX` share the hashes of the `u64`
  keys 0 and `u64::MAX`. That is two tables sharing a number, and it places no two rows together.
- **A `Uuid` and the `u128` of the same bits are different partitions.** `Hash for Uuid` hashes
  its bytes, and the test asserts the two differ. A table keyed by one cannot switch to the other
  without a migration, which is a choice [M12](../../object-storage/milestones.md#m12-tables-what-the-metadata-needs)
  ~~makes~~ made once for the object id: a `u128` ([F79](../../features/bucket-schema.md#design-choices)).
- **The doctest pair stays a pair.** A `compile_fail` doctest passes on any error. This one's
  first draft passed on an ambiguity between rkyv's `Serialize::serialize` and
  `RkyvSupport::serialize`, and only its compiling twin, which failed on the same line, showed
  it. The twins differ in the one call.

## Still open

- **A row holding a `Uuid` cannot derive `DeepSizeOf`.** deepsize2 0.1 has no `uuid` feature and
  its derive skips no field, so the golden test's `ByUuid` sizes itself by hand. It is not this
  item: a key of that type now compiles, and the gap is the row's. It is filed in
  [TODOs](../todos.md#a-row-holding-a-uuid-sizes-itself-by-hand) for M12, whose object id is
  minted as a UUID v7 ([S3](../../object-storage/objects.md#object-id)). M12 keyed it as a `u128`,
  so the generated rows do not meet it ([F79](../../features/bucket-schema.md#design-choices)).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal` doctest `shoal::traits`, the `compile_fail` twin | It compiles, and so fails, as soon as the archived method is back on the trait |
| `shoal` doctest `shoal::traits`, the compiling twin | Nothing on a revert. It is what keeps the twin honest: it fails if the shared program stops compiling for any other reason, and it holds an archived row's hash, taken after deserializing, to its key's |
| `shoal` `partition_keys::partition_keys_hash_to_frozen_values` | The binary does not compile, since `ByUuid`'s key, a `Uuid`, has no `to_native()`. Short of a revert, a change that moves a `u128` or `Uuid` key's hash or tablet, or makes a row and its key disagree for either, fails on the literal |

## Related

- [Resolved #65](gxhash-pin.md), whose golden key test found this item and declined to freeze
  the archived hash.
- [Resolved #92, #198](composite-partition-key.md), which defined how a composite key is hashed
  and left this item open beside it.
- [F26](../../features/archive-routed-requests.md) and
  [F68](../../features/conditional-writes.md), the two paths that route a row in its buffer by a
  key carried beside it.
- [Partitioning](../../architecture/partitioning.md), on the two ways a key is hashed.
- [S1](../../object-storage/prerequisites.md#optional), which listed this as optional, and
  [S3](../../object-storage/objects.md), on the path and the object id.
