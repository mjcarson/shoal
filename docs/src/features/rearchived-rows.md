# F28. A get answered off disk is written out of the archive it read

## Context

[O2](../appendix/optimizations.md#o2-every-returned-row-is-copied-at-least-twice) — *every
returned row is copied at least twice* — was half closed.
[F27](grouped-responses.md) removed the copy from the **resident** path: a get whose partitions are
all in memory is serialized out of the rows themselves, through a `RowRef<'a, T>` whose archived
type *is* the row's archived type, so the bytes are identical to what the copying path wrote and no
wire version had to move for it.

The open half was the archived path. A partition read from disk stays the archive it was read from
— a `ValidatedArchive` that lives as long as the entry does — and every row it answered was
materialized first:

```rust
found.push_built(P::from_archived(row));   // partitions.rs, and the unsorted twin beside it
```

A full rkyv deserialize per row, then a full serialize straight back into the layout that row had
just been read out of. Per heap field per row that is an allocation, a copy archive→owned, a copy
owned→output and a free, where one copy archive→output would do. The cost is per byte of
`String`/`Vec` payload, which is the axis O2 is graded on: the response codec grows ×432.8 on
decode and ×72.2 on encode over 64 B → 64 KiB.

It was filed as [O40](../appendix/optimizations.md) rather than done because of rkyv rather than
effort. **rkyv serializes in one direction only.** There is no `impl Archive for ArchivedString`,
none for `ArchivedVec`, and none for any archived type a derive generates; the only archived types
that can be written back out are the ones whose archived form is themselves — rkyv's own `rend`
scalars. O40 graded itself **XL** on the strength of the follow-on problem: a mirror would have to
recurse through field types `shoal-derive` never sees, since it reads the *syntax* of a field's
type and a `Vec<Tag>` for a `Tag` in another crate is a name and nothing more.

Nothing about the plumbing was in the way. An archived partition stays resident for the life of the
entry, the sealed reply path already reaches one, and both tables judge an `Accessible` partition
resident. The rows were there and outlived the scan; there was simply no way to write them out.

## What it does

An unprojected get whose partition is still an archive is now answered **out of that archive**. The
scan points at each archived row where it lies, and the reply is serialized straight from those
pointers into the buffer that goes to the client. Nothing is materialized on the way.

Three pieces make that possible.

**`Rearchive`, in `shoal-proto/src/shared/rearchive.rs`** — the direction rkyv does not have:

```rust
/// A type whose archived form can be written back into that same form
pub trait Rearchive: Archive {
    type ArchivedResolver;

    fn serialize_archived<S>(archived: &Self::Archived, serializer: &mut S)
        -> Result<Self::ArchivedResolver, S::Error>
    where S: Fallible + Writer + Allocator + ?Sized, S::Error: Source;

    fn resolve_archived(archived: &Self::Archived, resolver: Self::ArchivedResolver,
                        out: Place<Self::Archived>);
}

/// An archived value standing in for the owned one it was archived from
pub struct ArchivedRef<'a, T: Rearchive>(&'a <T as Archive>::Archived);
```

`ArchivedRef` implements `Archive<Archived = <T as Archive>::Archived> + Serialize<S>` by
delegating to the trait. That is the load-bearing line, and it is the same one `RowRef` rests on
arrived at from the other side: the archived type is the row's own rather than a type shaped like
it, so byte identity is a consequence of the definitions and not something to keep true.

It also **composes**. Anywhere rkyv has a generic impl over a type parameter, substituting
`ArchivedRef<'_, F>` for `F` reuses rkyv's own code — which is the only way `Option` could be
served at all, because `ArchivedOption`'s tag type is private to rkyv and an `ArchivedOption`
cannot be resolved from outside it any other way.

**A mirror generated per row and per projection**, by `shoal-derive/src/traits/rearchive.rs`. It
emits a resolver struct and a `Rearchive` impl whose `resolve_archived` munges the out-place into
one place per field and resolves them, field by field, against the archived struct rkyv's own
derive emits. Each field is classified from the syntax of its type:

| Field type | serialize | resolve |
| --- | --- | --- |
| scalars (`u*`, `i*`, `f*`, `bool`, `char`, `()`) | rkyv's own impl — `rend::u64_le` *is* `Archive<Archived = Self> + Serialize` | `out.write(*archived)` |
| `String` | `ArchivedString::serialize_from_str` | `ArchivedString::resolve_from_str` |
| `Vec<F>` where `F` archives to itself | `ArchivedVec::serialize_from_slice` — keeps rkyv's memcpy branch | `resolve_from_len` |
| `Vec<F>`, `Option<F>` otherwise | rkyv's own impls, over `ArchivedRef<'_, F>` | the same |
| **anything else** | `serialize_via_owned::<F, S>` — materializes *that field* | `resolve_via_owned`, out of the resolver |

**The archived row reaches the wire.** `ShoalProjection` gained `ARCHIVED_IDENTITY`, the archived
twin of F27's `IDENTITY` and set only by the table derive's impl of a row on itself. `RowRef`
became a two-variant enum — `Resident(&'a T)` and `InArchive(&'a <T as Archive>::Archived)` —
with `type Archived` unchanged, and `RowSink` gained a matching `Found::InArchive` and
`push_archived`. The two archived scans then stop building:

```rust
match P::ARCHIVED_IDENTITY {
    Some(_) => found.push_archived(row),
    None => found.push_built(P::from_archived(row)),   // a projection is a strict subset
}
```

Nothing else moved. `SealReply<P>`, `Answer`, `Shard::reply_sealed`, `PROTOCOL_VERSION` and the
wire format are untouched, because the bytes are identical either way — the property F27 already
relies on.

## Design choices

**The fallback is per field, not per row.** A field whose type the derive cannot see inside is
materialized on its own, and every field around it is still written straight out of the archive. A
row with one `HashMap` in it keeps the fast path for its other twenty fields. The mechanism is
`serialize_via_owned`, whose resolver carries the materialized value alive between `serialize` and
`resolve` — rkyv resolves a value after serializing it, and an archived row has no owned value to
keep. That is the whole trick, and it is why nothing had to be refused.

**A nested type can be opted back in.** `#[shoal(rearchive)]` on a field makes the derive emit
`<Ty as Rearchive>::…` instead of the fallback, for a type that implements the trait itself. Opt-in
rather than inferred, because the derive cannot tell whether a name it has never seen implements
anything.

**`ARCHIVED_IDENTITY` is a constant on the projection, hoisted out of the scan.** The sorted scan
asks once per execution rather than once per row, the way `collect_rows` already hoists `IDENTITY`.
A projection cannot set it: `Self::Row = Self` is what makes the identity function type-check, the
same argument `IDENTITY` rests on.

**`Found::InArchive` holds the *row's* archived type**, not the projection's. That is the type the
archive holds and the type `from_archived` takes, so a get that has to fall back to owned rows —
one that parked, or one answering a share of a split get — can still materialize them.
`RowSink::iter` maps it through `ARCHIVED_IDENTITY` at the single place a `RowRef` is minted.

**Byte identity was proven at the bottom before anything was built on top of it.** The first test
written was a hand-written mirror over a row of a scalar, a `String` and an opaque field,
serialized both ways and compared byte for byte. Everything above it — the derive, the sink, the
scan — is only correct because that holds.

## Alternatives rejected

**Copying the archived row's bytes verbatim.** rkyv's relative pointers survive a rigid
translation, so a self-contained subgraph could in principle be memcpy'd and relocated. A row's
sub-allocations are not contiguous with it in the partition's buffer — the partition was archived
as a whole, and a row's `String` data may sit anywhere in it — so there is no region to translate.

**Deserializing a partition once on first read and keeping it `Loaded`.** Every later get would
then take F27's existing resident path and this feature would not be needed. Rejected because it
doubles resident memory for a read-only partition, pays a whole-partition deserialize on first
touch whatever the get asked for, and gives up exactly what `ValidatedArchive` exists for.

**Requiring every field type to implement `Rearchive`.** Simpler code, and it breaks a schema whose
author did nothing wrong, at a `HashMap` field, with an error message about a trait they have never
heard of.

**All-or-nothing per row.** A row containing one field the derive cannot see would take the old
path entirely. One exotic field would then silently cost the row every other field's win, and the
cost would be invisible — both paths answer identically.

**Changing the response type to carry bytes.** The wire format does not need to move for any of
this, and a `GetRows` of raw bytes cannot be merged on the shard collecting a split get without
being read back, which is the copy this exists to remove. The same argument F27 recorded.

## Limitations

**A projection is still built.** `ARCHIVED_IDENTITY` is `Some` only for the whole row. A projection
is a strict subset of its row's fields and there is no archived value of it anywhere to point at,
so it is materialized however its partition is held. It copies less than the row would have, but it
copies.

**A get that parked materializes its rows on replay.** The first get for a partition that is on
disk finds nothing resident, parks on the read, and is replayed once the load lands — and a
replayed get takes the copying path, because the rows its earlier executions found have to outlive
the execution that found them. So the *first* get after a partition is read pays the old cost, and
every get after it is answered out of the archive. Probed rather than reasoned about: with a probe
on `push_archived` and one on `into_owned`, the first get off disk reaches both and the second
reaches only the first. Filed as
[O42](../appendix/optimizations.md#o42-a-get-replayed-after-a-disk-read-copies-rows-its-partition-is-now-holding).

**A share of a split get is still copied once per share.** Unchanged from F27, and for the same
reason: a share is owed to another shard, which has to merge it.

**`Option<Tag>` for an opaque `Tag` falls back as a whole.** The classifier looks one level into a
`Vec` or an `Option`, and an opaque inner type makes the outer field opaque too.

**The fallback's resolver is larger than a plain one.** It carries the materialized value as well as
rkyv's resolver for it, because that value has to survive until `resolve`. That is stack space per
opaque field per row being written, not per row in the reply.

**A row type that is not `Rearchive` can no longer be a projection.** `ShoalProjection` has
`Rearchive` as a supertrait, so a hand-written projection — the test types in `partitions.rs`, and
`shoal/tests/grouped_responses.rs`'s helper — needs a mirror or a bound. Every derived one gets it
for free.

## Invariants to uphold

**`RowRef`'s archived type must stay `<T as Archive>::Archived`, and so must `ArchivedRef`'s.** Not
a type that looks like it. Everything here — the identical bytes, the unchanged wire format, the
untouched client — is a consequence of those two lines. A generated mirror that is *structurally*
identical is not enough: it would be a distinct type, and the reply would archive as something the
client cannot read.

**A mirror must write what rkyv writes, field for field and in declaration order.** The resolve
half munges the out-place into rkyv's own archived struct, so a field resolved in the wrong place
is a silently corrupt row rather than a compile error. The byte-identity tests are what hold this,
and they must keep covering every field shape the classifier has a branch for.

**Only the identity projection may set `ARCHIVED_IDENTITY`.** `RowSink::iter` calls `.expect()` on
it the moment an archived row is in the sink, because a row could only have got there through a
projection that claimed to be its own row. If a projection could set it, that expect becomes a
transmute.

**`push_archived` must keep `mixed` bookkeeping identical to `push_resident`.** A sink stops
implying where its rows live the moment one of them is pointed at, whichever kind of pointer it is.
Getting this wrong misorders a reply that mixes resident, archived and built rows — which is an
ordinary get across several partitions.

**An archived partition must outlive the reply that points into it.** It does, structurally: the
sealed path takes `&self`, and that is exactly why it may not park, block, or take `&mut self`.

## Performance

**Captured** as `f28-rearchive`, micro layer, on a clean tree under the `performance` governor.
Read against `f27-row-sink` rather than against `compare`'s trailing baseline, which is from
2026-08-09 and predates F27 — every resident arm shows −95% against it, which is F27's win being
re-reported, not this one.

**The archived scan, which is what this change is:**

| Arm (1024 rows / 4096 rows) | Before | After | Change |
| --- | ---: | ---: | ---: |
| `maybe_loaded/get_all/1024` | 55.94 µs | 2.51 µs | **−95.5%** |
| `maybe_loaded/get_all/4096` | 224.6 µs | 9.38 µs | **−95.8%** |
| `maybe_loaded/get_range_64/1024` | 3.62 µs | 0.43 µs | −88.1% |
| `maybe_loaded/get_key/1024` | 139.8 ns | 106.0 ns | −24.2% |

`maybe_loaded/build_all` — the same scan through a projection naming every field, which is what
`get_all` did before this change — measures **53.55 µs** at 1024 and **215.1 µs** at 4096 *in this
build*, within 4% of what `get_all` measured in `f27-row-sink`. That is the arm doing its job: the
before and the after are one machine and one binary apart, not two captures apart.

`get_key` was predicted to barely move and moved 24%. One row of two short strings is a seek and
then a deserialize of two strings, and it turns out the deserialize was most of it.

**Writing the reply, which is the other half of the answer path:**

| Arm, 4096 rows | Time |
| --- | ---: |
| `wire_codec/response/build/owned` | 224.2 µs |
| `wire_codec/response/build/borrowed` | 35.5 µs |
| `wire_codec/response/build/archived` | 38.9 µs |

Both predictions held: writing out of an archive beats copying the rows first by **5.8×**, and does
not beat pointing at rows that are already in the target layout — it costs 10% more, which is the
mirror walking a row field by field where `borrowed` hands rkyv a row it can serialize directly.

So a 4096-row get answered off disk goes from *scan 224.6 µs + serialize 224.2 µs* to *scan 9.4 µs
+ serialize 38.9 µs* — **about 9× on the whole answer path**, for the rows it returns.

### What it cost, which is not zero

**`RowRef` became an enum, and F27's resident path pays for it.** Measured in isolation, back to
back on the same machine, at `a1b0cff` against this change:

| `wire_codec/response/build/borrowed` | Before | After | Change |
| --- | ---: | ---: | ---: |
| 16 rows | 198.0 ns | 265.5 ns | +34.1% |
| 256 rows | 1.799 µs | 2.407 µs | +33.8% |
| 1024 rows | 6.543 µs | 9.061 µs | +38.5% |
| 4096 rows | 25.27 µs | 35.47 µs | +40.4% |

A borrowed row was a pointer and is now a pointer and a discriminant — 16 bytes per row in the
`Vec` instead of 8 — and `resolve` gained a match with an unreachable arm, per row. **This is a
regression on the path most gets take**, imposed by the change that made the archived path 24×
faster, and it is filed as
[O43](../appendix/optimizations.md#o43-a-borrowed-row-costs-a-discriminant-it-usually-does-not-need)
rather than left inside this page. It is not a reason to revert: the resident path is still **6.3×**
faster than copying, which is what F27 bought and what this keeps.

### One number in the capture is not about the code

`wire_codec/width/request/encode/serialize/65536` reports **+71.5%** in this capture and
`width/response/encode/serialize/65536` **+42.6%**. Neither arm touches anything this change
touches — they serialize an owned request bundle. Run in isolation, the arm measures **7.50 µs at
this commit against 7.80 µs at the parent**, which is *faster*, against the **17.7 µs** the capture
recorded for it. So the arm costs more than twice as much when it runs two hundred benchmarks into a
capture than when it runs alone, and the capture number is about that rather than about the commit.

The designated control held — `partition_sorted/archived/walk_all` is +1.4% to +2.7% across all four
sizes, against the +9.4% it moved in `f27-row-sink` — so this is not a hot machine. It is
position-in-capture drift on the widest arms, and it is the third capture in a row to show a band
this page cannot explain. Added to the standing
[todos entry](../appendix/todos.md) that asks for a repeat capture at one commit, which would bound
it; that entry is now the cheapest unbuilt thing in the measurement corpus.

## Tests

| Test | Breaks if |
| --- | --- |
| `an_archived_row_is_byte_identical_to_the_row_it_was_read_from` | the mirror stops writing what rkyv writes, over a row of a scalar, a `String` and an opaque field |
| `every_container_writes_what_its_owned_form_writes` | a container shape drifts — `String`, `Vec<u8>`, `Vec<String>`, `Option<String>`, `Option<u64>` |
| `an_archived_row_is_byte_identical_to_an_owned_one` | `RowRef::InArchive` stops archiving as the row it points at |
| `a_mixture_of_resident_and_archived_rows_writes_what_owned_rows_write` | one reply carrying both kinds of pointer stops writing what a reply of owned rows writes |
| `an_archived_scan_points_at_every_row_it_returns` | an archived get materializes a row it could have written out of the archive — this feature's claim, stated as a count |
| `an_unsorted_archived_get_points_at_its_row` | the same, for the unsorted table |
| `a_projected_archived_scan_builds_every_row` | a projection is answered in place, which it cannot be |
| `an_accessible_and_a_loaded_partition_agree` | the two ways a partition is held stop answering identically |
| `a_get_off_disk_answers_the_same_rows_as_one_in_memory` | end to end, sorted: the rows, their order or their group index differ between a get off disk and the same get in memory |
| `a_get_off_disk_answers_the_same_row_as_one_in_memory` | the same, unsorted |
| `a_projected_get_off_disk_answers_what_a_resident_one_does` | a projection read off disk stops being a projection — both tables |
| `wire_codec/response/build/{owned,borrowed,archived}` | not a test — the arm that prices this, with the before and the after in one build |

The count tests are what matter. Both paths answer identically, so nothing else in the tree would
notice if a get quietly went back to materializing every row — which is exactly how
[item 80](../appendix/resolved/never-flushed-partitions.md) hid inside F27, and the reason a probe
was put on this path before it was called done.

## Related

- [F27](grouped-responses.md) — the resident half, and `RowRef`
- [F26](archive-routed-requests.md) — the request half, and the pattern both follow
- [O2](../appendix/optimizations.md#o2-every-returned-row-is-copied-at-least-twice) — closed by
  this, together with F27
- [O40](../appendix/optimizations.md) — what this was filed as
- [O42](../appendix/optimizations.md#o42-a-get-replayed-after-a-disk-read-copies-rows-its-partition-is-now-holding)
  — the parked replay, found on the way
- [F9](ephemeral-tables.md) — why an ephemeral table needs none of this: it holds rows, never an
  archive
- [Resolved #80](../appendix/resolved/never-flushed-partitions.md) — the discipline this page's
  probe follows
