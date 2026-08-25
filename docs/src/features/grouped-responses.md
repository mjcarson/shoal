# F27. A get's answer carries its partitions, and its rows where they lie

## Context

Two entries on the [optimizations page](../appendix/optimizations.md) shared a rank, a tier and a
line of code, and the page was explicit that they had to be taken together:

- **[O2](../appendix/optimizations.md#o2-every-returned-row-is-copied-at-least-twice)** — a get
  materialized owned rows out of the partition and then serialized them straight back into bytes.
  `f24-routing` measured the two stages that do it at **×65** (`execute`) and **×238**
  (`reply_serialize`) over 1 KiB → 512 KiB rows: together 35% of a wide get, and the only two
  stages growing faster than the query around them.
- **[O18](../appendix/optimizations.md#o18-the-gathered-reorder-rehashes-every-rows-partition-key)**
  — a share of a split get carried rows and nothing else, so the shard collecting the shares had to
  hash every row's partition key again to put them back in the order the query named their
  partitions in. Once per row, for something every shard already knew and threw away.

Both change `ResponseAction::Get`. Landing either alone pays the wire-format break twice, which is
what the [dependency edges](../appendix/optimizations.md#dependency-edges) table said and why
neither had been taken for four features.

This is the response-side mirror of [F26](archive-routed-requests.md), whose own page called for
it: *"a design pass that makes the table layer archive-aware in one direction and not the other is
doing the harder half of the work twice."*

## What it does

**A get's answer carries an index of the partitions its rows came from.**

```rust
pub struct RowGroup { pub partition: u64, pub len: u64 }

pub struct GetRows<T> { pub rows: Vec<T>, pub groups: Vec<RowGroup> }
```

`rows` is laid out in `groups` order: group *i* covers the rows starting at the sum of the lengths
before it. `order_by_partitions` ranks the groups — as many lookups as the query named partitions —
and moves each run into place. Nothing is hashed.

**And a get whose partitions are all resident is answered with the rows themselves.** The reply is
serialized inside the table that found them, out of pointers into the partitions holding them:

```
         a get that read only resident partitions          a get that had to read disk,
                                                           or is a share of a split get
         ──────────────────────────────────────            ───────────────────────────────
scan     RowSink::push_resident(&row)                      RowSink::push_built(P::from_row(row))
                     │                                                   │
build    GetRows<RowRef<'_, P>>                            GetRows<P>  ← owned, outlives the scan
                     │                                                   │
reply    seal(...) → AlignedVec        Answer::Sealed      Response<P>  Answer::Open
                     │                                                   │
shard    reply_sealed ──────────────────────────────────── reply → to_bytes → reply_sealed
```

`RowRef<'a, T>` is what makes the two paths write the same bytes, and it is fifteen lines:

```rust
impl<T: Archive> Archive for RowRef<'_, T> {
    type Archived = <T as Archive>::Archived;   // the *same type*, not a copy of its shape
    type Resolver = <T as Archive>::Resolver;
    fn resolve(&self, resolver: Self::Resolver, out: Place<Self::Archived>) {
        self.0.resolve(resolver, out)
    }
}
```

Because the archived type is the row's own, `Vec<RowRef<'a, T>>` archives to the identical
`ArchivedVec<Archived<T>>` a `Vec<T>` does, through the identical resolver. Byte identity is a
consequence of the type definitions rather than a property somebody has to keep true.

**The client did not change.** `ShoalResponse::access` reaches into `.rows` and keeps its
signature, so all **sixty-eight** `access::<T>()` call sites across the tests, benches, examples
and `shoal-bench` compile untouched. A new `ShoalResponse::groups` exposes the index for the rare
caller that wants it.

`PROTOCOL_VERSION` goes 1 → 2. No header field moved, but a peer built before this reads
`ArchivedGetRows` as `ArchivedVec` — a pointer into the wrong place rather than an error — so the
two must never speak. The version byte is refused before a frame is read, and it is mixed into
every schema fingerprint ([F10](framing-and-protocol-evolution.md)), so a mismatch is a refused
connection naming both sides twice over.

## Design choices

**Flat rows plus an index, not `Vec<(u64, Vec<T>)>`.** O18's entry proposed the nested shape and
graded itself **XL** on the strength of it, because nesting changes the payload a client walks from
one `ArchivedVec<Archived<T>>` into a vec of vecs and rewrites every caller. The flat shape carries
the same information and leaves all sixty-eight of them alone. It was **M**. The alternative was
already written down on [F2](projections.md)'s own *Alternatives rejected* section — "carrying
per-partition row counts on the wire" — which is where it was found.

**`RowRef` as a newtype, not `#[rkyv(with = Map<Inline>)]` on a mirror struct.** The `with`
attribute works and produces a structurally identical archived struct. It produces a *distinct
type*, though, so byte identity could only ever be asserted empirically, an unused-lifetime
parameter on the generated archived struct has to be worked around, and a field reordered on
either shape breaks it silently. Promoting `Inline` to a real type instead means one generic family
and one theorem.

**The sealer is handed down, not the borrowed reply handed up.** The derive generates a
`fn seal(Response<RowRef<'_, X>>) -> Result<AlignedVec<16>, _>` per variant and passes it into
`PersistentTable::handle`. Returning a lifetime-carrying reply upward instead would keep
`self.tables` borrowed across `Shard::reply(&mut self, ..)`, which does not compile and should not:
the borrow ends where the scan does, and the serialize has to happen inside it.

**`RowSink` rather than a homogeneity rule.** A get may name one partition that is resident and
another that is still an archive. The sink records each row as either `Resident(&'a P)` or
`Built(usize)` into its own scratch space, so such a get borrows the half it can. Refusing the
borrowing path unless *every* named partition is resident would have been simpler and would have
given up the common case, because a partition read from disk stays an archive and a long-lived
table is a mixture. The scratch space is indexed rather than referenced so that growing it cannot
dangle an earlier pointer.

**`ShoalProjection::IDENTITY` is a constant the compiler checks.** It is
`Option<fn(&Self::Row) -> &Self>`, and only the derive's impl on the row itself can set it, because
`Self::Row = Self` is what makes `|row| row` type-check at all. A projection that tried would not
compile. It defaults to `None`, and the default earned its keep immediately: the hand-written
stand-in in the partition tests did not set it and went on copying rows until it was given one.

**`Shard::reply` is written in terms of `reply_sealed`.** There is one path to the client relay
rather than two, and `exec_done`/`replied` bracket the same serialize wherever it runs — so, unlike
[F26](archive-routed-requests.md)'s `decode`, **a stage capture is directly comparable across this
change.** Worth saying out loud, because a reader who has just read F26's page will assume it is
not.

**The implementation that hashed is kept, `#[cfg(test)]`.** It is the only definition of correct
the new reorder has, and every arrival order of the shares is checked against it. The same
discipline F26 applied to `split_by_shard`.

## Alternatives rejected

**Serializing the archived rows in place too.** The full form of O2. rkyv has no `Serialize` for an
archived value back into its own layout — no `impl Archive for ArchivedString`, none for
`ArchivedVec`, none for any derived archived type; only rkyv's own `rend` scalars. The parts to
write a mirror exist one level deep (`ArchivedString::serialize_from_str`,
`ArchivedVec::serialize_from_slice`), but `shoal-derive` sees only the syntax of a field's type and
cannot look inside a `Vec<Tag>` whose `Tag` lives in another crate. Filed as
[O40](../appendix/optimizations.md#o40-a-row-read-out-of-an-archive-is-materialized-before-it-is-re-serialized).

**Making a share travel as bytes.** Would extend the borrowing path to split gets. A share has to
be *merged* on the shard collecting it, and bytes cannot be merged without being read back, which
is the copy the change exists to remove.

**Answering a parked get in place on its final pass.** A get that parked holds what it found across
executions, in `PendingGets`, and those rows are owned by definition — that is what being parkable
means. Borrowing only the last partition's rows would mean a `GetRows` with two kinds of row in it,
which `RowSink` in fact supports; the reason not to is that the parked path is already waiting on a
disk read, so the copy it pays is not what is costing it.

**A `Vec<RowRef>` that is not allocated at all.** `ArchivedVec::serialize_from_iter` needs
`ExactSizeIterator + Clone` because it walks its input twice, and a `BTreeMap` range is `Clone` but
not `ExactSizeIterator`. Collecting pointers first is eight bytes per row against the
`size_of::<P>()` per row it replaces. A cloneable counted iterator over the sink would remove even
that, and is not worth writing today.

## Limitations

**The sorted table does not reach the borrowing path at all, and this is the finding that matters
most.** `SortedPartition::check_disk` starts `true` and is only cleared by a partition arriving
from a read. When `block_on_load` asks storage and is told there is nothing on disk, it returns
without recording that answer — so a sorted partition that has only ever been written to is judged
possibly-non-resident for ever, and refused the borrowing path correctly but permanently. Filed as
[item 80](../appendix/known-issues.md#80-a-sorted-partition-that-was-never-on-disk-asks-storage-about-it-on-every-get).
**The unsorted table does take it**, ten times over its integration suite. This was found by
putting a probe on the path and watching it never fire, not by reasoning — both paths answer
identically, so nothing failed and no test would have caught it.

**A row read out of an archive is still materialized.** [O40](../appendix/optimizations.md), above.

**A share of a split get is still copied once per share.**

**A projection is built, never borrowed.** `IDENTITY` is `Some` only for the whole row. A
projection is a strict subset of its row and copies less than the row would have, but it copies.

**A scalar-only row loses rkyv's `memcpy` on the borrowing path.** `Vec<T>` reaches
`serialize_from_slice`, which copies a padding-free type in one go; `Vec<RowRef<'_, T>>` cannot
take that branch. The bytes are identical — that is asserted, and asserted *together with* the fact
that the two branches really are different, so the comparison is not a tautology — but the encode
may be slower for a row with no heap fields and no padding.

**The group index costs sixteen bytes per partition on every get**, including the single-partition
case that never needed one.

**Nothing here has been captured.** See [Performance](#performance).

## Invariants to uphold

**The two response enums must keep the same variants in the same order.** rkyv writes a variant's
position as its discriminant, so a variant added to `#DbResponseKinds` and not to
`#DbResponseKindsRef` silently renames every variant after it, and a client reads a get as an
insert. They are generated from one list, and three tests archive every variant of both and compare
the bytes.

**`RowRef`'s archived type must stay `<T as Archive>::Archived`.** Not a type that looks like it.
Everything else here — that the borrowing path is byte-compatible, that `retrieve` reads back what
the server wrote, that no wire version moves for it — is downstream of that one line.

**`rows` is laid out in `groups` order, and every row is covered exactly once.** The client reads
the rows *through* the index and cannot check this for itself. `GetRows::is_consistent` states it
and the tests assert it after every operation that touches either half.

**Only `can_answer_in_place` may send a get to `get_sealed`.** It is the reason nothing in that
function can park, block, or take `&mut self`. Its three refusals — a share, a parked get, a
partition that may need a disk read — are each about the same thing: the rows would have to outlive
the scan that found them.

**A sealed answer never carries a failure.** A failure reaches a query only when a read it parked
on gave up, and a query that has parked is never answered in place. The two are exclusive by
construction, and `apply_failure` asserts it rather than assuming it.

**`exec_done` is stamped before the seal and `replied` after it, wherever the seal runs.** This is
what keeps a stage capture comparable across this change, and it is the thing that would rot first.

## Performance

**Captured as `f27-grouped-responses`** — micro layer only, 218 benchmarks, on a clean tree with
the `performance` governor. The macro half is a separate step and has not been taken; see the last
prediction below for why it may say nothing.

One of these numbers contradicts the entry it was built for, which is the more useful half of the
capture.

**The reorder: the win shrinks as the partition count rises, which is the opposite of what O18
expected.** `wire_codec/response/gather/{hash,groups}`, 1024 rows spread over a sweeping number of
partitions:

| Partitions | rows each | `hash` | `groups` | |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1024 | 15.97 µs | 18.3 ns | early return — a single run has no order to fix |
| 4 | 256 | 15.95 µs | 1.03 µs | **×15.5** |
| 16 | 64 | 15.96 µs | 1.65 µs | ×9.7 |
| 64 | 16 | 16.38 µs | 2.70 µs | ×6.1 |
| 256 | 4 | 18.01 µs | 15.70 µs | **×1.15** |

O18's entry said what would show it was "a response of many rows drawn from many partitions against
one drawn from a few", which reads as a prediction that the win grows with the partition count. It
falls. The reason is visible once stated: ranking groups instead of rows only helps while there are
many fewer groups than rows, and at 256 partitions of 4 rows the two counts have nearly converged.
What is left at that end is not the ranking at all but `order_by`'s per-run bookkeeping — it splits
the rows into one `Vec` per group and concatenates them back, so 256 groups is 256 allocations.
Filed as [O41](../appendix/optimizations.md#o41-reordering-a-gathered-get-allocates-a-vec-per-partition).

**So the entry was right that the axis matters and wrong about which end of it pays.** A fan-out
get over many single-row partitions — which is what `macro/fanout/n` drives — gains almost nothing
here. A get over a few partitions holding many rows each gains an order of magnitude.

**Building the reply: ×8.6 at a thousand rows.** `wire_codec/response/build/{owned,borrowed}`, where
each arm includes the step in front of the serialize, because timing the serialize alone would
compare the two shapes at the one thing they do identically:

| Rows | `owned` (clone, then serialize) | `borrowed` (point, then serialize) | |
| ---: | ---: | ---: | ---: |
| 16 | 751 ns | 202 ns | ×3.7 |
| 256 | 14.49 µs | 1.83 µs | ×7.9 |
| 1024 | 57.54 µs | 6.73 µs | **×8.6** |
| 4096 | 230.9 µs | 25.74 µs | ×9.0 |

**The ratio grows with the row count and then stops**, which is what says the win is the clone
rather than anything about the serialize: the clone is O(rows) and so is the serialize, so past a
few hundred rows the two scale together and the ratio settles just under ×9.

**And the scan itself, which is the number this feature is actually about.** Against the trailing
capture, the resident partition scans:

| Benchmark | before | after | |
| --- | ---: | ---: | ---: |
| `partition_sorted/get_all/4096` | 210.66 µs | 7.55 µs | **−96.4%** |
| `partition_sorted/get_all/1024` | 52.17 µs | 1.89 µs | −96.4% |
| `partition_sorted/get_all/256` | 12.87 µs | 0.57 µs | −95.6% |
| `partition_sorted/get_range_64/1024` | 3.05 µs | 0.19 µs | −93.9% |
| `partition_sorted/get_all/16` | 536.07 ns | 74.99 ns | −86.0% |
| `partition_sorted/get_key/1024` | 35.22 ns | 28.79 ns | −18.3% |

A scan that no longer clones the rows it returns costs a twenty-eighth of what it did at 4096
rows. The floor it is approaching is the filter and the walk, which is all that is left once the
copy is gone — which is also why the single-key gets move by 18% rather than by 96%: one row's
clone against the seek that found it is a much smaller share.

**The same capture showed the archived scans rising 5–14%**, and chasing that is where the more
useful lesson is.

The first reading was structural and plausible: `RowSink::push_built` wrote to two vectors where
the old code wrote to one, on the one path that gains nothing, since an archive holds no row to
point at and every row there is built. That was fixed — the index is not written until a row is
actually pointed at — and re-measured as `f27-row-sink`. It cleared `get_all/16` and
`get_range_64/*`, which fell back inside the noise band.

**It did not clear `archived/walk_all/16` or `maybe_loaded/get_key/*`, and `walk_all` is why that
matters.** That benchmark builds a plain `Vec` and calls `access`, `live_row_values` and
`from_archived` — **it touches nothing this feature changed.** It reports +9.3% in the first
capture and +9.4% in the second. So roughly nine points of that band is not this change at all; it
is the distance between today's machine and whenever the trailing capture was taken, on
benchmarks of a few hundred nanoseconds where the declared band is already ±9%.

Read against that control, the archived arms are:

| Benchmark | uses the sink? | f27-grouped-responses | f27-row-sink |
| --- | --- | ---: | ---: |
| `archived/walk_all/16` | **no** | +9.3% | +9.4% |
| `maybe_loaded/get_all/16` | yes | +10.0% | within noise |
| `maybe_loaded/get_range_64/16` | yes | +14.0% | within noise |
| `maybe_loaded/get_key/256` | yes | +11.3% | +11.4% |
| `maybe_loaded/get_key/1024` | yes | +9.8% | **+22.9%** |

The two that moved are the two the fix was aimed at. `get_key` did not move, sits about two points
above a control that should be flat, and at 1024 rows went *further* out between two captures of
almost identical code — 124.89 ns to 139.79 ns. A 15 ns swing on a 115 ns benchmark is not
something this pair of captures can attribute, and saying which of code, layout and machine owns it
would need a repeat capture at one commit. **Filed rather than explained**, in
[TODOs](../appendix/todos.md).

Both captures are kept. `f27-grouped-responses` is the one that shows the regression the fix was
for, and a fix with no *before* is an assertion.

The predictions written down before the capture, which stand:

- `execute` falls on the read arms of the width sweep — the `P::from_row` clone is gone — and
  **`reply_serialize` does not move**, because the same bytes are still written. A reader expecting
  the ×238 stage to fall will misread the capture.
- ~~The `wire_codec/response/encode/{owned,borrowed}` pair is **within noise**.~~ Wrong, and wrong
  because the prediction was about a benchmark that was not built: the pair that exists is
  `build/{owned,borrowed}` and each arm includes the clone or the pointing in front of the
  serialize, which is the comparison that means something. The serialize halves really are
  identical work; the arms differ by what happens before them, and that is ×9.
- **The macro layer may not move at all**, and this is the claim most likely to fail. The grid's
  read arms are sorted-table arms, and the sorted table does not reach the borrowing path — see
  the first limitation. Until item 80 is fixed, the capture that would show this feature working is
  a capture of the unsorted table.

Nothing under `shoal-bench/src/workloads/` changed, `shoal.yml` is untouched, no workload
identifier moved, and `STAGE_NAMES` is unchanged — so every existing capture still joins for
`compare`.

**One thing was fixed in the plumbing rather than the code.** `docs/perf/sources.json` did not list
`shoal-proto/src/shared/responses.rs` in the micro layer, although `wire_codec/response/*` archives
and reads back exactly that file. A capture would have been reported *unaffected* by the change
that rewrote the thing it measures. That is the same drift [Resolved #78](../appendix/resolved/sources-manifest-drift.md)
fixed, arriving the other way round — not a listed path that stopped existing, but a measured file
that was never listed — and the test guarding that file cannot see it, because it only checks that
the paths named there resolve.

## Tests

| Test | Breaks if |
| --- | --- |
| `a_borrowed_row_is_byte_identical_to_an_owned_one` | `RowRef` stops archiving as the row it points at, over a `String` row, a `Vec` row and a scalar-only row |
| `the_two_serializers_the_identity_test_compares_are_different_ones` | rkyv stops copy-optimizing the scalar row, which would make the test above compare one code path against itself |
| `a_borrowed_row_reads_back_as_the_row_itself` | the bytes stop being reachable through the row's own archived type |
| `every_response_kinds_variant_is_byte_identical_to_its_ref_mirror` | the two generated enums drift in variants or in order |
| `an_answer_with_no_rows_is_byte_identical_through_either_enum` | the same, for the five answers that carry no rows |
| `a_borrowed_reply_reads_back_through_the_owned_enum` | a client can no longer read what a borrowing shard wrote |
| `a_resident_unprojected_scan_copies_no_rows` | a resident get copies a row it could have pointed at — O2's claim, stated as a count |
| `an_archived_scan_builds_every_row_it_returns` | an archived row is answered in place, which rkyv has no way to do |
| `groups_cover_every_row_exactly_once` | a merge, a limit or a reorder leaves a gap or an overlap in the index |
| `the_groups_a_get_returns_name_its_partitions_in_the_order_it_asked_for` | the index is built in arrival order rather than named order |
| `a_gathered_get_orders_its_rows_without_hashing_any_of_them` | the grouped reorder disagrees with the hashing one, over six arrival orders of four shares |
| `a_limit_trims_the_group_index_with_the_rows_it_trims` | a limit trims one half and not the other |
| `wire_codec/response/gather/{hash,groups}` | not a test — the benchmark [O18](../appendix/optimizations.md) never had, and the arm that found [O41](../appendix/optimizations.md) |
| `a_repeated_get_answers_the_same_rows_and_names_their_partition` | a get answered twice against the same rows answers differently |

Four were confirmed by breaking the code under them: dropping the index trim fails two on
`is_consistent`, leaving the groups unsorted fails the oracle comparison, and reversing the mirror
enum's variant order fails all three byte-identity tests.

The four existing ordering tests — `get_partition_order_is_stable_across_repeats`,
`get_partition_order_survives_disk_loads`, and `projection_orders_rows_across_partitions` in both
suites — pass **unchanged**, which is what says the reorder still does what it did.

## Related

- [O2](../appendix/optimizations.md#o2-every-returned-row-is-copied-at-least-twice) — half closed,
  with the remainder filed rather than buried
- [O18](../appendix/optimizations.md#o18-the-gathered-reorder-rehashes-every-rows-partition-key) and
  [O36](../appendix/optimizations.md#o36-every-get-re-collects-its-rows-into-a-fresh-vec-even-when-it-read-one-partition)
  — closed
- [O40](../appendix/optimizations.md#o40-a-row-read-out-of-an-archive-is-materialized-before-it-is-re-serialized)
  — the archived half, and why it is not effort but rkyv
- [Item 80](../appendix/known-issues.md#80-a-sorted-partition-that-was-never-on-disk-asks-storage-about-it-on-every-get)
  — why the sorted table does not reach any of this yet
- [Resolved #20](../appendix/resolved/orphaned-sources.md) — `response.rs`, kept for years as this
  entry's prior art, opened at last and found to be a stub
- [F26](archive-routed-requests.md) — the request half, and the pattern this follows
- [F2](projections.md) — whose partition-key limitation this lifts, and whose rejected alternative
  turned out to be the right design
- [O41](../appendix/optimizations.md#o41-reordering-a-gathered-get-allocates-a-vec-per-partition)
  — found by the benchmark this built, and the reason O18's win falls away at high partition counts
- [F10](framing-and-protocol-evolution.md) — the version byte and fingerprint that make the flag
  day a refused connection rather than undefined behaviour
