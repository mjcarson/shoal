# F4. Archives are validated once, not once per read

An evicted partition is read where it lies rather than deserialized, which is the point of holding
it as an archive. Until this, every query that touched one re-ran rkyv's validator over the whole
buffer first — so a get naming one row of a 4,096-row partition paid for 4,096 rows, and paid again
on the next get.

## Context

Filed as [O3](../appendix/optimizations.md), ranked **A1** — the top of that page's priority queue —
and measured by O23, which was the only entry on the page a benchmark already settled:

| Partition rows | Access, then project one row | `access` alone |
| --- | --- | --- |
| 16 | 148.0 ns | 117.7 ns |
| 256 | 1.912 µs | 1.890 µs |
| 1,024 | 7.611 µs | 7.558 µs |
| 4,096 | 30.43 µs | 29.89 µs |

29.89 µs of a 30.43 µs cold single-row get was validation. The resident equivalent was 51.3 ns — a
factor of 590 — and the difference was almost entirely one call.

`RkyvSupport::access` (`shared/traits.rs`) is rkyv's *checked* entry point: `rkyv::access` runs
`bytecheck` over the whole buffer, O(bytes), before it will hand back a reference into it, and the
`#[instrument]` on it adds a span creation on top. `MaybeLoaded::Accessible` held the raw
`ReadResult` off disk, so nothing remembered that a buffer had already been through that.

**The obstacle was never the cost, it was where the answer has to live.** `rkyv::access` returns a
reference *into* the buffer, so holding "the validated form" means holding a reference next to the
thing it borrows from, which is self-referential and not a thing a Rust struct can be. The entry
originally claimed the narrow form could be had "without a single `unsafe`". That is not true, and
it is worth saying plainly rather than quietly not doing it.

## What it does

**A partition's bytes are validated when the read that produced them lands, and not again.**

```rust
pub struct ValidatedArchive<P, B = ReadResult> {
    raw: B,
    len: usize,
    root_pos: usize,
    kind: PhantomData<fn() -> P>,
}
```

`server/tables/partitions.rs`. `new` is the only constructor and it validates; `archived` then reads
without validating. `MaybeLoaded::Accessible` holds one of these instead of a bare buffer, so the
type carries the fact that validation happened rather than each reader having to redo it.

**Four sites construct one**, and they are exactly the places a partition arrives from disk:
`PersistentSortedTable::load_partition`, `PersistentUnsortedTable::load_partition` (twice, for the
vacant and the stale-accessible entry), and `FileSystem::load_scanned` on the recovery path.

**Thirteen sites consumed one.** Every `access(read).unwrap()` on a `MaybeLoaded::Accessible` arm —
the sorted and unsorted gets, `exists`, `is_tombstoned`, the insert/delete/update paths that have to
deserialize an archive to modify it, and the three intent-replay arms — is now `read.archived()`.
None of them can panic any more, because none of them can fail.

**A sort key sought in an archive is validated once per query too.** `SeekBytes` (the carrier
[F1](sort-key-ranges.md) introduced for O19) held raw `AlignedVec`s and `seek_archived` validated one
per key **per partition**; it now holds `ValidatedArchive<S, AlignedVec>` and validates in
`SeekBytes::new`. That is *k* validations for a query naming *k* keys across *p* partitions, against
*k × p* before.

**A corrupt archive now fails the read that produced it.** `load_partition` on both table types
returns `Result<Option<..>, ServerError>` where it returned `Option<..>`, and the generated caller in
`shoal-derive` propagates it. Previously a corrupt archive panicked at the first query to touch it —
and on every query after that.

**The archived read path became reachable from a test.** `MaybeLoaded` takes its buffer as a type
parameter, defaulted to `ReadResult`. That is not cosmetic: glommio's `ReadResult` constructors are
private to glommio, so before this **the entire `Accessible` arm could not be constructed outside a
running server**, and neither the tests nor the benchmarks reached a line of it. Eleven of the
fourteen tests below exist only because of that parameter.

## Design choices

**The validated form is a fact, not a reference.** The struct holds bytes and the knowledge that
those bytes passed validation. That is the only shape that fits: a reference cannot be stored beside
its own buffer, and re-deriving the reference on each read is what `access_pos_unchecked` does in
constant time.

**`root_pos` is computed at construction rather than per read.** `rkyv::access_unchecked` derives it
from `bytes.len()` every call. Pinning it means the buffer's length plays no part in the safety
argument at all — a root position derived once, from the length validation actually saw, cannot
drift from what was validated.

**`StableBytes` is an `unsafe` trait, and that is the point.** The buffer type is a public type
parameter, and the unchecked read is correct only if every deref yields the same pointer and length.
Without the trait, a downstream `Deref` impl returning a different slice on the second call would
break `archived()` **without writing a single `unsafe` itself**. The obligation belongs on whoever
adds a buffer type, so it is written as a safety contract rather than as a comment.

**The `#[instrument]` on `RkyvSupport::access` stays.** It now fires once per partition load, which
is where a span belongs. Removing it is a separate and much smaller decision.

**Validation is eager, at load, rather than lazy at first use.** A partition is only ever read
because a query is waiting on it, so nothing is validated that would not have been; and eager
validation is what lets the failure be reported by the code path that owns the error.

## Alternatives rejected

**`yoke` or another self-referential container.** Holding `Yoke<&'static Archived<P>, ReadResult>`
puts the `unsafe` in a library instead of in this file — but it needs an `unsafe impl StableDeref for
ReadResult` *in this crate* anyway, plus a new dependency and a considerably harder type. It moves
the same obligation somewhere less visible and charges a dependency for it.

**`access_unchecked` everywhere, with no validation at all.** This is the form
[O3](../appendix/optimizations.md) called `Major`, and it is the one that depends on
[archive checksums](../appendix/todos.md#archive-checksums). It is **not what this does** and that
dependency is still unpaid. Validating once keeps every guarantee the old code had: the same bytes
are checked by the same validator, just at the read rather than at each query. Dropping validation
outright would mean trusting bytes read back after a crash, which is exactly when they are least
trustworthy.

**A `#[cfg]`-swapped inner buffer type instead of a generic parameter.** Zero cost in production and
no generic to thread — but the test build would then exercise a *different type* than production
does, which is the one thing a test of this code must not do.

**A public `ReadResult` constructor in the glommio fork.** `ReadResultInner` holds a
`ScheduledSource`, which needs a live reactor; a `from_vec` variant means a new arm in its `Deref`.
An invasive change to a vendored dependency, to buy a test affordance a type parameter buys for free.

**Keeping `load_partition`'s `Option` and panicking earlier.** `expect("corrupt archive")` would have
been behaviour-preserving and avoided touching the derive macro. Rejected because the derive edit is
one character and a typed error at the read beats a panic at an arbitrary later query.

**Dropping a partition that fails validation and releasing its queries.** This answers a get with "no
rows" for a partition that exists and is corrupt — a loud failure converted into silent data loss.

## Limitations

**Validation still costs what it always cost; it is paid in a different place.** A cold partition is
still O(bytes) to take on. What changed is that the cost is per *read from disk* rather than per
*query*, so a partition read once and queried once is no faster. Everything here is a win on the
second query onwards, and on any query that touches a partition another query already loaded.

**Startup got slower in proportion.** `load_scanned` validates every partition an intent log named,
at recovery, rather than at the first query to touch it. Those partitions were about to be replayed
into anyway, so this is close to free — but it is not free, and it is on the startup path.

**A corrupt archive still takes the shard down**, now at load rather than at query time. The queries
blocked on that partition are never released either way. That is a pre-existing gap rather than
something this introduced, and it is filed as
[item 51](../appendix/known-issues.md#51-a-partition-that-fails-to-load-never-releases-the-queries-blocked-on-it).

**`SeekBytes::new` got slower** by one validation per key, because it now validates what it just
serialized. That is the trade: it is paid once per query instead of once per key per partition. A
query naming one key against one cold partition breaks exactly even.

**The unchecked read is only as good as `StableBytes`.** Two impls exist and both are correct. A
third, added carelessly, is unsoundness with no `unsafe` at the call site.

## Invariants to uphold

This is the section to read before changing `ValidatedArchive`.

1. **`new` must remain the only constructor.** Every guarantee here reduces to "this value exists,
   therefore these bytes were validated". A second way to build one — a `From`, a struct literal
   reachable from outside the module, a `#[cfg(test)]` shortcut — voids it. This is also where
   **alignment** is checked: `access_pos_unchecked` only `debug_assert!`s alignment, so in a release
   build a misaligned buffer that skipped `new` is silent undefined behaviour.
2. **`raw` must never be exposed by reference, and never by `&mut`.** The bytes read must be the bytes
   validated. `len()` answers from the recorded length rather than from the buffer for the same
   reason.
3. **Any new `StableBytes` impl must return the same base pointer and the same length from every
   deref, and the bytes behind it must never change.** `ReadResult` qualifies because its pointer and
   length are immutable fields read through `&self`; `AlignedVec` qualifies because it only moves its
   buffer through `&mut self`, which a `ValidatedArchive` never hands out.
4. **`root_pos` must keep coming from the length validation saw.** It is the position
   `rkyv::access` itself computed. Recomputing it from a later length would reintroduce exactly the
   drift storing it removes.
5. **`archived_is_the_same_reference_access_returns` is the test that holds the whole argument
   together.** It asserts that `archived()` returns the same pointer the checked `access` does. If
   rkyv ever changes where a root sits, that test fails rather than the read silently returning
   nonsense. Do not delete it, and do not weaken it to a value comparison.
6. **Nothing may be validated *less* often than once.** The narrow form is what keeps a corrupt
   archive from becoming a bad pointer. Turning `new` into a no-op is the `Major` change this
   deliberately did not make, and it needs [archive checksums](../appendix/todos.md#archive-checksums)
   first.
7. **`MaybeLoaded`'s buffer parameter must stay defaulted.** Production never names it. If it stops
   being defaulted, the eight `MaybeLoaded<..>` signatures that rely on the default all have to
   change and the parameter stops being free.

## Performance

Measured by `scripts/bench.sh`, `o3-before` against `o3-after`, on the hardware in
[Performance Baseline](../operations/performance-baseline.md). Both captures were taken in the same
session under the same settings, so the pair is the result; comparisons against the frozen `B1`
baseline carry an environment difference as well.

**The prediction was written down before the run**, so that the result could disprove it: a cold
keyed get should fall by about what `codec/access` costs. At 4,096 rows that predicted ~27.8 µs. It
fell by 28.6 µs — within 3% of the prediction.

**The percentages are not the finding. The flatness is.** `partition_sorted/maybe_loaded/*`, mean of
the sampled distribution, in nanoseconds:

| Benchmark | 16 rows | 256 | 1,024 | 4,096 |
| --- | --- | --- | --- | --- |
| `get_key` before | 218 | 1,852 | 7,339 | 28,764 |
| `get_key` **after** | **106** | **114** | **119** | **138** |
| `exists_key` before | 197 | 1,902 | 7,554 | 29,048 |
| `exists_key` **after** | **80** | **93** | **92** | **103** |
| `get_range_64` before | 663 | 5,400 | 10,656 | 32,778 |
| `get_range_64` **after** | **537** | **3,435** | **3,487** | **3,531** |
| `get_all` before | 712 | 15,655 | 63,179 | 252,904 |
| `get_all` after | 598 | 14,022 | 56,656 | 227,905 |

A cold get that names one row used to cost the size of the partition it landed in. It now costs a
`log n` descent: 106 ns to 138 ns across a 256-fold increase in partition size, against 218 ns to
28,764 ns before. `get_range_64` is flat at ~3.5 µs the way its resident twin is flat at ~3.06 µs,
which is [F1](sort-key-ranges.md)'s claim finally holding on the archived path as well as the
resident one.

**`get_all` is the honest one.** −10% to −11%, because a walk of every row amortises the validation
it used to pay and the deserialization dominates. This entry never claimed a full scan was the
problem, and the numbers agree.

**What the cost is now, and where.** `maybe_loaded/validate_once` measures `ValidatedArchive::new`
alone — 132 ns / 2,026 ns / 7,953 ns / 31,852 ns across the four sizes, which is `codec/access` plus
the constructor. **The work did not get cheaper; it stopped repeating.** Paid once per read from
disk instead of once per query against that read.

**`SeekBytes::new` got slower, as designed**: 54.9 → 75.2 ns for one key, 103.7 → 120.2 ns for a
range, 3,050 → 3,670 ns for sixty-four keys. It validates what it serialized, once per query, so
that `seek_archived` does not validate once per key *per partition*. A query naming one key against
one cold partition breaks even; everything wider wins.

**End to end: no detectable change**, and this is worth being blunt about. The `tmdb` macro
benchmark moved 1,786.0 ms → 1,809.8 ms and 1,800.3 ms on the repeat: +1.3% and +0.8% against a
run-to-run spread of 3.0–9.0% within a single capture of five runs. The reason
is not that the win is small — it is that **the workload never reaches this code**. The `hotpath`
capture is unambiguous: `SortedPartition::get` fires **once** and `fs::load_partition` fires
**once** across 447,251 inserts and 99,999 gets, and `ValidatedArchive::new` does not appear in the
profile at all because nothing was ever evicted and re-read. The macro layer cannot adjudicate this
change, and no reading of it should be offered as if it could.

### The controls moved, and it is not this change

`codec/access` and `archived/access_and_one_row` were carried unchanged specifically so that they
would not move. Against `o3-before` they moved **+9% to +13%**, outside the noise band, and the
repeat capture put them within 0.7% of the first — so it reproduces, and it is a property of the
build rather than of the moment.

**The third baseline is what settles it.** Against frozen `B1`, the *after* build's
`codec/access/{256, 1024, 4096}` is **+3.5% to +4.1% — inside the band**. It was `o3-before` that
was the outlier, measuring `codec/access/4096` at 27,835 ns where B1 had 29,891 ns and both after
runs had ~31,030 ns. Nothing here touches `RkyvSupport::access`, which is what those two benchmarks
call directly, and `archived/walk_all` — same function — moved −4% in the *opposite* direction at
the same time.

The likeliest mechanism is the one
[Performance Baseline](../operations/performance-baseline.md#what-the-micro-layer-can-actually-resolve)
names as invisible to a confidence interval: the bench binary changed shape, and the buffer those
two walk landed at a different offset. A validator walking 30 µs of one `AlignedVec` is exactly what
that would show up on.

**It does not affect the conclusion**, and the reason is arithmetic rather than judgement: the effect
measured here is 99.5%, two orders of magnitude larger than a 13% drift. Taking the controls at face
value and deflating the before numbers by the full 13%, a 28,764 ns get was really 25,400 ns — and
still became 138 ns.

Recorded rather than explained away, and filed as
[O24](../appendix/optimizations.md#o24-two-benchmarks-move-with-the-shape-of-the-binary-around-them).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `new_rejects_a_truncated_archive` | Validation stopped running. Truncation rather than a flipped byte, because a flip usually lands in a payload the validator has no opinion about |
| `new_rejects_a_corrupt_root_pointer` | The root region goes unchecked — the corruption an unchecked read would follow straight into a bad pointer |
| `archived_is_the_same_reference_access_returns` | The unchecked read stopped agreeing with the checked one. See invariant 5 |
| `an_accessible_get_returns_every_row` | The `SortSelect::All` walk over an archive |
| `an_accessible_get_seeks_a_named_sort_key` | `seek_archived`, including the validated-key path |
| `an_accessible_get_bounds_a_range` | The archived range walk and its inverted-range guard |
| `an_accessible_get_shares_its_limit` | A limit spanning two archived partitions |
| `an_accessible_and_a_loaded_partition_agree` | The actual contract — an archived partition answering differently from a resident one holding the same rows |
| `an_accessible_exists_answers_for_a_named_key` | The `exists` arm over an archive |
| `a_projected_accessible_get_returns_the_projection` | `P::from_archived` out of a validated archive |
| `an_accessible_partition_reports_its_byte_size` | Memory accounting charging something other than the archive's bytes |
| `an_accessible_unsorted_get_returns_its_row` | The unsorted archived get |
| `an_accessible_unsorted_tombstone_is_tombstoned` | An archived tombstone read as a live row |
| `an_accessible_unsorted_update_deserializes` | An update against an archived unsorted partition |
| `partition_sorted/maybe_loaded/*` | Not a test — the benchmark group that adjudicates this, and the first one to reach `MaybeLoaded` at all |

## Related

- [O3 and O23](../appendix/optimizations.md) — what this was filed as, struck through and kept
- [F1](sort-key-ranges.md) — `SeekBytes`, whose "bytes in the carrier, references at the point of
  use" shape this completes
- [F3](performance-harness.md) — the harness that made this adjudicable
- [Performance Baseline](../operations/performance-baseline.md) — the numbers above, in context
- [archive checksums](../appendix/todos.md#archive-checksums) — still unpaid, and still the
  dependency of the `Major` form this did not take
