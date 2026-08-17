# Optimizations

Work that would make Shoal faster, indexed the same way as [Known Issues](known-issues.md): each
entry names what the code does, where, and why it costs. Item numbers are prefixed `O` and are
never reused.

~~**None of these are measured.** They come from reading the source, and they are ordered by the
size of the argument for them, not by observed benefit. Anything here should be confirmed against
a profile before it is acted on — [Benchmarking](../performance/benchmarking.md) and the `hotpath`
feature are what that is for.~~

That was true of every entry below when it was filed, and it is worth keeping because it says
what the entries *are*: arguments from the source, ordered by the strength of the argument
rather than by observed benefit. What has changed is that there is now somewhere to settle
them. [F3](../features/performance-harness.md) built a harness that can resolve a few percent,
[Performance Baseline](../performance/baseline.md) records what the system currently
does, and the `hotpath` feature — which until then was wired up in a way that produced an empty
profile — reported 57 scopes on a `tmdb` run, which was the workload the figure was taken
from before [F8](../features/purpose-built-workloads.md) retired it.

So the rule is now stronger rather than weaker. **An entry is not acted on until a benchmark
exists that would show the difference**, and the entry says which one. An entry with no such
benchmark is asking for the benchmark first. A change that removes work from a path nothing is
waiting on is a change that only adds risk, and a change that cannot be measured is
indistinguishable from one.

**The first entry settled this way was [O3 + O23](#o3-every-archived-read-is-fully-validated-inside-a-tracing-span)**,
and [F4](../features/validated-archives.md) is worth reading for what the rule cost and bought. It
cost an extra capture: the benchmark that would show the difference did not exist and could not be
written without part of the change, so the enabling half landed and was measured on its own first.
It bought a 99.5% result with a control, a null, and a confirming repeat behind it — and it caught a
[measurement defect](#o24-two-benchmarks-move-with-the-shape-of-the-binary-around-them) that would
otherwise have read as a regression the change caused.

Defects are in [Known Issues](known-issues.md); several entries below share a root cause with one
and say so. An entry that has been done is struck through and kept, with what replaced it, for the
same reason a resolved issue keeps its page.

**Every citation on this page was re-resolved against the tree in August 2026**
([Review](review-2026-08.md)) and most had drifted. Two things that sweep is worth knowing about:
the quoted *original* text of a struck-through entry keeps its **original** line numbers, which now
point at unrelated live code — O6's and O17's are called out where they appear — and O2's code
snippets were still showing the pre-[F2](../features/projections.md) lines that the paragraph below
them said had changed.

## How these are ranked

Every open entry carries a scorecard under its heading, and [the priority queue](#the-priority-queue)
orders all of them. Four axes, graded the same way everywhere.

**Impact** is graded by *what backs the claim*, not by how large the claim is. That distinction is
the whole point of the rule above, so it is a column rather than a caveat:

| Grade | Means |
| --- | --- |
| **Measured** | A micro-benchmark or a baseline figure, cited on the entry |
| **Profiled** | A `hotpath` scope. Attribution only — [never a result](../performance/baseline.md#profile--where-the-time-goes) — so a call count is worth more here than a duration |
| **Asymptotic** | The cost grows in something a caller controls, so it is established without a number |
| **Argued** | Source reading alone. The default, and the weakest |

**Difficulty**:

| Grade | Means |
| --- | --- |
| **S** | A local edit or a type annotation |
| **M** | Contained to one module |
| **L** | Crosses module boundaries, or changes an internal type that travels between them |
| **XL** | Reaches the wire format, the on-disk format, or the client |

**Tradeoff** is `None`, `Contained` (a local behaviour change, revertible), or `Major` (safety,
determinism, or a compatibility break). A `Major` entry is not a worse entry — O3 is the best one
here — but it is one that needs a decision rather than a patch.

**Depends on** and **Blocks** carry hard edges only. **A missing benchmark is a dependency**, since
this page forbids acting without one, and it is the most common one: five entries are blocked on a
benchmark rather than on any code.

## The priority queue

Tiered rather than a single 1-to-N ordering, because an entry backed by a measurement and an entry
backed by an argument are not comparable on one scale, and pretending otherwise would undo the rule
this page opens with. Ordered inside each tier.

**Tier A — the cost is established and the change is contained.** These are actionable now.

| # | Entry | Impact | Diff | Depends on | Tradeoff | Adjudicable today |
| --- | --- | --- | --- | --- | --- | --- |
| ~~**A1**~~ | ~~[**O3** + **O23**](#o3-every-archived-read-is-fully-validated-inside-a-tracing-span)~~ — **done**, by [F4](../features/validated-archives.md) | Measured — 29.89 µs of a 30.43 µs cold single-row get | M | — | Contained | it was, and it was |
| ~~**A2**~~ | ~~[**O17**](#o17-handle_flushed-runs-on-every-message)~~ — **done**, by [F5](../features/flushed-sweep-gate.md) | Profiled — 711,638 calls became 21,279 | S | — | Contained | it was, on the profile alone |
| **A3** | [**O13**](#o13-a-multi-partition-get-is-quadratic-in-the-partitions-it-names) (+ [**O12**](#o12-to_blocked-clones-the-whole-filter-set-per-blocked-partition) beside it) — the quadratic multi-partition get | Asymptotic — O(n²) in a caller-set n | S | — | None | no — still needs a bench over `PersistentSortedTable::get` |
| **A4** | [**O5**](#o5-the-hottest-maps-use-siphash), [**O14**](#o14-fixed-thousand-element-preallocations-on-per-call-paths), [**O28**](#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends) — hasher, allocation sizes, and a doubled map guard | Argued | S | — | None | no — needs a table-layer bench, and O28 needs the client measured at all |
| **A5** | [**O25**](#o25-two-instrument-spans-remain-on-per-query-paths) — two `#[instrument]` spans on per-query paths | Argued — but the cost is in the *uninstrumented* binary | S | — | Contained | no — needs a with/without capture |

**A3 is now the head of the queue**, and it is the first one there that a profile cannot settle: it
needs a benchmark over `PersistentSortedTable::get` that does not exist yet. A1 and A2 are struck
rather than deleted because what each got wrong is the useful part — A1 claimed the narrow form
needed no `unsafe`, and there is no such form; A2 accepted a rotation delay that turned out not to be
necessary, and would have quietly changed what four tests exercise if it had been. See
[O3](#o3-every-archived-read-is-fully-validated-inside-a-tracing-span) and
[O17](#o17-handle_flushed-runs-on-every-message).

**A5 is new and comes out of doing A2.** It is ranked last in the tier despite being the smallest
diff, because unlike everything else here its impact is argued rather than profiled — a span's cost
is invisible to the profile that would normally rank it, which is precisely what makes it worth
filing.

**Tier B — argued, contained, waiting on its benchmark.** The profile is what orders this tier:
`write_helper` is 30.5 ms per call against roughly 350 ns for the insert it persists, so a write-path
entry is removing work from a path that is [already waiting on the
device](../performance/baseline.md#profile--where-the-time-goes). O9 and O8 lead it
because they are the exception — their cost scales with data on disk rather than with request rate,
so they get worse by existing longer rather than under load.

| # | Entry | Impact | Diff | Depends on | Tradeoff | Adjudicable today |
| --- | --- | --- | --- | --- | --- | --- |
| **B1** | [**O9**](#o9-every-intent-log-rotation-walks-the-entire-on-disk-partition-set) → [**O8**](#o8-partitions-are-read-one-at-a-time-each-with-its-own-dup-and-close) (with [**O22**](#o22-recovery-loads-the-partitions-it-scanned-one-await-at-a-time) in the same change) | Argued — but O(data on disk) | M, then L | O9 before O8 | Contained | no |
| **B2** | [**O4**](#o4-deep_size_of-is-a-recursive-walk-called-on-every-mutation) — carry a row's measured size | Argued | M | — | Contained | no — and it settles [item 22](known-issues.md#22-size-accounting-inconsistencies) either way |
| **B3** | [**O10**](#o10-serializedmapsave-snapshots-by-cloning), [**O15**](#o15-one-partition-load-costs-a-dup-and-a-close), [**O21**](#o21-a-forced-rotation-of-an-empty-intent-log-does-the-whole-rotation-anyway) — contained cleanups | Argued | S–M | — | Contained | no |
| **B4** | [**O11**](#o11-a-fresh-alignedvec-per-write-and-per-response) — reuse the serialization buffer | Argued | M | a storage write-path bench | None | no |

**Tier C — blocked on a design pass, not on effort.**

| # | Entry | Impact | Diff | Depends on | Tradeoff | Adjudicable today |
| --- | --- | --- | --- | --- | --- | --- |
| **C1** | [**O1**](#o1-queries-are-fully-deserialized-on-arrival) — zero-copy the request half | Argued | L | a `wire_codec` bench; the `BytesMut` reaching the shard | Contained | no |
| **C2** | [**O2**](#o2-every-returned-row-is-copied-at-least-twice) + [**O18**](#o18-the-gathered-reorder-rehashes-every-rows-partition-key), together | Argued — the largest read-path win available | **XL** | each other; a `wire_codec` bench | **Major** — wire format and the client | no |
| **C3** | [**O30**](#o30-nothing-can-see-what-a-connection-costs-to-open) — the connect path is unmeasured | **Unknown, and that is the entry** | S for the workload, unknown for whatever it finds | a `connect` workload | — | **no, and that is the point** |
| **C4** | [**O31**](#o31-the-disjointness-rule-cannot-tell-a-result-from-a-saturated-workload) — a saturated workload passes the rule that decides what is real | **Measured** — four points report encryption making queries faster | S to detect, M to decide | nothing | Contained | **yes, it already has been** |

**Tier D — declined, kept with the reason.** A rejected optimization is recorded, not dropped.

| Entry | Why it is not in the queue |
| --- | --- |
| [**O20**](#o20-a-sort-key-get-reads-a-partition-it-may-not-need) | Makes the cost of a query depend on what happens to be resident, which turns a reproducible latency into a flaky one. Declined on determinism, not on difficulty |
| [**O22**](#o22-recovery-loads-the-partitions-it-scanned-one-await-at-a-time) standalone | Startup path, and the set is normally small. It rides along with O8 in **B1** or it does not happen |
| [**O16**](#o16-compaction-shares-the-shards-executor) | Not an actionable entry — it is a consequence of thread-per-core. Its effect on this page is that it **raises O8 and O9**, since it means their cost lands on query serving rather than in the background |

### Dependency edges

Rendered as a table rather than a graph: the book has no mermaid preprocessor, so a diagram would
come out as a code block.

| Edge | Why |
| --- | --- |
| ~~**O23 ⊂ O3**~~ | Not two entries. O23 was the measurement of O3, and they were taken as one — [F4](../features/validated-archives.md) |
| **O9 → O8** | O9 replaces the full scan with an incremental total. Doing O8 first means grouping reads inside a walk that O9 would have deleted |
| **O8 ↔ O22** | The same edit — group by archive file, issue concurrently — in the compactor and in recovery |
| **O2 ↔ O18** | Both change `ResponseAction::Get`. Landing either alone means paying the wire-format break twice |
| **O18 → F2** | A projection is [required to carry its table's partition key](../features/projections.md#limitations) only so that O18's rehash can work. Taking O18 lifts that |
| **O4 → item 22** | The mismatched size bases exist *because* the size is re-derived at each site. One edit closes both |
| **O16 raises O8, O9** | Compaction shares the query executor, so their cost is not background cost |
| **O3 → archive checksums** | Only for the *drop validation outright* form, which [F4](../features/validated-archives.md) did **not** take. Still unpaid: validation, now once per read rather than once per query, is still the only thing between a corrupt archive and a bad pointer |
| **`wire_codec` bench → O1, O2, O18** | Unbuilt ([TODOs](todos.md#benchmark-coverage-the-harness-does-not-have)) |
| **table-layer bench → O5, O12, O13** | Unbuilt, and until recently believed to exist — see below. [F4](../features/validated-archives.md) closed half the gap by making `MaybeLoaded` constructible, but all three of these live a layer above it in `PersistentSortedTable` |
| **write-path bench → O11, O21** | Unbuilt, and it is the layer that dominates the profile |

### Which entries a benchmark can currently adjudicate

| Entry | Benchmark that would show it |
| --- | --- |
| O1, O18, O19 | none yet — a `wire_codec` bench over `Queries` and `ResponseKinds` is unbuilt. It belongs in the **micro** layer, not the macro one ([TODOs](todos.md#benchmark-coverage-the-harness-does-not-have)) |
| O2 | `partition_sorted/maybe_loaded/get_all` and `archived/walk_all`, with `get_all` for the resident twin |
| ~~O3, O23~~ | `partition_sorted/maybe_loaded/get_key` and `exists_key`, against `codec/access` — **settled**, see [F4](../features/validated-archives.md#performance) |
| O5, O12 | **none yet** — an isolated bench over `PersistentSortedTable::get` is still unbuilt ([TODOs](todos.md#benchmark-coverage-the-harness-does-not-have)). `maybe_loaded/*` reaches `MaybeLoaded`, one layer below where both live |
| O13 | ~~none yet~~ `macro/fanout/{resident,evicted}/n` since [F8](../features/purpose-built-workloads.md) — **the question, not the isolated cost**. See the note below |
| O20 | none yet — a `routing` bench over `Ring::find_shard` and `split_by_shard` is unbuilt, and likewise belongs in the micro layer |
| O21 | `hotpath` `fs::commit` and `stream::prep` only; no micro-benchmark of the write path exists, though [F8](../features/purpose-built-workloads.md) built the standalone binary that would host one |
| O28, O30, O31 | none — **the client is not instrumented at all**. No `tracing` span, no `hotpath` scope, and no workload that isolates it. O30 is the only entry on this page whose cost is not even bounded by an argument |

> This table previously claimed that `partition_sorted/insert` and `get_key` adjudicated **O5**,
> and that `seek_bytes/*` adjudicated **O12** and **O13**. ~~It was wrong about all three.~~ Those
> benchmarks construct a `SortedPartition` directly (`shoal/benches/partitions.rs`) and never build
> a `PersistentSortedTable`, which is where all three entries live — the `partitions` and `blocked`
> maps, `to_blocked`, and the quadratic loop are in `persistent/sorted.rs` and
> `shared/queries/sorted.rs`, none of which those benches reach. `seek_bytes/new` measures
> `SeekBytes::new` and nothing around it. **The three cheapest entries on this page were the three
> whose evidence was furthest away**, and the table said the opposite.

> **What F8 changed, and what it did not.**
> [F8](../features/purpose-built-workloads.md) built `macro/fanout/{resident,evicted}/n` over
> *n* ∈ {1, 2, 4, 16, 64, 256} — a get over *n* partition keys against both a resident table and
> one that has to be read. That is the shape [TODOs](todos.md) asked for, and it needed no glommio
> executor inside criterion, because driving `PersistentSortedTable::get` through a live server
> does not need one.
>
> It adjudicates **O13's question** and not **O13's cost**. Every sample includes the wire, the
> routing, `split_by_shard`, the response merge and the client, so the absolute number is not a
> cost of the table method. But a quadratic per-partition term bends the curve against a flat
> control at *n* = 1 whatever constant overhead sits on top of it.
>
> **The first full capture does not settle it.** `f8-powersave` puts the median below the chord at
> both *n* = 16 and *n* = 64, which is the right sign, but the marginal cost between adjacent
> points — 0.91, 0.64, 1.11, 0.83, 1.12 µs — does not rise monotonically and is consistent with
> noise around a straight line. A smoke-scale run had suggested a clean rise off forty samples and
> did not survive contact with the full one, which is worth recording as a caution about reading
> smoke runs rather than as evidence about O13.
>
> **O5 and O12 are not touched**: neither is about how cost scales with the partition count, so
> neither shows up as a bend.
>
> The isolated, criterion-sampled bench remains unbuilt and remains the thing that would settle
> all three.

---

## Read path

### O1. Queries are fully deserialized on arrival

| | |
| --- | --- |
| **Rank** | **C1** — blocked on a design pass |
| **Impact** | Argued — every `String`, `Vec` and filter in a bundle, per request |
| **Difficulty** | L — the `BytesMut` has to survive as far as the shard that executes the query |
| **Depends on** | ~~A `wire_codec` bench~~ (built, [F10](../features/framing-and-protocol-evolution.md)); `ServerMsg::Query` giving up its owned `QueryKinds` |
| **Blocks** | nothing |
| **Tradeoff** | Contained — a lifetime on the query type, not a format change |
| **Benchmark** | `wire_codec/request/decode`, which runs the validated `access`, the unchecked `access_unchecked` and the full `deserialize` as three separate functions at 1, 10 and 100 queries per bundle — so the gap between the second and the third is what this entry is worth |

```rust
// load our arhived query from buffer
let archived = Queries::access(&data)?;
// deserialize our queries
let queries = <Queries<D::ClientType> as RkyvSupport>::deserialize(archived)?;
```

`shard.rs:633-635`

Every `String`, `Vec`, and filter in every query of the bundle is allocated and copied out of a
buffer that already holds them in a readable layout. This branch is named for making *responses*
zero-copy; the request half was not converted.

The machinery for it already exists and is unused: `ShoalDatabase::unarchive_queries`
(`shared/traits.rs:341-345`) returns `&ArchivedQueries` via `access_unchecked` and has no callers.

The obstacle is real, though, and worth stating: `send_to_shard` consumes the queries by value
(`shard.rs:512`) and `ServerMsg::Query` carries an owned `QueryKinds` (`messages.rs:148-153`), so
this is not a call-site swap. It needs the archived form to survive as far as the shard that
executes the query, which means the `BytesMut` has to travel with it.

**Sequencing, filed while writing [Direction](../direction/overview.md).** This is not a format
change and does not need [D2](../direction/framing.md) — but D2 *is* a format change, it rewrites
both read loops, and it is the point at which the server's `BytesMut::zeroed(len)` gets replaced
anyway. Taking this in the same pass costs one visit to that code instead of two, and the
`wire_codec` bench both are blocked on is the same bench.

### O2. Every returned row is copied at least twice

| | |
| --- | --- |
| **Rank** | **C2**, with O18 — the largest read-path win, and the largest change |
| **Impact** | Argued — two copies per returned row, on every get |
| **Difficulty** | **XL** — `ResponseAction::Get` reaches the wire format and the client |
| **Depends on** | O18, which changes the same shape; ~~a `wire_codec` bench~~ (built, [F10](../features/framing-and-protocol-evolution.md)) |
| **Blocks** | O18 |
| **Tradeoff** | **Major** — a wire-format break, and `FromShoal::retrieve`'s signature with it. **Cheaper than it was**: [F10](../features/framing-and-protocol-evolution.md) put a version byte and a schema fingerprint on the wire, so a format change is now a refused connection naming both sides rather than undefined behaviour |
| **Benchmark** | `partition_sorted/archived/walk_all` and `get_all` bound the copies; `wire_codec/response/encode` and `/decode` at 16, 256, 1024 and 4096 rows are the wire half, which used to be uncovered |

`SortedPartition::get` copies out of the `BTreeMap`:

```rust
found.push(P::from_row(row));
```

`tables/partitions.rs:585`, and `:293` for unsorted

The archive path is worse — it materializes an owned value from bytes per row
(`tables/partitions.rs:1092`, and `:363` for unsorted):

```rust
found.push(P::from_archived(archived));
```

Then `Shard::reply` serializes the whole `Vec<T>` back into bytes (`shard.rs:663`). A read served
from an `Accessible` partition therefore goes **bytes → owned rows → bytes**, and a read served
from memory goes **rows → cloned rows → bytes**.

The response type is what forces it: `ResponseAction::Get(Option<Vec<T>>)`
(`shared/responses.rs:31`) can only hold owned rows.

**Narrowed, not closed, by [F2](../features/projections.md).** The two lines above used to read
`found.push(row.clone())` and `let loaded = R::deserialize(row).unwrap(); found.push(loaded)`;
they are now `P::from_row` and `P::from_archived`, where `P` is what the get asked to be answered
with. A get that named a projection copies only the fields that projection declared, so the archive
path materializes a smaller owned value and the wire carries less. A get that named none still
copies the whole row twice — the identity projection is exactly the two lines above — so the shape
of this entry is unchanged and only its magnitude moved.

**A third entry now wants the same flag day.** This and O18 already had to land together to avoid
paying the wire break twice ([dependency edges](#dependency-edges)); [D2](../direction/framing.md)
is a third break, and the argument is identical. **The expensive part of a wire-format change is
the flag day, and it is paid per break rather than per field** — so if D2 is taken first and these
two later, the cost is two. Whether they can realistically be designed together is the open
question, since D2 is a header change and these are a payload change; but the sequencing decision
should be made deliberately rather than by whichever is picked up first.

### ~~O3. Every archived read is fully validated, inside a tracing span~~

**Done, in the narrow form**, by [F4](../features/validated-archives.md), together with
[O23](#o23-a-cold-get-of-one-row-validates-the-whole-partition-it-landed-in) as this page said it
had to be. A partition read off disk is validated once, when the read lands, and held as a
`ValidatedArchive` that carries that fact; the thirteen `access(read).unwrap()` sites on the
`MaybeLoaded::Accessible` arms became `read.archived()`. `SeekBytes` took the same treatment, so a
sort key is validated once per query rather than once per key per partition.

The original entry read:

> **Take the narrow form first.** Validating once when a partition is read off disk, and holding the
> validated form, removes the per-get walk without a single `unsafe` and without depending on
> checksums — and O23's measurement says that is where nearly all of the cost is. `access_unchecked`
> is a second, separable decision; the precedent for it already exists in the tree, at
> `shared/traits.rs` for queries and in `sorted.rs` for intent replay, both on data this process
> wrote moments earlier.
>
> ```rust
> #[instrument(name = "RkyvSupport::access", skip_all, err(Debug))]
> fn access(raw: &[u8]) -> Result<&<Self as Archive>::Archived, rkyv::rancor::Error> {
>     rkyv::access::<<Self as Archive>::Archived, rkyv::rancor::Error>(raw)
> }
> ```
>
> `shared/traits.rs:62-77`
>
> `rkyv::access` is the *checked* entry point: it runs `bytecheck` over the whole buffer, O(bytes),
> every call. The `#[instrument]` adds a span creation and enter on top of that.
>
> The bytes were written by this process and read back from a file it owns, so they are validated
> once per read from disk at best and once per *query* as it stands. Validating at load and using
> `access_unchecked` afterwards removes both the walk and the span from the read path — at the cost
> of making [archive checksums](todos.md#archive-checksums) matter more, since validation is
> currently the only thing standing between a corrupt archive and a bad pointer.
>
> This is the entry with the best ratio of cost removed to code changed.

It was right about the ranking, right that the narrow form was the one to take, and right that
`access_unchecked` everywhere is a separate decision — which is **still not taken**, and still
depends on [archive checksums](todos.md#archive-checksums).

**It was wrong about `unsafe`, and that is the part worth keeping.** ~~"without a single
`unsafe`"~~ — there is no such form. `rkyv::access` returns a reference *into* the buffer, so
holding "the validated form" means holding a reference beside the thing it borrows from, which is
self-referential. What is holdable is the *fact* that validation happened, and reading through that
fact is one `unsafe` block behind a private constructor. Every byte is still validated, exactly
once, by the same validator. See
[F4's invariants](../features/validated-archives.md#invariants-to-uphold) before touching it.

The `#[instrument]` was kept rather than removed: it now fires once per partition load, which is
where a span belongs.

### O5. The hottest maps use SipHash

| | |
| --- | --- |
| **Rank** | **A4** — near-free, do it whenever the surrounding code is open |
| **Impact** | Argued — one SipHash per query at minimum, more on a parked one |
| **Difficulty** | S — a type annotation |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none — the `partition_sorted/*` benches never build a `PersistentSortedTable` |

`partitions: HashMap<u64, MaybeLoaded<..>>` and `blocked: HashMap<u64, ..>`
(`.../persistent/sorted.rs:142`, `:167`, `:238`, `:257`; `.../persistent/unsorted.rs:95`, `:113`,
`:184`, `:201`) all use std's default hasher. `partitions` is looked up at least once per query.

Two more have joined them since this was filed, and both are keyed by `(Uuid, usize)` rather than
by a `u64` — sixteen bytes of SipHash instead of eight: `PendingGets::parked`
(`.../tables/persistent.rs:137`) and `pending_exists` (`.../persistent/sorted.rs:161`). They are
touched only by a query that parked on a disk read, which is the path that is already waiting, so
they are the less interesting half of the entry.

The LRU sitting beside them already uses `BuildHasherDefault<GxHasher>` (`shard.rs:320`, and `shared/traits.rs:307`),
and `gxhash` is already a dependency, so this is a type annotation rather than a change.

### O12. `to_blocked` clones the whole filter set per blocked partition

| | |
| --- | --- |
| **Rank** | **A3**, beside O13 — same function, same loop |
| **Impact** | Argued — one filter-set clone per cold partition named |
| **Difficulty** | S — an `Rc`/`Arc` around the filters, or a borrowed narrowed query |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none — needs the table-layer bench |

`SortedGet::to_blocked` (`shared/queries/sorted.rs:445`) calls `for_partitions` (`:422-443`), which
clones `sort_select` and `filters` into the narrowed query, and `get` calls it once for every
partition that has to be read from disk (`.../persistent/sorted.rs:617`). A get across 100 cold
partitions makes 100 copies of the same filters and the same selection, all of which are then held
in `blocked` until the loads land. A range clones two bounds rather than a key set, so it is the
cheaper of the two selections to copy — but the filters dominate either way.

`SortedExists::to_blocked` (`:516`, calling `:496-514`) and the unsorted twin
(`shared/queries/unsorted.rs:165`, `:207`) have the same shape, reached from
`.../persistent/sorted.rs:713` and `.../persistent/unsorted.rs:570`.

### O13. A multi-partition get is quadratic in the partitions it names

| | |
| --- | --- |
| **Rank** | **A3** — the best difficulty-to-argument ratio on the page |
| **Impact** | **Asymptotic** — O(n²) in *n*, the partition count a caller sets directly |
| **Difficulty** | S — a rank index on `PendingGet`, and a running count in `filled_before` |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | ~~none — needs the table-layer bench~~ `macro/fanout/{resident,evicted}/n` since [F8](../features/purpose-built-workloads.md), which shows the curve bend but not the isolated cost — see [the table above](#which-entries-a-benchmark-can-currently-adjudicate) |

**This entry was filed against code that has since moved, and it came out broader.** It used to
read — and, like every quoted original on this page, **its line numbers point at code that is no
longer there**:

> ### ~~O13. `blocked.retain(..)` runs inside the per-key loop~~
>
> `.../persistent/sorted.rs:424`, `:473`, `:630` — a linear scan of the blocked list per partition
> key, making a get over *n* keys O(n²). Small *n* today, but *n* is the number of partitions a
> single query names, which is the one thing a caller controls directly.

Two of those three sites are gone, and the surviving `blocked.retain` is not on the get path at
all. What is left, and what was found in its place:

| Where | Per key | Reached by |
| --- | --- | --- |
| `PendingGet::rank` — `keys.iter().position(..)` (`.../tables/persistent.rs:58`) | scan of every key the get named | **every** multi-partition get |
| `PendingGet::filled_before` — walks `slots[..rank]` (`:93`) | scan of every slot before this one | every multi-partition get **with a limit** |
| `blocked.retain(..)` (`.../persistent/sorted.rs:727`) | scan of the keys still outstanding | an `exists` replayed after a disk read |

So the quadratic term did not go away when [items 26 and 39](resolved/partition-order.md)
introduced slot-based gathering — it moved from the blocked list into `PendingGet`, and **widened
in the process**. The old shape cost O(n²) only on a get whose partitions were being read from
disk; `rank` is called once per key on the resident path too, so every multi-partition get pays it
now, and a limited get pays `filled_before` on top. That is worse than what was filed, on a path
that is not waiting for anything.

It is still small *n* today and still O(n²), and *n* is still the one quantity a caller controls
directly — which is the argument for fixing it while it is cheap. `PendingGet` already owns `keys`
and `slots` side by side, so a `HashMap<u64, usize>` built once in `new` answers `rank` in O(1),
and carrying a running row count answers `filled_before` the same way.

**Filed on the way:** the entry's original claim is now false as written, which is why the old text
is kept above rather than edited in place. Nothing here is a defect — a quadratic term in a small
*n* is a cost, not a bug — so it stays on this page rather than moving to
[Known Issues](known-issues.md).

### ~~O19. A wanted sort key is re-archived for every archived partition it is sought in~~

**Done**, by [F1](../features/sort-key-ranges.md). `MaybeLoaded::seek_archived` used to serialize
and validate the key it was looking for once per key per partition, so a get naming *k* sort keys
across *p* archived partitions did that *k × p* times for *k* distinct values.

`SeekBytes` (`.../tables/partitions.rs`) now owns the archived forms of a query's keys and bounds,
and `PersistentSortedTable::get`/`exists` build it **at most once per execution** — lazily, on the
first partition actually being read in place, so a get every one of whose partitions is resident
still builds none of it. Serialization is down from *k × p* to *k*; validation is still per
archived partition, because holding a `&Archived<Sort>` across the loop would need a
self-referential struct.

Kept here rather than deleted because the shape it settled on is the one an equivalent change
elsewhere should copy: bytes in the carrier, references built at the point of use.

### O20. A sort-key get reads a partition it may not need

| | |
| --- | --- |
| **Rank** | **Tier D — declined.** Not on difficulty; on determinism |
| **Impact** | Argued — saves a whole archive read, but only when the *entire* key set hits in memory |
| **Difficulty** | M |
| **Depends on** | a `routing` bench; the invariant in [item 8](resolved/sort-keys.md#invariants-to-uphold) |
| **Blocks** | nothing |
| **Tradeoff** | **Major** — makes query cost depend on residency, turning a reproducible latency into a flaky one |
| **Benchmark** | none — `routing` is unbuilt, and no bench varies residency |

An in-memory row or tombstone shadows whatever an archive holds for the same key
(`SortedPartition::merge_from_disk`), so a get whose named sort keys are *all* resolved in memory —
as live rows or as tombstones — could answer without reading the partition at all, even with
`check_disk` set. The read is currently unconditional, which is deliberate: see the invariant in
[item 8](resolved/sort-keys.md#invariants-to-uphold). Recorded here rather than lost, with two
warnings attached. It pays only when the *whole* key set hits, and it makes the cost of a query
depend on what happens to be resident, which is the kind of thing that turns a reproducible
latency into a flaky one.

**It does not extend to a range.** A key set can in principle be checked off; a range cannot,
because there is no way to know that the rows in memory are *all* of the rows in that span without
reading the archive that might hold more. Ranges made this optimization strictly narrower rather
than more attractive — see [F1](../features/sort-key-ranges.md#invariants-to-uphold).

---

### O18. The gathered reorder rehashes every row's partition key

| | |
| --- | --- |
| **Rank** | **C2**, with O2 — the same shape, so the same change |
| **Impact** | Argued — one gxhash per row of a split query |
| **Difficulty** | **XL** — a grouped share is a wire-format change |
| **Depends on** | O2; a `wire_codec` bench |
| **Blocks** | [F2](../features/projections.md#limitations) — a projection must carry its partition key only because of this |
| **Tradeoff** | **Major** — wire format, shared with O2 |
| **Benchmark** | none — `wire_codec` is unbuilt |

`ResponseAction::order_by_partitions` (`shared/responses.rs:109`) sorts the merged rows of a split
query by where their partition was named. A `Response` carries rows and nothing else, so the only
way to ask a row which partition it came from is to hash its partition key again:

```rust
rows.sort_by_cached_key(|row| ranks.get(&row.get_partition_key()).copied().unwrap_or(usize::MAX));
```

`sort_by_cached_key` keeps that to one hash per row rather than one per comparison, and for a
string partition key a gxhash over the field is cheap next to the row clone that already happened
to get here. Still, the information was known and thrown away: every shard produced its rows
grouped by partition already, in the right relative order.

Two ways out, both bigger than they look. A k-way merge over the shares by partition rank would
be `O(n)` with no hashing, but `merge` is called pairwise as shares arrive rather than once at
the end, so it means buffering the shares and merging them together. Alternatively a share could
carry its rows grouped — `Vec<(u64, Vec<T>)>` rather than `Vec<T>` — which removes the question
entirely, at the cost of a wire format change that lands on the same `ResponseAction::Get` shape
**O2** wants to change for a different reason. Worth doing with O2 rather than before it.

[F2](../features/projections.md) added a second argument for the grouped share. A projection has to
carry its table's partition key for no reason other than this rehash, which is a real constraint on
what a projection is allowed to leave out — a projection of a title alone is not expressible. Taking
this entry would lift that requirement as well as removing the hash.

---

### ~~O23. A cold get of one row validates the whole partition it landed in~~

**Done**, by [F4](../features/validated-archives.md), as one unit with
[O3](#o3-every-archived-read-is-fully-validated-inside-a-tracing-span). This was never a separate
entry — it was O3's measurement, and it is the reason O3 was taken before anything else on this
page.

The first entry here that was found by measurement rather than by reading, and it is worth keeping
for what the measurement said:

| Partition rows | Access, then project one row |
| --- | --- |
| 16 | 158 ns |
| 256 | 1.93 µs |
| 1,024 | 7.89 µs |
| 4,096 | 31.0 µs |

Linear in the partition, for a query whose answer is one row — the same shape
[F1](../features/sort-key-ranges.md) removed from the *resident* path, still present on the archived
one. `partition_sorted/get_range_64` is flat at ~3.03 µs across those same sizes.

The entry closed with a warning that was worth having and turned out to matter:

> Worth confirming against a realistic row shape before acting: the benchmark rows are small, so
> this measures validator overhead per row at close to its worst ratio.

That is still true, and it bounds what F4 claims. The rows here are two short strings, so the
validator does the most work it can per byte of payload. On a wide row the same partition is more
bytes of payload and fewer relative pointers to check, and the ratio moves. What does not move is
the *shape*: the cost was per query and is now per read.

**A benchmark that reached the real code did not exist when this was filed.**
`archived/access_and_one_row` mimicked `MaybeLoaded::Accessible` by hand because that variant could
not be constructed outside a running server. `partition_sorted/maybe_loaded/*` is the group that
actually calls it, and `archived/*` and `codec/*` are kept beside it as controls — they exercise
`RkyvSupport::access` directly, which F4 did not change, so if they move the machine moved.

## Write path

### O4. `deep_size_of()` is a recursive walk called on every mutation

| | |
| --- | --- |
| **Rank** | **B2** — the entry whose *correctness* value exceeds its performance value |
| **Impact** | Argued, and discounted — the write path is [waiting on the device](../performance/baseline.md#profile--where-the-time-goes), not on this |
| **Difficulty** | M — 13 call sites, but they all want the same thing |
| **Depends on** | nothing |
| **Blocks** | [item 22](known-issues.md#22-size-accounting-inconsistencies) — the same edit settles it |
| **Tradeoff** | Contained — a row's size becomes state that can go stale, which is the thing to test |
| **Benchmark** | none; `partition_sorted/insert` covers the partition but not the accounting |

Ranked above the other write-path entries despite the profile, because it is the only one that
buys a correctness fix with the same edit. Take it for item 22 and treat the cost removal as
change left over.

It measures the whole object graph, so its cost is proportional to the row, not constant. Call
sites on the write path:

| Where | Calls per operation |
| --- | --- |
| `SortedPartition::insert` (`tables/partitions.rs:481`, `:487`) | Two — the new row and the one it replaced |
| `SortedPartition::update` (`:809`, `:813`) | Two — before and after |
| `SortedPartition::remove` (`:708`), `tombstone` (`:731`) | One |
| `UnsortedPartition::new` (`:228`), `update` (`:313`) | One — and `update`'s is a walk of the whole partition, not of the row |
| `merge_from_disk` (`:774`) | Every live row in the merged result |

Carrying a row's measured size alongside it would make all of these O(1). It would also settle
[item 22](known-issues.md#22-size-accounting-inconsistencies) — the mismatched bases between
`UnsortedPartition::new` and `update` exist precisely because the size is re-derived at each site
instead of being owned by one.

### O11. A fresh `AlignedVec` per write and per response

| | |
| --- | --- |
| **Rank** | **B4** — last in its tier, because the profile says its path is already waiting |
| **Impact** | Argued — one allocation and one extra copy per write and per response |
| **Difficulty** | M — rkyv can serialize into a caller-supplied buffer |
| **Depends on** | a storage write-path bench, which is [the biggest gap in the harness](todos.md#benchmark-coverage-the-harness-does-not-have) |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none — the layer that dominates the profile is the one with no confidence interval |

- `FileSystem::commit` (`.../fs.rs:366`) allocates via `RkyvSupport::serialize`, then copies the
  bytes a second time into the DMA buffer (`.../fs.rs:385`).
- `Shard::reply` (`shard.rs:663`) allocates one per response.
- `Shoal::send` (`shoal-core/src/client.rs:211`) allocates one per bundle on the **client** side,
  which this entry never mentioned and which is on the same round trip.
- `write_map_intent!` (`.../fs/compactor.rs:85`) allocates one per archive entry written, and
  `write_partition` (`:305`) allocates one per partition.

rkyv can serialize into a caller-supplied buffer, so all three could reuse one. `commit` is the
interesting one, because the destination buffer it copies into is already there — `prep` hands
back a `&mut [u8]` sized for the record (`.../fs/stream.rs:655-672`).

### ~~O17. `handle_flushed` runs on every message~~

**Done**, by [F5](../features/flushed-sweep-gate.md). The shard sweeps its tables when a write has
landed or a log is due to rotate, and not otherwise: **711,638 calls became 21,279** over the same
workload, and the time in them fell from 1.344 s to 0.453 s summed across twelve shards.

The original entry read:

> | | |
> | --- | --- |
> | **Rank** | **A2** — the cheapest entry with evidence behind it |
> | **Impact** | **Profiled** — 705,886 calls, 1.8 µs each, 11% of wall clock |
> | **Difficulty** | S — a dirty flag, or gate it on `DataFlushed` having arrived |
> | **Depends on** | nothing |
> | **Blocks** | nothing |
> | **Tradeoff** | Contained — a compaction check is delayed by at most one message |
> | **Benchmark** | `hotpath` `shard::handle_flushed`; no micro-benchmark |
>
> **Why a `hotpath` number is enough here, when the page says it is attribution only.** The
> [caveat](../performance/baseline.md#profile--where-the-time-goes) is about *durations* —
> the instrumented binary perturbs them. The **call count** is not perturbed: 705,886 calls against
> 617,175 queries is a structural fact about the loop, and it is the part of this entry that matters.
> The 1.8 µs is the soft half of the claim.
>
> `shard.rs:844` calls it unconditionally each loop iteration — and `:852` again after the loop — and
> it reaches
> `tables.handle_flushed` → per-table `get_flushed` → `compact_if_needed`
> (`.../persistent/sorted.rs:1097-1116`).
>
> *(Those line numbers are the pre-F5 ones. The gate is now `shard.rs:980-983` and
> `get_flushed` is `.../persistent/sorted.rs:1172`.)* So every message pays a pass over every table, including
> every `DataFlushed` wakeup — of which there is one per completed write.

**It was right about the ranking and about the evidence, and wrong about the tradeoff.** The
"compaction check is delayed by at most one message" was accepted here and then not paid: rotation
is driven by bytes *accepted*, and the accepted byte count is two field reads away, so a synchronous
`compaction_due()` predicate keeps rotation firing on exactly the message it fired on before. That
mattered more than it looks — `build_pressured_config` forces rotation every few writes on purpose,
and a gate that let it drift would have changed what four eviction tests exercise while leaving them
green.

**It also missed half of its own cost.** The `#[instrument]` on `handle_flushed` created an INFO span
per call, and unlike everything else the profile attributes, *that* cost is in the uninstrumented
binary too. It is removed. The same reasoning applies to two more spans on hot paths and is filed as
[O25](#o25-two-instrument-spans-remain-on-per-query-paths).

---

## Recovery

### ~~O7. Startup reads the same archive once per update intent~~

**Done**, as a side effect of fixing [item 31](resolved/multi-log-recovery.md) rather than as an
optimization in its own right — the correctness fix and this wanted the same change.

`scan` used to be called once per intent record and, per call, allocate a set sized for a
thousand keys in order to hold at most one:

```rust
// build a set of partitions to load from disk
let mut to_load = HashSet::with_capacity(1000);
```

It then called `load_partition_direct` for that key, which opens, reads, and closes an archive.
Because the set was per record, nothing deduplicated across records: *N* update intents against
one partition cost *N* archive reads.

`scan` was split into a synchronous `scan_keys` that only names partition keys, and the loading
moved into `read_intents`, which unions the keys across **every** log and then loads each one
once. So the deduplication is now across all logs rather than merely across the records of one —
and doing it only per log was never an option, because loading after a replay is precisely what
item 31 was.

The set is allocated once per recovery instead of once per record.

Note in passing that recovery still holds every record's `ReadResult` in memory before replaying
any of them, and now does so for every log at once rather than one log at a time, so peak memory
went from the size of the largest log to the total size of all logs. That is a deliberate
trade — see [Recovery](../storage/recovery.md#limitations).

### O22. Recovery loads the partitions it scanned one await at a time

| | |
| --- | --- |
| **Rank** | **B1** as a passenger on O8; **Tier D** on its own |
| **Impact** | Argued — one serial await, one `dup` and one `close` per scanned partition |
| **Difficulty** | S once O8 exists — it is the same grouping applied to a second loop |
| **Depends on** | O8 |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none; startup is not measured at all |

`FileSystem::load_scanned` (`.../storage/fs.rs`) walks the key set the prescan built and awaits
one load per key:

```rust
for partition_key in to_load {
    if partitions.contains_key(&partition_key) { continue; }
    if let Some(partition_read) = self.load_partition_direct(partition_key).await? {
```

This is [O8](#o8-partitions-are-read-one-at-a-time-each-with-its-own-dup-and-close)'s shape on
the recovery path rather than the compaction one, and each iteration also pays
[O15](#o15-one-partition-load-costs-a-dup-and-a-close)'s `dup`/`close`. Neither of those covers
this loop, so it is filed separately — but it should be fixed in the same change as O8, since
grouping by archive file is the same work in both places.

What makes it newly worth filing is that [item 31](resolved/multi-log-recovery.md) is what made
it possible. Under the old `scan` the loads were interleaved with reading, one key at a time,
discovered as each record went past — there was no set to batch. The whole key set is now known
before a single load happens, which is exactly the precondition for grouping them by archive and
issuing them concurrently. The correctness fix handed this optimization its opening.

Worth taking together with the [item 22](known-issues.md#22-size-accounting-inconsistencies)
bullet about this same loop: it is where a partition enters the memory counter in archive bytes,
so whatever touches it next is already reading that line.

**Not taken**, because it is on the startup path rather than a hot one, and the set is normally
small — an interrupted compaction is rare and the active log usually names few partitions.

---

## Compaction

### O8. Partitions are read one at a time, each with its own `dup` and `close`

| | |
| --- | --- |
| **Rank** | **B1**, after O9 and carrying O22 |
| **Impact** | Argued — but O(partitions changed), and [O16](#o16-compaction-shares-the-shards-executor) means it lands on query serving |
| **Difficulty** | L — grouping by archive plus concurrent reads, in two loops and in recovery |
| **Depends on** | **O9 first** — otherwise the grouping is built inside a walk O9 deletes |
| **Blocks** | O22 |
| **Tradeoff** | Contained — concurrency against a `RefCell` borrow that is [already held across awaits](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor) |
| **Benchmark** | none; compaction is not measured at all |

Worth taking with [item 35](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor)
rather than around it — this is the loop that holds the borrow, and issuing the reads concurrently
is exactly the change that would turn that latent defect into a live one.

```rust
for partition in self.changes.keys() {
    if let Some(entry) = self.map.to_archive.borrow().get(partition) {
        let handle = self.map.get_archive(&entry.archive).await?;
        let read = handle.read_at(entry.offset, entry.size).await?;
```

`.../fs/compactor.rs:230-241`

Serially awaited, one read per partition, with no grouping by archive file and no coalescing of
entries that happen to be adjacent in the same archive. `get_archive` returns a `dup` of a cached
handle (`.../fs/map.rs:481-509`) and the caller closes it, so each read also costs a `dup`/`close`
pair. `compact_archives` (`:443-500`) has the same shape.

Grouping `changes` by `entry.archive` before reading would let one handle serve many reads, and
glommio's read APIs can issue them concurrently rather than one await at a time.

(This is also the loop with the borrow-across-await in
[item 35](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor).)

### O9. Every intent log rotation walks the entire on-disk partition set

| | |
| --- | --- |
| **Rank** | **B1** — the head of Tier B, and the entry that ages worst |
| **Impact** | Argued — but **O(total partitions on disk) per rotation**, regardless of how few changed |
| **Difficulty** | M — maintain a per-archive used-byte total in `set_partition` and `remove_partition` |
| **Depends on** | nothing |
| **Blocks** | O8, O21 |
| **Tradeoff** | Contained — an incremental total is state that can drift from the truth it summarises |
| **Benchmark** | none; compaction is not measured at all |

Ranked first in its tier because it is one of the two entries whose cost grows with how long the
database has existed rather than with how hard it is being used. Everything else on this page gets
worse under load; this gets worse while idle.

`compact_if_needed` queues a `CompactionJob::Archives` on every rotation
(`.../fs.rs:460-461`), and that job calls `sort_by_load` (`.../fs/map.rs:551-585`), which iterates
all of `to_archive` and **copies every `ArchiveEntry`** into a fresh
`HashMap<Uuid, Vec<ArchiveEntry>>`:

```rust
for (_, archive_entry) in self.to_archive.borrow().iter() {
    let entry: &mut usize = used_by.entry(archive_entry.archive).or_default();
    *entry += archive_entry.size;
    let entries_entry = sorted.entries.entry(archive_entry.archive).or_default();
    entries_entry.push(*archive_entry);
}
```

The cost is O(total partitions on disk) per rotation, regardless of how few of them changed.

`compact_archives` then opens **every** candidate archive with `DmaFile::open`
(`.../fs/compactor.rs:460`) — bypassing the handle cache in `loaded_archives` that
`get_archive` maintains — purely to call `file_size()`, and closes it again for the ones it skips
on the 50% utilization test (`:462-478`).

Maintaining a per-archive used-byte total incrementally in `set_partition` and `remove_partition`
would replace the whole scan, and archive sizes are already known to the writer.

### O10. `SerializedMap::save` snapshots by cloning

| | |
| --- | --- |
| **Rank** | **B3** — a contained cleanup |
| **Impact** | Argued — a full copy of the archive map per serialization |
| **Difficulty** | S — serialize from the borrow rather than from a copy of it |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — the clone is what keeps the borrow short, so removing it holds a `RefCell` open across serialization. Compare [item 35](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor) before doing it |
| **Benchmark** | none |

```rust
all_archives: map.all_archives.borrow().clone(),
to_archive: map.to_archive.borrow().clone(),
```

`.../fs/map.rs:205-206`, inside `SerializedMap::save` (`:202`) — a full copy of the archive map
before every serialization. Both `HashSet` and `HashMap` are also rebuilt at
`with_capacity(1000)` (`:194-195`), so the copy allocates for a thousand entries whether or not
there are that many.

### O16. Compaction shares the shard's executor

| | |
| --- | --- |
| **Rank** | **Tier D — not an actionable entry.** It is a consequence of thread-per-core |
| **Impact** | Argued — and it is what makes O8 and O9 foreground costs rather than background ones |
| **Difficulty** | n/a — changing it means giving up the design |
| **Depends on** | nothing |
| **Blocks** | nothing. It **raises** O8 and O9 |
| **Tradeoff** | The design itself: no cross-core locking, in exchange for compaction competing with queries |
| **Benchmark** | none |

Kept in the queue as a rank of its own because it changes how two other entries should be read, and
a reader who skips it will under-rate them.

The compactor and the loader are both spawned onto `medium_priority` on the same glommio executor
as the query loop (`.../fs.rs:164-167`, `:594`). A long `compact_archives` competes directly
with query serving, and the task queue's share (`Shares::Static(500)` against the high priority
queue's 1000, `shard.rs:356-366`) is the only lever over it. That is a deliberate design — it is
what thread-per-core buys — but it means O8 and O9 are not merely background costs.

Note also that `write_partition` iterates `self.loaded`, a `HashMap`
(`.../fs/compactor.rs:303`), so partitions land in the archive in hash order and reads of
related partitions get no locality from it.

### O21. A forced rotation of an empty intent log does the whole rotation anyway

| | |
| --- | --- |
| **Rank** | **B3** — but take only the cheap two thirds |
| **Impact** | Argued — startup cost only, per restart per shard, dominated by the O9 walk behind it |
| **Difficulty** | S for suppressing the `Archives` job; **L** for skipping the rotation itself |
| **Depends on** | O9 removes most of what makes this expensive |
| **Blocks** | nothing |
| **Tradeoff** | None for the cheap form. **Major** for the full form — the generation counter, `FlushProgress.rotated` and `MarkEvictable` are all keyed off the rotation happening |
| **Benchmark** | none; startup is not measured |

**The split is the whole entry.** Queue the `Archives` job only when a rotation had changes to
compact: S, no tradeoff, and it removes the expensive part. Suppressing the rotation is a separate,
much larger decision that touches generations, and it should not be bundled in.

`compact_if_needed` rotates on `force` without looking at whether the active log holds anything
(`.../fs.rs:435-475`), and startup always forces one
(`.../tables/persistent/sorted.rs:274`). A table nobody wrote to therefore pays, per restart per
shard, a rename, a fresh file for the new active log, a `CompactionJob::IntentLog` that reads a
zero length file, the `glommio::io::remove` that now deletes it
([item 14](resolved/empty-rotated-logs.md)), and a `CompactionJob::Archives` behind it — which is
[O9](#o9-every-intent-log-rotation-walks-the-entire-on-disk-partition-set)'s full walk of the
on-disk partition set, the expensive part by some margin.

Skipping the rotation when the log is empty is not a local change, which is why item 14 cleaned up
after the rotation instead of preventing it: the generation counter, `FlushProgress.rotated`, and
the `MarkEvictable` that advances a table's compacted generation are all keyed off the rotation
happening. Suppressing the `Archives` job alone — queue it only when a rotation had changes to
compact — is the cheap two thirds of this and does not touch generations at all.

Startup cost only, not per write, which is why it is here rather than in Known Issues.

---

## The harness itself

### O24. Two benchmarks move with the shape of the binary around them

| | |
| --- | --- |
| **Rank** | **Tier B**, unranked inside it — this is a measurement defect, not a cost in Shoal |
| **Impact** | **Measured** — `codec/access` and `archived/access_and_one_row` moved +9% to +13% across a change that does not touch the function they call |
| **Difficulty** | M — the fix is a benchmark harness question, not a Shoal one |
| **Depends on** | nothing |
| **Blocks** | nothing, but it **widens the noise band** for anything validated against those two ids |
| **Tradeoff** | None |
| **Benchmark** | itself |

Found while taking [F4](../features/validated-archives.md), which carried both of these ids forward
untouched precisely so that they would act as controls.

Both moved together, outside the ±5% band, and a repeat capture put them within 0.7% of the first —
so the shift is reproducible within a build and meaningless across builds. It is not the change that
was being measured: nothing in F4 touches `RkyvSupport::access`, and `archived/walk_all`, which calls
the same function, moved −4% in the opposite direction at the same time.

**Having a third baseline is what identified which run was wrong**, and it is the argument for
keeping `B1` frozen. The *post*-change build sits +3.5% to +4.1% from B1 — inside the band. It was
the *pre*-change capture that was the outlier, at 27,835 ns against B1's 29,891 ns for
`codec/access/4096`. With only a trailing baseline the drift would have looked like a regression the
change caused.

What the two that moved have in common is that they are dominated by a linear walk over one
`AlignedVec` built at group setup. `AlignedVec` guarantees 16-byte alignment and nothing about where
the buffer lands relative to a cache set or a page, so adding a benchmark group ahead of them in the
binary changes what they are walking over. This is the between-process variation
[Performance Baseline](../performance/baseline.md#what-the-micro-layer-can-actually-resolve)
says a confidence interval cannot see, caught in the act.

**Fix direction:** neither obvious nor free. Allocating the buffer at a known page offset would pin
it, at the cost of measuring an alignment production does not have — a `ReadResult` off a DMA read is
page aligned, an `AlignedVec` is not, so the honest fix may be to make the benchmark buffers match
the DMA case rather than to pin them arbitrarily. Until then, treat a movement in
`codec/access` or `archived/access_and_one_row` of under ~15% as saying nothing.

### O25. Two `#[instrument]` spans remain on per-query paths

| | |
| --- | --- |
| **Rank** | **A5** — last in Tier A, because it is the only entry there a profile cannot rank |
| **Impact** | Argued — 617,175 INFO spans each, per run, in the **uninstrumented** binary |
| **Difficulty** | S — delete an attribute, or set `level = "trace"` |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — it is observability, and `reply`'s span is a real parent |
| **Benchmark** | none — and the point of this entry is that the obvious one does not exist |

Filed while taking [F5](../features/flushed-sweep-gate.md), which removed a third one.

`Shard::handle_query` (`shard.rs:688-693`) and `Shard::reply` (`:652`) both carry `#[instrument]`, both default to
`INFO`, and the subscriber is a `Registry` with a `fmt` layer filtered at `Info` (`server/trace.rs`)
against a `shoal.yml` that sets `level: Info`. So both callsites are enabled: each call allocates a
span in the registry's slab, enters, exits and closes it. At 617,175 calls apiece that is 1.2 million
span lifecycles per run.

**This is the entry the profile is structurally unable to rank**, which is why it is filed rather
than taken. `hotpath` attributes time to *its own* scopes; a span inside a scope is counted as part
of that scope's duration and never appears as a row. Worse, the cost is present in the baseline
binary and absent from nothing — so unlike every other entry on this page, there is no capture in
`docs/perf/` that contains the number. Settling it needs a purpose-built pair of runs with the
attributes present and absent, which is a capture nobody has taken.

**The two are not equivalent, and should not be decided together.** `reply`'s span uses
`parent = &span` to attach a reply to the query that caused it, so it carries the one piece of trace
structure this path has; deleting it flattens the trace. `handle_query`'s is a plain wrapper around a
function that is already the top of its own `hotpath` scope. If only one goes, it is `handle_query`'s.

**Fix direction:** `level = "trace"` on both is the conservative form — the callsite survives for a
debugging session and costs a cached interest check rather than a slab insert. F5 removed its span
outright instead, but that one had no fields, no children, and wrapped a function that usually did
nothing. Neither of these is that.

## Routing and memory

### ~~O6. The ring is a 1000×N `BTreeMap` answering a question arithmetic would answer~~ — done

**Taken, with [items 11, 12 and 37](resolved/tablet-ring.md).** The ring was replaced by a tablet
map: `find_shard` is now a shift and two indexed loads into a 4096-entry `Vec<u16>` — 8 KiB,
against a 16,000-entry `BTreeMap` — and there is no search at all.

The original entry read — **its line numbers describe code that no longer exists**, and `ring.rs`
today is the tablet map (`Ring::new` at `:72`, `find_shard` at `:140`, `TABLET_BITS` at `:25`):

> `ring.rs:26-44` builds it, `ring.rs:51-68` searches it, and `find_shard` runs once per partition
> key per query. At 16 shards that is a 16,000-entry `BTreeMap` — pointer-chasing, one allocation
> per node — consulted on the hot path.
>
> As built it does not need to be a search structure at all. Every shard uses the same fixed stride
> `RING_JUMP`, so the ring is exactly periodic and the owning shard is computable directly; that is
> the same property that makes the vnodes useless in item 12. Once item 12 is fixed and positions
> become independent, a search is needed again — but a sorted `Vec<(u64, usize)>` with
> `partition_point` is still strictly better than a `BTreeMap` for a structure that is built once
> and then only read.
>
> The two items should be done together: fixing item 12 without touching this doubles down on the
> structure that costs the most.

It was right that the two had to move together, and right that arithmetic could answer the
question — but it framed the choice as *periodic ring, so compute* versus *independent positions,
so search*. Tablets are neither: an explicit assignment table is a third option that is both O(1)
*and* evenly balanced, which is the combination the entry assumed was unavailable. The reason to
prefer it is not speed, though — it is that a stored assignment can be **moved**, which a computed
one cannot, and that is what a distributed Shoal needs.

Still unmeasured, as everything on this page is. The array is small enough to stay cache resident
where the `BTreeMap` was not, but that is an argument, not a profile.

### O14. Fixed thousand-element preallocations on per-call paths

| | |
| --- | --- |
| **Rank** | **A4** — near-free, do it whenever the surrounding code is open |
| **Impact** | Argued — a 1,000-element allocation to hold a handful of entries |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | none |

- `evict_data` allocates a `Vec::with_capacity(1000)` per table it touches, to hold however many
  victims that table has (`shard.rs:861`), plus a `HashMap::with_capacity(10)` per call (`:852`) —
  and it is called on **every message** while a shard is over its memory limit, including when it
  can free nothing at all ([item 59](known-issues.md#59-a-shard-that-cannot-free-anything-keeps-trying-on-every-message-in-silence)).
- `write_partition` allocates `to_mark` at 1000 per call (`.../fs/compactor.rs:299`).
- `SerializedMap::save` rebuilds both halves of the map at 1000 per serialization
  (`.../fs/map.rs:194-195`), which is [O10](#o10-serializedmapsave-snapshots-by-cloning)'s clone
  seen from the allocation side.
- ~~The per-record `HashSet` in [O7](#o7-startup-reads-the-same-archive-once-per-update-intent).~~
  Gone — it is allocated once per recovery now.

### O15. One partition load costs a `dup` and a `close`

| | |
| --- | --- |
| **Rank** | **B3** — a contained cleanup, with a second half that is not one |
| **Impact** | Argued — two syscalls per partition read |
| **Difficulty** | S for the `dup`/`close`; M for evicting from `loaded_archives` |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — borrowing the cached handle means the cache's lifetime now bounds the read's |
| **Benchmark** | none |

**The second paragraph of this entry is not an optimization.** A file descriptor held per archive
for the life of the process is a resource leak with a hard ceiling behind it, and it gets worse as
a table accumulates archives. It is filed here because it was found here, but it should be read as
a defect that has not been filed as one — the reason it has not is that no `EMFILE` has been
observed, so it is an argument rather than a symptom.

It is no longer only an argument about the future, though. `EMFILE` is the failure the loader's
retry classification exists for: an archive that cannot be opened is the one error class worth
attempting again, precisely because the descriptor another read is holding may come back
([Resolved #16, 51](resolved/partition-load-failure.md#the-fix)). Doing this optimization would
narrow what that retry is for.

`read_partition_helper` closes the handle the map just handed it (`.../fs/loader.rs:92-94`), even
though `ArchiveMap` caches open handles in `loaded_archives` specifically so it does not have to
reopen (`.../fs/map.rs:338`, `:481-509`). Borrowing the cached handle rather than duplicating it
would remove both syscalls from every partition read.

**The retry loop multiplies it.** A read is now attempted up to `MAX_LOAD_ATTEMPTS` times
(`.../fs/loader.rs:25`, three), so a `Retryable` failure pays the `dup`/`close` pair once per
attempt. That is the right behaviour and it is worth noticing here, because the descriptor
shortage the retry exists to ride out is the one this entry's second paragraph is about — the
retry is treating a symptom that borrowing the cached handle would reduce the incidence of.

The cache has the opposite problem at the other end: nothing evicts from `loaded_archives` except
`remove_archive` (`.../fs/map.rs:538-548`) and the shutdown drain (`:615`), so a table with many
archives holds a file descriptor per archive for the life of the process. It is preallocated for a
thousand of them (`:382`), which is the shape of the expectation.

### O27. An ephemeral write makes a mixed database sweep every table

| | |
| --- | --- |
| **Rank** | **Tier B**, last — the cost only exists in a database that mixes table kinds |
| **Impact** | Argued — one extra walk of every table per shard loop iteration that had an ephemeral write in it |
| **Difficulty** | S |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None, but the fix has to preserve [F5](../features/flushed-sweep-gate.md)'s gate exactly |
| **Benchmark** | none — needs a workload over a schema holding both kinds, which no workload does |

`ShoalDatabase::compaction_due` is generated as an OR across every table
(`shoal-derive/src/traits/db.rs`), and the shard sweeps when
`data_flushed || tables.compaction_due()` ([F5](../features/flushed-sweep-gate.md)). An ephemeral
table answers `true` from the moment a row is inserted until the next sweep releases its response,
which is correct and necessary — it is the only thing that wakes the shard to answer that insert
([F9](../features/ephemeral-tables.md#design-choices)). But the sweep it asks for is a sweep of
*every* table, so a persistent table sharing the database has `get_flushed` called on it — a
`compact_if_needed`, a pending-response scan — for a wakeup that had nothing to do with it.

An all-ephemeral or all-persistent database pays nothing: in the first case every table genuinely
had something to release, and in the second nothing changed. The cost is exactly the mixed case,
and it scales with how many persistent tables share the database with a busy ephemeral one.

The shape of the fix is a sweep that asks each table rather than the database — `handle_flushed`
already visits every field, so the gate could move to the same place as the visit instead of
sitting above it. That is a change to F5's mechanism, which is why it is filed rather than taken:
F5 exists because that sweep used to run unconditionally, and the way to get this wrong is to
reintroduce that.

**No benchmark would show it today.** Every workload drives one table. A workload over a schema
holding both kinds is the thing to build first, and it is worth having for its own sake — a mixed
database is the shape a real use of ephemeral tables has.

---

## The client

### O28. The client takes two guards on its response map for every query it sends

| | |
| --- | --- |
| **Rank** | **A4**, beside O5 and O14 — near-free, and on a path whose cost nobody has measured |
| **Impact** | Argued — two `papaya` guard acquisitions per query where one would do, plus one owned guard per response |
| **Difficulty** | S — a single `pin()` held across the check and the insert |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None |
| **Benchmark** | `macro/transport/send_one/small`, and it is **adjudicable now** — ~~`client.rs` has no `tracing` spans and no `hotpath` scopes at all~~, it has both since [F16](../features/client-builder.md) |

`Shoal::track_response` registers a query's response channel by asking whether an id is taken and
then inserting under it:

```rust
if self.channel_map.pin().get(&*query_id).is_none() {
    // insert this id
    self.channel_map.pin().insert(*query_id, tx.clone());
```

`shoal-core/src/client.rs:195-197`

`channel_map` is a `papaya::HashMap`, where `pin()` acquires a guard into the collector's epoch.
Two calls means two guards for one logical operation, and the pair is not atomic either — which
does not matter here, since a single-threaded caller cannot race itself for an id it just
generated, but does mean the two-call shape is buying nothing.

The other side pays a heavier one: `TcpProxy` uses `pin_owned()` once per response arriving
(`:536`), and an owned guard is the variant that allocates rather than borrowing the caller's.

**Why this is filed at all, given how small it is.** It is on the one layer of the system that has
no instrumentation whatsoever. `docs/src/appendix/todos.md` records that "`client.rs` has neither
`tracing` spans nor `hotpath` scopes, so the share of measured latency that is the harness's own is
unknown" — every macro number in
[Benchmark Results](../performance/overview.md) includes this code and none of them can
attribute anything to it. That makes a client-side entry worth *recording* even when it is too
small to act on, because the total it belongs to has never been bounded.

**Established by reading the source**, during the [August 2026 review](review-2026-08.md).

**Fix direction:** hold one guard — `let map = self.channel_map.pin();` — across the check and the
insert. `papaya` also has an `entry`-shaped API that expresses "insert if absent" in one operation,
which is what this loop actually wants. ~~Neither should be taken before
`transport/{send_one,send_batched,stream,stream_unordered}`
([TODOs](todos.md#what-f8-left-undone)) exists, which is the workload that would give the client
half a number at all.~~ **That workload exists** ([F13](../features/transport-workloads.md)), so
this entry is adjudicable for the first time — and the arm to adjudicate it on is
`macro/transport/send_one/small`, where a per-query cost is not buried under the bytes. It stays
open because nothing has been measured, not because nothing can be.

~~**That workload was blocking more than this entry, and half of that is now unblocked.**~~
**Both halves have landed.** The [Direction](../direction/overview.md) chapter is nine design pages
about the client, and its step 0 — before any of them — is exactly what this entry asks for: spans
and `hotpath` scopes in `client.rs`, plus the `transport/*` workloads
([D6](../direction/connection-pool.md#how-it-would-be-measured)). The workloads landed with
[F13](../features/transport-workloads.md) and the instrumentation with
[F16](../features/client-builder.md), which put a `hotpath` scope on `track_response` itself. **Step
0 is done and this entry is adjudicable for the first time since it was filed.**

**F16 deliberately did not take the fix**, and the reason is a rule worth reusing: it landed the
instrumentation and measured that, so that the capture carrying the instrumentation's cost is not
also the capture carrying this fix's benefit. Two changes in one capture is one number nobody can
attribute. The fix is still one guard instead of two, and it is still small; what it now has is a
before and an after that mean something.

**One thing to know before measuring it.** F16's own capture found no result in 144 metrics across
sixteen `transport` workloads, at spreads of a few percent on the `small` arms. A pair of `papaya`
guard acquisitions is nanoseconds against a wall clock of ~60 µs per query, so this is very likely
below what the macro layer can see at all, and the honest place to adjudicate it may be a
`hotpath` capture over the `client::track_response` scope rather than a `transport` wall clock.

## The wire

### O29. A request body is zeroed and then immediately overwritten

| | |
| --- | --- |
| **Rank** | **B4** — free bytes on every request, behind a shape change that is not free |
| **Impact** | Argued — one `memset` of the whole bundle per request, discarded on the next line |
| **Difficulty** | M — `ServerMsg::Client` has to stop carrying an owned `BytesMut` |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | Contained — a shape change inside the server, no format change |
| **Benchmark** | none — `wire_codec` measures the codec, not the relay's allocation |

```rust
// allocate a buffer that is exactly the right size
let mut data = BytesMut::zeroed(header.body_len());
// wait for messages from our client
if let Err(error) = tcp_rx.read_exact(&mut data).await {
```

`shard.rs`, `client_rx_relay`

`zeroed` writes the whole buffer and `read_exact` overwrites every byte of it on the next line. At
a hundred queries a bundle that is tens of kibibytes of `memset` per request, for nothing.

[Item 34](resolved/unvalidated-length-prefix.md) named this alongside the unbounded allocation, and
[F10](../features/framing-and-protocol-evolution.md) fixed the allocation and left the zeroing. It
is now *bounded* waste, which is the part that item was actually about.

The reason it was left is worth stating, because the fix looks like a one-line swap for
`BytesMut::with_capacity` plus `unsafe { set_len }` and is not: `ServerMsg::Client` carries the
`BytesMut` by value across a channel into `handle_client`, so the buffer's initialization state
becomes a property of a message type that several call sites construct. Doing this with `MaybeUninit`
or an `unsafe` `set_len` needs the read that fills it to be the only way that message can be built,
which `server/messages.rs` does not currently guarantee.

---

## ~~Suggested order~~ — superseded by [the priority queue](#the-priority-queue)

Kept because it was right about more than it was wrong about, and because what it left out is the
clearest argument for why the queue above exists. It read:

> 1. **O3**, then **O1** — both remove work from every read, neither changes an on-disk or wire
>    format. O3 is the smaller change and the larger win; do it first, and read
>    [archive checksums](todos.md#archive-checksums) before dropping validation.
> 2. **O4** — retires a real correctness wart
>    ([item 22](known-issues.md#22-size-accounting-inconsistencies)) with the same edit that removes
>    the cost, which makes it the easiest one to justify.
> 3. **O9**, then **O8** — the only entries whose cost scales with total data on disk rather than
>    with request rate. Everything else gets worse under load; these get worse just by existing
>    longer.
> 4. **O5** and **O14** — near-free, and worth doing whenever the surrounding code is open.
>
> **O6** has been taken, together with items 11, 12 and 37 as this list said it had to be.
>
> **O2** is deliberately not on this list. It is the largest single win available on the read path
> and also the largest change, because it needs `ResponseAction::Get` to hold something other than
> `Vec<T>`, which reaches the wire format and the client. It is worth its own design pass rather
> than a slot in an ordering.

**What it got right, and the queue keeps.** O3 first. O9 before O8. O4 justified by the correctness
fix rather than by the cost. O5 and O14 as near-free. O2 as a design pass rather than a queue slot —
the queue puts it in Tier C for exactly the stated reason, and only adds that O18 has to go with it.

**What it got wrong.** It paired **O1 with O3**, on the grounds that both remove work from every
read. That grouping does not survive the evidence: O3 is the one entry a benchmark already settles,
and O1 is one of five that no benchmark can currently see at all. They belong two tiers apart, and
the thing that separates them is not size but whether the claim can be checked.

### O30. Nothing can see what a connection costs to open

| | |
| --- | --- |
| **Rank** | **C3** — not actionable, because there is nothing to act on yet |
| **Impact** | **Unknown.** Every other entry on this page is at least argued from the source; this one cannot be, because the quantity is a wall clock and no clock is started |
| **Difficulty** | S to build the workload. Unknown for whatever it then shows |
| **Depends on** | a `connect` workload in `shoal-bench`. The *instrumentation* half is done ([F16](../features/client-builder.md) put a `hotpath` scope and a span on `ShoalConnectionManager::connect_to`), so what is missing is now only the workload |
| **Blocks** | any judgement about [F12](../features/authentication.md)'s cost, and about [D4](../direction/encryption.md)'s and [D6](../direction/connection-pool.md)'s |
| **Tradeoff** | — |
| **Benchmark** | the missing one *is* the entry |

Every macro number in [Benchmark Results](../performance/overview.md) is measured against an
already-warm pool. `Shoal::new` runs before the timer starts, so the ten connections it opens, the
ten handshakes they exchange, and — since [F12](../features/authentication.md) — the ten SCRAM
exchanges and twenty PBKDF2 derivations that go with them, are all invisible to every capture this
repository has taken.

That was defensible while a connection was a `TcpStream::connect` and a `set_nodelay`. It is
becoming less so with each thing added in front of the first query:

| Change | What it added per connection |
| --- | --- |
| [F10](../features/framing-and-protocol-evolution.md) | one round trip, two 24 byte frames |
| [F12](../features/authentication.md) | two more round trips and a PBKDF2 derivation on each end, when a config asks for it |
| [D4](../direction/encryption.md) | a TLS handshake, when it exists |
| [D6](../direction/connection-pool.md) | whatever a real health check costs on a connection that is being created |
| [F16](../features/client-builder.md) | nothing per connection — but a client with several endpoints may now try, and be refused by, more than one before it opens one |

**What is needed is not a query workload.** The `transport/*` workloads
~~[TODOs](todos.md#benchmark-coverage-the-harness-does-not-have) plans~~ —
**built** ([F13](../features/transport-workloads.md)) — still measure a warm
pool, because that is what they are for, so this entry is no more adjudicable than it was. This
wants time to first successful query from a cold
client, with `min_idle` as a parameter, run against a server with and without an `auth` section —
the second being a control-and-null pair in the sense [F4](../features/validated-archives.md)
settled on, where the axis is whether authentication happened at all.

[D3](../direction/authentication.md#how-it-would-be-measured) predicted this and predicted it
correctly, which is why it is filed here rather than argued: the rule this page opens with does not
stop applying once something has shipped.

**[F14](../features/encryption-in-transit.md) tried to close this and did not.** Its client sweep
opens *n* independent `Shoal` instances, each with its own pool and therefore its own handshakes,
on the theory that the per-connection cost would appear as the count rose. It does not, and the
reason is worth recording so nobody builds the same thing twice: **`bb8` fills `min_idle` inside
`Pool::build()`**, so all ten connections of every client have connected, framed and authenticated
before the constructor returns and long before the first sample is taken. The sweep measures steady
state with *n* warm pools.

Opening more connections is not the same experiment as timing one. This entry still wants a clock
around `Shoal::new` itself.

### O31. The disjointness rule cannot tell a result from a saturated workload

| | |
| --- | --- |
| **Rank** | **B** — not a speed change at all, a correctness change to how speed is judged |
| **Impact** | **Measured.** Four points of the `f14-encryption` capture report encryption making queries 14% to 47% *faster*, and every one of them passes the rule that decides whether a difference is real |
| **Difficulty** | S to detect, M to decide what to do about it |
| **Depends on** | nothing |
| **Blocks** | trusting any macro comparison taken near saturation |
| **Tradeoff** | Contained — a workload that is refused or flagged is one that produced a number nobody should have read |
| **Benchmark** | `macro/encryption/depth/*/128`, which is the thing that exposed it |

The macro layer calls a difference a result when the two sides' **observed intervals are disjoint**
— when the slowest run of one is still faster than the fastest run of the other. That is a good rule
and it is doing its job. What it cannot do is notice that the workload was not measuring what its
name says.

At a load depth of 128 the encryption sweep leaves the regime where a service time means anything.
Throughput *falls* as depth rises — 351,150 queries a second at depth 32 against 277,402 at depth
128, for 256 byte rows on the plaintext arm — which is the signature of a queue past its knee, and
the p50 stops being a latency and becomes a measure of how long the queue is. In that regime the
encrypted arm measured **faster**:

| Row | depth 32 | depth 128 |
| ---: | ---: | ---: |
| 256 B | +5.7% | **−23.5%**, separated |
| 4 KiB | +11.3% | **−46.8%**, separated |

Both of the depth-128 rows are cleanly separated across five runs. They are reliably weird, and the
rule detects *reliably* different, not *meaningfully* different.

**This is not an argument for dropping the rule**, which is the only thing standing between the page
and a curve drawn through noise. It is an argument that disjointness is necessary and not
sufficient, and that a workload has no way today to say "the number I just produced is outside the
regime I am for".

**Fix direction**, cheapest first. A workload could record its own **throughput against the previous
point on its axis** and flag a capture where more load bought less work — the data is already in the
artifact, since `wall_clock_ns` and the query count are both recorded, so this is an analysis
change and not a measurement one. Beyond that, a saturating sweep wants a declared knee: an axis
that stops where throughput stops rising, which is a property of the machine rather than of the
workload and would have to be found once and recorded.

Found while reading the first capture that held the sweeps, which is the only way it could have
been found — every point of it is correct, the harness did nothing wrong, and the numbers are still
not readable.

**Partly addressed by [F17](../features/workload-grid.md), and still open.** The grid ships a
four-rung **load depth ladder** at its reference cell — depths 1, 8, 32 and 128, identical in every
other respect — and [Access patterns](../performance/access-patterns.md) draws throughput and
latency against depth together and states in prose whether throughput fell at any rung. That makes
the knee *visible* for the one cell every grid arm's depth was chosen from, which is the first time
anything here could see it at all. What it does not do is either half of the fix direction above:
nothing computes the flag automatically, no axis carries a declared knee, and the ladder covers one
cell rather than every sweep. **The entry stays open.**

### O32. `Queries::deserialize` costs about nine nanoseconds more than it did

`shoal-core/src/server/shard.rs:1184`, `<Queries<D::ClientType> as RkyvSupport>::deserialize`

The server deserializes the whole request bundle once per client request. Measured before and
after [F15](../features/client-server-split.md) moved `Queries` and `RkyvSupport` into
`shoal-proto`:

| `wire_codec/request/decode/deserialize` | before | after |
| --- | --- | --- |
| 1 query | 29.52 ns | 38.28 ns (+29.7%) |
| 10 queries | 400.12 ns | 423.43 ns (+5.8%) |
| 100 queries | 5.79 µs | 5.89 µs (+1.7%) |

**Measured, and reproduced.** A second capture on the same tree put the one-query case at 38.91 ns,
so it is not a noisy reading. The cost is roughly constant in absolute terms and dilutes as the
bundle grows, which is the signature of a fixed per-call overhead rather than a slower loop — the
shape a function that stopped being inlined leaves.

The obvious cause is not the cause. `RkyvSupport::serialize` and `deserialize` are default trait
bodies that moved crates, so both were given `#[inline]`; that recovered `encode/serialize/1`
(70.58 → 64.04 ns, outside the noise band) and moved `deserialize/1` not at all. Whatever this is,
it is not the trait method's own inlining.

**It does not show up end to end.** The macro layer over the transport and fanout workloads has no
reproducible movement — see F15's Performance section — and nine nanoseconds against a p50 get of
roughly 30 µs is about 0.03%. It is filed because it is real and unexplained, not because it is
urgent.

**Where to start:** compare the generated code for `<Queries<S> as RkyvSupport>::deserialize`
across the boundary; check whether rkyv's `Pool` allocation is being hoisted differently; and note
that [item 66](known-issues.md) means there is no LTO to hide any of this, so whatever it is would
likely vanish under `lto = "thin"` — which is itself worth measuring before chasing this further.

### O33. The archives are written with glommio's defaults, and nothing can tune them

| | |
| --- | --- |
| **Rank** | **C** — argued, and about to become measured |
| **Impact** | Unknown. The bulk write path uses whatever `DmaStreamWriterBuilder` defaults to, and the setting that appears to govern it does not |
| **Difficulty** | S — thread `&self.conf` into two branches of one function |
| **Depends on** | [item 71](known-issues.md), which is the same finding as a defect |
| **Blocks** | any tuning advice about bulk ingest |
| **Tradeoff** | None known. It is a setting that already exists reaching code it already names |
| **Benchmark** | `macro/conf/storage/throughput_buffer/*` and `macro/conf/storage/throughput_write_behind/*` ([F20](../features/configuration-sweeps.md)) |

`ArchiveMap::get_active_writer` builds a `DmaStreamWriter` with no `with_buffer_size` and no
`with_write_behind`, in both its branches (`map.rs:415`, `:433`), while `new_writer` twelve lines
above configures the map's own intent log from `throughput_sensitive`. So the archives — the actual
bulk data — are written at glommio's default buffer size and queue depth, and the only thing
`throughput_sensitive` reaches is a small latency-shaped write.

**Argued from reading the source.** What makes this worth an `O` number rather than only a defect is
that the default may well be *wrong* for the workload: the archive writer is the one place in the
engine that streams whole compacted partitions, which is exactly the case a deep queue and a large
buffer exist for, and it is running at whatever a general-purpose default chose.

**How to adjudicate it.** The two sweeps named above are expected to be flat today. That flatness is
[item 71](known-issues.md)'s evidence. Fix the wiring, re-run `--group conf/storage`, and the same
two sweeps say whether the setting is worth anything — if they are still flat afterwards, the
default was fine and this entry closes as measured-and-declined rather than as taken.

### O26. `handle_query` cloned a `QueryMetadata` for a gather almost no query has

| | |
| --- | --- |
| **Rank** | **B** — measured as free, taken because it was free, not because it was ranked |
| **Impact** | One `QueryMetadata` clone per query removed — 617,175 per baseline run |
| **Difficulty** | S — one line, already taken |
| **Depends on** | nothing |
| **Blocks** | nothing |
| **Tradeoff** | None — the clone was only ever read on a path that checked the same condition |
| **Benchmark** | none of its own; folded into no F6 number, see below |

Filed and taken while building [F6](../features/stage-breakdown.md).

`Shard::handle_query` (`shard.rs:695`) cloned the whole `QueryMetadata` before handing it to the
tables:

```rust
// keep a copy of our metadata, since handling this query consumes it and a
// share of a split query has to travel back with the metadata it came from
let gathered_meta = meta.clone();
```

The copy is read in exactly one arm — the one taken when `meta.gather` is `Some`, meaning the
query was split across shards. Every other query paid for a clone of a `Uuid`, a `Uuid`, a
`usize`, a `bool`, an `Option<ShardContact>` and a `Span` and then dropped it. In the `tmdb`
workload **no query is ever split**: `MovieGet::new(vec![movie.id])` names one partition, so
`found.len() == 1` and `gather` is `None` for all 617,175 of them.

It is now cloned only when there is something to clone it for:

```rust
let gathered_meta = meta.gather.is_some().then(|| meta.clone());
```

F6 made this worth doing rather than merely tidy: [`StageStamps`](../features/stage-breakdown.md)
rides on `QueryMetadata`, so under a profiling build the clone got bigger, and a stage profile
that pays for its own instrumentation on a path it is measuring is the thing to avoid.

**Deliberately not measured as part of F6's capture.** An F6 run is a `stage-profile` build,
whose absolute latencies are not comparable to a shipping one, so folding a shipping-build
optimization into that capture would produce a number that means nothing. It needs its own
before-and-after macro capture against the frozen baseline, which has not been taken.


**What it left out is the larger point.** It covered seven entries. It was silent on **O23**, which
turned out to be the only measured entry on the page; on **O17**, the cheapest change with evidence
behind it; and on **O13**, which was quietly getting worse the whole time — its quadratic term moved
onto the resident get path and the entry was never updated. Three of the top four ranks were not on
the list, which is what a flat catalogue with an ordering bolted to the end will do.
