# F26. Requests are routed from the archive

## Context

A bundle of queries arrives as one rkyv archive. The coordinator turned the whole thing into owned
Rust values before it looked at any of it:

```rust
// load our arhived query from buffer
let archived = Queries::access(&data)?;
// deserialize our queries
let queries = <Queries<D::ClientType> as RkyvSupport>::deserialize(archived)?;
```

`shard.rs`, `Shard::handle_client`

Filed as [O1](../appendix/optimizations.md#o1-queries-are-fully-deserialized-on-arrival), which
named the cost — every `String`, `Vec` and filter in every query of the bundle allocated and
copied out of a buffer that already held them in a readable layout — and named the obstacle:
`send_to_shard` consumed the queries by value and `ServerMsg::Query` carried an owned
`QueryKinds`, so the archived form had to survive as far as the shard that executes the query.
The entry sat in **Tier C**, blocked on a design pass rather than on effort, through four
features.

Reading the path to write that design pass found it was worse than the entry said, in a way that
matters for what the fix should be. `split_by_shard`'s write arms end in `self.clone()`:

```rust
SortedQuery::Insert { key, .. } | SortedQuery::Delete { key, .. } => {
    // a write names a single partition so it goes to a single shard
    found.push((ring.find_shard(*key), self.clone()));
}
```

`routing.rs`, `ShardRouting for SortedQuery<T>`

`SortedQuery::Insert { key: u64, row: T }` carries the row. So an insert's row was deserialized
out of the archive **and then deep-copied again**, and both copies landed on core 0 — the single
coordinator every request in the system passes through. At the 512 KiB end of the
[row-size sweep](../performance/row-size.md) that is a megabyte of allocation and memory traffic
per insert, on the one core that cannot be scaled out of the way.

The two blockers the entry named are both discharged. The `wire_codec` benchmark it wanted was
built by [F10](framing-and-protocol-evolution.md) and captured in `f22-row-size`; and the
sequencing note asking this to be taken with D2 is moot, since [D2](../direction/framing.md)
landed as F10 as well.

## What it does

The coordinator no longer deserializes anything. It validates the bundle once, reads only the
scalars it needs to route by, and hands each destination shard the buffer itself:

```
coordinator (core 0)                    executing shard
────────────────────                    ───────────────
Queries::access(&body)?   ← validates once, per bundle
for each archived query:
  read partition keys / limit
  from the archive (scalars only)
  group by shard
  send { meta, body.clone(), offset, keys }
                              ────────▶  unarchive_queries(&body)   // unchecked
                                         deserialize(&archived.queries[offset])
                                         narrow_to(keys)
                                         tables.handle(meta, query)
```

`RequestBody` gains a second and last exit, `freeze`, which hands over its `BytesMut` as a
`Bytes` — the same allocation, now a shared read-only buffer whose clone is a refcount rather
than a copy. `ServerMsg::Query` gives up its owned `QueryKinds` and carries that buffer, the
query's offset within the bundle, and the partition keys the receiving shard owns.

The routing decision itself moves to a new trait beside `ShardRouting`:

```rust
pub trait ArchivedShardRouting: rkyv::Archive + Sized {
    fn route_archived<'a>(
        archived: &<Self as rkyv::Archive>::Archived,
        ring: &'a Ring,
        found: &mut Vec<(&'a ShardInfo, Option<Vec<u64>>)>,
    );
    fn archived_partition_keys(archived: &<Self as rkyv::Archive>::Archived) -> Vec<u64>;
    fn archived_limit(archived: &<Self as rkyv::Archive>::Archived) -> Option<usize>;
    fn narrow_to(self, keys: Vec<u64>) -> Self;
}
```

`routing.rs`

Every field `route_archived` reads is a `u64` or an `Option<usize>` sitting inline in the archive.
It never touches a row, a filter or a sort key, which is what lets a bundle of megabyte rows be
routed for the cost of its keys.

**A shard is handed `Some(keys)` or `None`, and the difference is load-bearing.** `None` is not
"every key" — it means the receiving shard must not narrow. Every write takes it, because a write
names its partition in a field the narrowing does not reach.

**The per-query deserialize is a stage of its own.** `decode` still exists and is still a batch
level cost, but it now covers only `access` — validating that the bytes are a bundle. What it used
to hold is a new twentieth stage, `query_decode`, spanning `exec_dequeued → query_decoded` on the
shard that executes the query, and it is deliberately **not** a batch stage. A twenty-first,
`partition_wait`, comes with it — see below. A reader comparing a
capture from before this to one after has to add the two together to compare like with like.

**A released query is a separate message.** A query parked on a partition read was decoded when it
first arrived and narrowed to the partition it blocked on; there is no bundle to read it out of and
no decode to charge it for twice. It comes back as `ServerMsg::Released` rather than as an arm of
`ServerMsg::Query`, so the difference between the two — which costs have already been paid — is in
the type rather than in a comment.

**And it is dequeued twice, which needed a stamp of its own.** Splitting the decode out of
`execute` gave `exec_dequeued` two jobs: it ends `exec_queue` and it starts `query_decode`. A
replay re-stamping it therefore reported every parked query as having decoded instantly, and put
the whole disk wait inside `execute` as well as inside `exec_queue`. `mark_exec_resumed` is a
second stamp so that neither has to lie, and the wait between them is a twenty-first stage,
`partition_wait` — a phase the layer never named, because until the decode was split out there was
nothing for it to sit between.

## Design choices

**`Bytes`, not `BytesMut` or an `Arc<Vec<u8>>`.** A bundle naming partitions on several shards is
held by all of them at once, and `BytesMut::clone` copies. `Bytes` is a refcount over the same
allocation, is `Send`, and is immutable — and that immutability is half of what makes the
unchecked read on the far side sound.

**`access_unchecked` on the executing shard, with the validation as a documented precondition.**
`ShoalDatabase::unarchive_queries` already existed for exactly this and had no callers. It has been
made `unsafe fn` with a `# Safety` block, because as a *safe* fn wrapping `rkyv::access_unchecked`
it would let any caller hand it any bytes. The alternative — revalidating per shard — means every
shard walking the whole bundle with `bytecheck` to reach one query in it, which costs more than the
copy this change exists to remove and gets worse as the bundle grows.

**The narrowing splits from the decision.** `route_archived` says which shards and which keys;
`narrow_to` applies the keys, on the shard that will answer. Keeping the narrowing in one place
means `for_partitions` is still the only thing that builds a narrowed query, and the sorted and
unsorted arms did not have to be duplicated.

**`split_by_shard` is kept.** It is no longer on the live path, and it is not dead: it is the
reference implementation every test here checks `route_archived` + `narrow_to` against, and
`routing/split_by_shard/*` is the benchmark the new `routing/route_archived/*` arms are read
against. A change to how keys are placed has to move both.

**`deserialize_query` has no default body.** Writing one on `ShoalDatabase` means proving
`Archived<QueryKinds>: Deserialize<QueryKinds, _>` for every schema at once, which is not provable
from the bounds that trait has. The derive knows the concrete enum and rkyv has already written
that impl for it, so the one line is generated rather than written generically.

## Alternatives rejected

**Just remove the second copy.** `split_by_shard` could take `self` by value, and the write arms
would stop cloning the row. That is an `S` change contained to one file and it halves the
request-side cost at the wide end. It was rejected because it leaves the remaining copy on core 0,
which is the half that does not scale: the coordinator is a serialization point for every request
in the system, and moving work off it is worth more than removing the same work from a shard. The
change taken here subsumes it — there is no clone left to remove.

**Execute against the archive, never deserializing at all.** The full form of O1: the table layer
reads filters, sort keys and rows straight out of `&ArchivedQueryKinds`. This is where the rest of
the entry's cost lives, and it is not this change. It reaches every table method and the derive's
filter and update codegen, it is `XL` rather than `L`, and an insert needs an owned row to put in
its partition map whatever the query layer does — so the ceiling on it is lower than it looks.
Filed in [TODOs](../appendix/todos.md).

**Re-derive each shard's keys from its own ring instead of carrying them.** Every shard owns a
`Ring` and could work out which of a get's keys are its own, saving a `Vec<u64>` per message. It
was rejected because the *order* a get named its partitions in is what the gather uses to put the
shares back together, and reconstructing that order per shard is both more work and one more place
for the two sides to disagree. Carrying the keys is a handful of `u64`s against a query that is
about to read partitions.

**One `ServerMsg::Query` with a payload enum** rather than a second `Released` variant. Rejected
because the message loop then has to branch on which shape it got before it can do anything, and
the two shapes differ in exactly the thing this feature is about.

## Limitations

**A fan-out get is deserialized once per shard it lands on, not once.** Before, the coordinator
deserialized a split get once and `for_partitions` cloned its filter set per destination. Now each
destination deserializes it. That is close to a wash — a filter-set clone and a filter-set
deserialize are the same allocations — and it happens on *N* cores rather than one, but it is not
a saving, and for a get with a very large filter set against many shards it could be a loss. The
arms that carry bytes are unaffected: a write is always single-destination by construction.

**Sort-key normalization moved, and happens more often.** `SortedGet::for_partitions` documented
that its selection was "normalized once, where this query entered the server, instead of once per
shard it is split to". That is no longer true — the coordinator never deserializes a selection, so
`narrow_to` normalizes on each shard. The comment has been corrected rather than left standing.
It is a sort and a dedup of a small `Vec`, now done on the shards rather than on core 0.

**The bundle outlives the routing loop.** A bundle's buffer is held until the last shard it was
routed to has deserialized its share. That is bounded and short, but it is a change: before, the
buffer was dropped as soon as the coordinator finished with it, and a slow shard now pins bytes
rather than only its own query.

**`decode` is not comparable across this change.** It measured the bundle deserialize and now
measures the bundle validate. Any reading of a stage report that spans this feature has to add
`query_decode` back in.

**Nothing here is measured yet.** See [Performance](#performance).

## Invariants to uphold

**The coordinator validates before it shares, and nothing writes to the buffer afterwards.** This
is the whole basis for `access_unchecked` on every shard downstream. `Queries::access` runs in
`handle_client` before `freeze`, and `Bytes` cannot be written to. Break either half — route
without validating, or introduce a mutable path to those bytes — and every shard is reading
attacker-shaped pointers. `unarchive_queries` is `unsafe` so that this cannot be broken silently.

**`RequestBody` keeps exactly two exits.** `Deref` and `freeze`, both reachable only from a
`RequestBody`, which only `read_from` builds. [F25](read-buffers-are-filled-not-zeroed.md) is what
makes those bytes initialized at all; a third way out that does not go through a completed read
hands a shard memory nothing wrote.

**`route_archived` and `split_by_shard` must agree.** Not "should" — the tests assert it, because
the second is the only definition of correct the first has. If `split_by_shard` is ever deleted,
these tests lose their oracle and become assertions about hardcoded shard numbers, which is a much
weaker thing.

**`None` means do not narrow.** Sending `Some(keys)` for a write would put it through
`narrow_to`, which is a no-op for a write *today*. That is a coincidence of the current arms, not
a guarantee.

**A released query is never decoded again.** It has no bundle to be decoded from, and stamping it
`query_decoded` would put a zero into a percentile that is supposed to describe real decodes.

## Performance

**Nothing here has been captured yet.** The prediction is written down first, so the capture can
disagree with it:

- `decode` collapses toward the cost of `access` alone, and loses most of its sensitivity to row
  width. It was one of the fastest-growing stages on the width axis.
- `query_decode` appears carrying what `decode` lost — but on *N* shards rather than on core 0.
- **`decode + query_decode` is less than today's `decode` on write-heavy arms**, because the
  `self.clone()` is gone. This is the claim that can fail, and the one worth checking first.
- Write-heavy wide arms — `macro/grid/*/r0/{8192,524288}` — improve. The 1 KiB reference cell
  moves little, because at 1 KiB the copies this removes are small.

Two instruments price it. `routing/route_archived/{get,write}` are new micro arms beside
`routing/split_by_shard/{get,write}` from [F24](routing-benchmarks.md), and the difference between
the pairs is what routing from the archive is worth per query before the moved deserialize is
counted at all. The stage layer is what attributes the moved deserialize, and it is why
`query_decode` exists.

`wire_codec/request/decode` and `wire_codec/width/request/decode/*` are the benchmarks
[O1](../appendix/optimizations.md) named as its precondition. They already exist and were captured
in `f22-row-size`; they price the gap between `access` and `deserialize`, which is the quantity
this change moves off the coordinator.

Nothing under `shoal-bench/src/workloads/` changed except the stage list, `shoal.yml` is untouched,
and no workload identifier moved — so every existing capture still joins for `compare`. Stage
captures taken before this correctly report `stale`, because the stage set really did change.

## Tests

| Test | Breaks if |
| --- | --- |
| `a_get_routes_the_same_way_off_its_archive` | `route_archived` + `narrow_to` chooses different shards, or narrows differently, than `split_by_shard` |
| `a_sorted_get_routes_the_same_way_off_its_archive` | the same, for the sorted query enum and every arm of its row selection |
| `sort_keys_are_still_normalized_after_narrowing` | normalization was lost when it moved off the coordinator |
| `a_write_is_routed_to_one_shard_and_never_narrowed` | a write is routed with keys to narrow to, or to more than one shard |
| `a_gets_limit_and_partition_order_read_the_same_off_the_archive` | the gather's limit or partition order is read wrongly out of the archive, which reorders or truncates a client's rows without failing anything |
| `a_body_read_in_chunks_holds_every_byte_it_was_sent` | F25's guarantee, which `freeze` now also depends on |
| `a_parked_get_does_not_count_its_disk_wait_as_execution` | a replayed query overwrites the stamp `query_decode` is measured from, which reports its decode as instant and its disk wait as execution |

The first four were checked by breaking the code they cover and confirming they fail: dropping the
`normalized()` call fails two of them, and routing a write with `Some(keys)` fails the third. So was
the last: anchoring `execute` at `query_decoded` again reports a parked get's `execute` as 50,100 ns
against the 100 ns of work it did.

The end-to-end path is covered by the existing integration suites — every query in
`persistent_sorted_table.rs`, `persistent_unsorted_table.rs` and their ephemeral pairs now travels
through the archive rather than through a deserialized bundle, so a break in the plumbing fails
them rather than only the tests above.

## Related

- [O1](../appendix/optimizations.md#o1-queries-are-fully-deserialized-on-arrival) — the entry this
  closes, kept struck through with what it got right and what it missed
- [O2](../appendix/optimizations.md) — the response half, still open and still the larger one
- [F25](read-buffers-are-filled-not-zeroed.md) — built `RequestBody`, whose guarantee `freeze`
  extends
- [F24](routing-benchmarks.md) — built the routing benchmarks the new arms sit beside
- [F10](framing-and-protocol-evolution.md) — built the `wire_codec` benchmark O1 was blocked on
- [Row size and what it costs](../tables/row-size.md) — where this cost stops being small
