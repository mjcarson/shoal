# F24. The routing layer gets a benchmark, and it finds a second quadratic

## Context

Routing is the hop between a bundle arriving on a socket and the shards that can answer it: a
partition key becomes a shard, and a query naming several becomes one narrowed query per shard.
Every query in Shoal passes through it, and it was the one layer of the query path with **no
benchmark of any kind**.

[TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have) had asked for it since
[F3](performance-harness.md), and twice recorded that nothing blocked it. The first note filed it
under macro coverage, as though it needed the workload harness; the correction beside it is the
useful part:

> An `rkyv` round trip and `Ring::find_shard` are pure CPU over plain data and need no server, no
> executor and no storage backend — they belong beside `shoal/benches/partitions.rs` in the **micro**
> layer, where they would get criterion's sampling and a confidence interval instead of a wall clock
> with an 11% spread. They were not built there because nobody had noticed they could be.

`wire_codec` was built on that reasoning by [F10](framing-and-protocol-evolution.md). `routing` was
not, for four more features, and this is it.

## What it does

`shoal/benches/routing.rs`, four groups, no features required — `server::ring` and `ShardRouting`
are both public API, so unlike `partitions` this needs no `bench` feature and runs on a bare
`cargo bench -p shoal`.

| Group | Axis | What it answers |
| --- | --- | --- |
| `routing/find_shard` | 1, 4, 12, 64 shards | what one key→shard lookup costs, and whether it grows with the ring |
| `routing/ring_new` | the same | what building a ring costs, once per server start |
| `routing/split_by_shard/get` | 1, 2, 4, 16, 64, 256 keys | what splitting a multi-partition get costs, as a curve in the key count |
| `routing/split_by_shard/write` | the same key counts | **the control** — a write names one partition whatever the get beside it named |

The key counts are `macro/fanout/{resident,evicted}/n`'s own values, deliberately, so the isolated
cost measured here and the end-to-end curve measured there describe the same points.

### What it found

**`find_shard` is constant.** 392 ps, flat to **0.4%** across a 64× change in the ring. That is the
tablet ring answering in constant time, which is what
[Resolved #11/#12/#37](../appendix/resolved/tablet-ring.md) replaced a 1000×N `BTreeMap` to achieve
and what nothing had ever checked. [O6](../appendix/optimizations.md) said "still unmeasured, as
everything on this page is"; it is measured now.

**Splitting a get is quadratic in the keys it names**, and that was unfiled. `group_by_shard`
deduplicates by scanning every key it has already placed:

```rust
// a key we have already placed names a partition we are already reading
if grouped.iter().any(|(_, keys)| keys.contains(key)) {
    continue;
}
```

so placing the *i*-th key walks the *i*−1 before it. Filed as
[O39](../appendix/optimizations.md#o39-routing-a-multi-partition-get-is-quadratic-before-the-query-reaches-a-table).

| *n* | get | per key | control |
| ---: | ---: | ---: | ---: |
| 1 | 11.32 ns | 11.32 ns | 2.72 ns |
| 16 | 93.06 ns | 5.82 ns | 2.72 ns |
| 64 | 309.08 ns | 4.83 ns | 2.72 ns |
| 256 | 1.774 µs | 6.93 ns | 2.73 ns |

Fitted on the two widest points: **4.13 ns·n + 0.0109 ns·n²**. The quadratic is 3% of the split at
*n* = 16, 14.5% at 64, **40% at 256**.

**And the entry it found does not deserve much rank**, which the page says out loud. The whole split
at *n* = 256 is 1.77 µs against a read service time of roughly 40 µs, so removing the quadratic
outright buys under 2% of the widest query anybody runs and nothing at all of a single-partition
one. Recording that is the point: a benchmark that finds a real asymptotic and then says it is small
is more useful than one that finds nothing.

**The thing worth more than either** is what it does to a curve already in use.
`macro/fanout/{resident,evicted}/n` is the evidence [O13](../appendix/optimizations.md) is ranked
on, and O13 is *also* an O(n²) in the same caller-set *n*, one layer later. So that curve has two
known quadratics under it and cannot attribute a bend to either. Nobody had noticed, because
nothing had ever looked at the routing layer.

## Design choices

**The control is a write, not a null.** `split_by_shard` on an insert takes a branch that calls
`find_shard` once and never enters `group_by_shard`, so it exercises the same function, the same
dispatch and the same buffer, and is flat in the only thing the get arm varies. It came out flat to
0.3% across all six points, which is what makes the get arm's growth attributable to the dedup scan
rather than to criterion, the allocator, or the shape of the binary — the failure
[O24](../appendix/optimizations.md#o24-two-benchmarks-move-with-the-shape-of-the-binary-around-them)
exists to make somebody check for.

**Distinct keys, not repeated ones.** A get naming the same key *n* times bails on the first
comparison every time and would measure the dedup's *best* case while looking like its worst.

**The buffer is reused across iterations**, cleared rather than reallocated, because that is what
the shard loop does. Allocating a fresh `Vec` per iteration would fold an allocation into every
sample and flatten the very curve the benchmark is for.

**`Ring::new` is included though nothing is waiting on it.** It runs once per server start and it is
the thing [O6](../appendix/optimizations.md) replaced, so a number bounding it is worth the six
seconds it costs.

## Alternatives rejected

**Adding these to `shoal/benches/partitions.rs`.** That file requires the `bench` feature because it
reaches crate-private partition internals. Routing needs no such thing, and gating it would make a
bare `cargo bench -p shoal` silently skip the layer that had no coverage — the same reasoning
`wire.rs` records for staying ungated.

**Driving the split through a live server instead.** That is `macro/fanout`, it already exists, and
it is precisely what cannot attribute a cost to this layer. The whole value here is that a sample
contains routing and nothing else.

**Sweeping the shard count on the split arms too.** The dedup scan is O(keys) regardless of how many
shards the keys land on — `grouped.iter().any(...)` walks every key placed, not every shard — so a
shard axis would have cost six times the arms to draw six flat lines. Twelve shards is what the
benchmark host's `shoal.yml` resolves to and is the ring every other number on the page assumes.

## Limitations

- **The key counts stop at 256**, because that is where `macro/fanout` stops. The quadratic only
  becomes the dominant term past roughly 1024, so the fit above is extrapolated there rather than
  measured, and the page says so.
- **One ring size on the split arms**, for the reason above. If `group_by_shard`'s dedup is ever
  changed to be per shard rather than global, that reasoning stops holding and the axis has to come
  back.
- **It does not adjudicate [O20](../appendix/optimizations.md)**, which the adjudication table
  expected a `routing` bench to settle. O20 is about whether a get should read a partition it may
  not need — a *residency* question — and nothing in the micro layer varies residency. The bench
  that table asked for was specified by the code it touches rather than by the question it answers.
- **`find_shard` is measured with one key**, repeatedly, so it is a hot-cache number. A ring walked
  by keys scattered across the whole tablet map might miss more, and nothing here shows that.

## Invariants to uphold

- **The control must stay flat.** If `routing/split_by_shard/write` starts moving with its
  parameter, it has stopped being a control and no reading of the get arm is safe until it is
  understood. Its parameter is deliberately ignored by the code under it.
- **The key counts must stay `macro/fanout`'s.** The value of these two curves is that they describe
  the same points; changing one axis and not the other silently ends that.
- **The keys must stay distinct**, or the benchmark measures the opposite of what it is for.
- **This file must not acquire a required feature.** The layer went unmeasured for four features
  partly because measuring it looked like it needed machinery it does not.

## Performance

This benchmark measures; it changes no shipped code. `shoal/benches/routing.rs` and four lines of
`shoal/Cargo.toml` are the whole diff, so no capture is invalidated and no workload fingerprint
moves.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `cargo bench -p shoal --bench routing` | the routing layer has no coverage at all again |
| `routing/split_by_shard/write` | the get arm's curve has no control, so a change in the binary's shape reads as a change in the dedup scan |
| `routing/find_shard` | nothing checks that the tablet ring still answers in constant time, which is the entire property it was built for |

## Related

- [O39](../appendix/optimizations.md#o39-routing-a-multi-partition-get-is-quadratic-before-the-query-reaches-a-table) — what this found
- [O13](../appendix/optimizations.md#o13-a-multi-partition-get-is-quadratic-in-the-partitions-it-names) — the other quadratic in the same *n*, one layer later
- [O6](../appendix/optimizations.md) — the tablet ring, now measured
- [Resolved #11/#12/#37](../appendix/resolved/tablet-ring.md) — what replaced the `BTreeMap`
- [Partitioning](../architecture/partitioning.md) — what routing does and why
- [F10](framing-and-protocol-evolution.md) — `wire_codec`, built on the same correction this was left out of
- [F8](purpose-built-workloads.md) — `macro/fanout`, the end-to-end curve this cannot replace and now qualifies
