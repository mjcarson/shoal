# Row size and what it costs

[Row size](../performance/row-size.md) draws what an even read/write mixture costs across eleven row
widths. This page says why the curve has the shape it has, what is a property of the system and what
is a property of the measurement, and which entry in [Optimizations](../appendix/optimizations.md)
would change each part of it.

The split is the same one [Tuning](../operations/tuning.md) makes against
[Configuration and what each setting is worth](../performance/configuration.md): the numbers are
generated from a capture and are re-rendered whenever a new one is taken, while the account of what
they mean is written by hand and revised when it is wrong.

## The shape

From `F20-conf` (2026-08-22), the unsorted pair at an even mixture and a load depth of 32. The
persistent and ephemeral arms differ in the storage engine and in nothing else
([F9](../features/ephemeral-tables.md)), so the gap between the two columns is what storage costs at
that width.

| Row | Persistent q/s | Ephemeral q/s | Persistent write p50 | Persistent read p50 | Persistent read p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B | 58,280 | 352,672 | 958.01 µs | 39.28 µs | 82.14 µs |
| 1 KiB | 53,344 | 355,482 | 1.09 ms | 40.00 µs | 78.91 µs |
| 8 KiB | 36,562 | 335,164 | 1.57 ms | 46.72 µs | 95.95 µs |
| 512 KiB | 4,224 | 12,900 | 10.03 ms | 333.55 µs | 15.19 ms |
| 4 MiB | 466 | 800 | 66.13 ms | 23.49 ms | 126.58 ms |

Three regimes, and the boundaries are not in the same place for the two halves.

**Flat, to about a kilobyte.** A fixed per-query cost dominates, so throughput barely moves while the
payload grows sixteenfold. Read service time is ~39 µs on the persistent tables and ~84 µs on the
ephemeral ones, and the ephemeral arms nonetheless do six times the throughput — a *higher* latency
at a *higher* rate is a queue, not a slower path, and it is what a workload sitting past the knee of
its own throughput curve at depth 32 looks like.

**One octave where only the persistent half moves.** Over 1 KiB → 8 KiB the persistent arms lose
**31%** of their throughput and the ephemeral arms lose **6%**. Whatever happens there is in the
storage engine, and [the intent log stops batching](#the-intent-log-stops-batching-past-the-staging-buffer)
is the candidate.

**A byte ceiling, past that.** Both halves fall roughly as 1/width, which is what a per-byte cost
looks like once it has overtaken the per-query one. Part of this is arithmetic and not a defect: a
store that moves 64 times the bytes per query cannot also answer as many queries. The question worth
asking is not *that* it falls but **what ceiling it falls toward**, and the ceilings are low:

| | Peak payload throughput |
| --- | ---: |
| Ephemeral sorted, 512 KiB | **6.9 GiB/s** |
| Persistent unsorted, 1 MiB | **2.1 GiB/s** |

6.9 GiB/s is the ceiling for a table with **no storage engine at all**, on a machine whose memory
bandwidth is an order of magnitude above it. Some of that gap is the loopback TCP the benchmark
client talks over, and nothing here separates the two — that subtraction needs the client-side
instrumentation [TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have) records
as missing. The rest is a copy budget, and the next section is where it goes.

## Why: four mechanisms

Kept apart because they are not equally established, and because the last one is not a defect at
all — it is the shape any store has once it is moving bytes rather than answering queries.

### The payload is walked about six times per round trip

Every one of these is O(bytes), which is why none of them is visible at 64 bytes and why together
they are most of the cost at 4 MiB. This is the mechanism that is present on **both** halves of the
pair, and therefore the one that explains why the ephemeral arms fall too.

| Where | What it costs | Filed as |
| --- | --- | --- |
| `shard.rs:105` — `BytesMut::zeroed(header.body_len())` | a `memset` of the whole request body, overwritten by the `read_exact` on the next line | [O29](../appendix/optimizations.md#o29-a-request-body-is-zeroed-and-then-immediately-overwritten) |
| `shard.rs:1184` — `Queries::deserialize` | every `String` and `Vec` in the bundle allocated and copied out of a buffer that already holds them in a readable layout | [O1](../appendix/optimizations.md#o1-queries-are-fully-deserialized-on-arrival) |
| `partitions.rs:585`, `:293` — `P::from_row` | the row copied into the partition, and copied again on the way out of a get | [O2](../appendix/optimizations.md#o2-every-returned-row-is-copied-at-least-twice) |
| `fs.rs:367` — `RkyvSupport::serialize` | the row serialized back into a fresh `AlignedVec` for the intent log | [O11](../appendix/optimizations.md#o11-a-fresh-alignedvec-per-write-and-per-response) |
| `fs.rs:372` — `hasher.write(archived.as_slice())` | a second full pass over the record, for its checksum | [O11](../appendix/optimizations.md#o11-a-fresh-alignedvec-per-write-and-per-response) |
| `fs.rs:386` — `buff.write_all(archived.as_slice())` | a third pass, copying it into the DMA buffer | [O11](../appendix/optimizations.md#o11-a-fresh-alignedvec-per-write-and-per-response) |
| `shard.rs:1212` — `rkyv::to_bytes(&response)` | the response serialized into another fresh `AlignedVec` | [O2](../appendix/optimizations.md#o2-every-returned-row-is-copied-at-least-twice), [O11](../appendix/optimizations.md#o11-a-fresh-alignedvec-per-write-and-per-response) |

A read served from an archived partition is worse still: `P::from_archived`
(`partitions.rs:1092`, `:363`) materializes an owned row from bytes, so the path is **bytes → owned
rows → bytes** rather than **rows → copied rows → bytes**.

**What the row-size axis adds to those entries.** Each of them was filed as a small constant cost on
a hot path, and ranked accordingly. They are not constant. Their cost grows in the row width, which
is a quantity **the caller controls** — the page's *Asymptotic* grade rather than its *Argued* one —
and this axis is where that becomes the whole story. The ranking of O1, O2, O11 and O29 against each
other does not change; what changes is that all four are much larger for a caller with wide rows than
their scorecards suggest.

*Evidence: read from the source, consistent with the capture. Nothing measures any individual copy —
but `wire_codec/width/request/decode/{access,deserialize}` and `wire_codec/width/response/encode`
now measure two of them directly, in the micro layer, and the per-stage breakdown at three widths
([F22](../features/row-size-benchmarks.md)) says which of the nineteen stages the rest of them are
in. Neither has been captured.*

### The intent log stops batching past the staging buffer

**Persistent tables only**, and the candidate for the 1 KiB → 8 KiB octave where only they move.

`shoal.yml` sets `latency_sensitive.buffer_size: 4096`, and every arm of the capture records
`latency_buffer_size: 4096` in its own `conf` block, so this is live in the numbers above. The
writer stages records into one aligned buffer and flushes it when the next record will not fit
(`fs/stream.rs:655-665`):

```rust
pub async fn prep(&mut self, size: usize) -> &mut [u8] {
    // if we don't have enough usable space then write our current buffer out
    if self.usable() < size + self.buff_pos {
        // we won't have enough space to write this new data to out buffer so get a new one
        // make this new buffer big enough for our next write or bigger
        let new_usable = std::cmp::max(self.default_buffer_size, size);
        // write but not sync our current buffer to disk
        self.write(new_usable).await.unwrap();
    }
```

`write` then calls `alloc_buffer(new_usable)` (`:617`), which is a fresh `alloc_dma_buffer` sized to
the record. So the behaviour is a step, not a slope:

- **Below the buffer**, several records share one aligned write. At 1 KiB, three of them do.
- **At or above it**, every record is its own DMA write, preceded by its own DMA buffer allocation,
  and the group commit that amortizes the durability barrier across concurrent writers has nothing
  left to group.

8 KiB is the first width in the sweep above 4096, and it is the width at which the persistent arms
diverge from the ephemeral ones. Filed as
[O34](../appendix/optimizations.md#o34-a-record-wider-than-the-staging-buffer-defeats-intent-log-batching).

**The configuration sweep could not see this.** `latency_buffer` is swept across five rungs and
comes back at 1.06× — a `yes` in the *Real?* column and a recommendation nobody should act on,
because the sweep runs at the grid's reference cell of **1 KiB**, which is the one width where the
setting cannot bite. Reading that row as "the buffer size is worth 6%" is reading it at the only
width where the answer is no.

~~See [What would settle this](#what-would-settle-this).~~ The same five rungs now run at 8 KiB and
at 64 KiB as well ([F22](../features/row-size-benchmarks.md)), and the configuration page labels
each sweep with the width it ran at — so the 1.06× row is still there and no longer stands alone.
Nothing has been captured at those widths yet, which is why this section still argues from the
source.

*Evidence: read from the source. The capture is consistent with it and does not isolate it — the
persistent half of that octave also pays more per byte for everything in the previous section. The
`r0` width sweep is what would size the write path's share of it, and the `latency_buffer` rungs
above the buffer are what would adjudicate the step itself. Both exist; neither has been run.*

### A wide response blocks every narrow one behind it

`client_tx_relay` (`shard.rs:200`) is one serial loop per connection: it takes a response off the
channel, writes it to completion with `write_vectored`, and only then looks at the next one. There is
one such task per client connection, and every shard's replies for that client go through it.

The consequence is visible in the percentiles rather than in the medians. At 512 KiB the persistent
unsorted arm reads at a **333.55 µs p50 and a 15.19 ms p99** — a 45× spread on a table that is
entirely resident. At 8 KiB the same ratio is 2×. A read that took 333 µs of work and 15 ms of wall
clock spent the difference waiting, and the thing in front of it was a multi-megabyte write to the
same socket. Filed as
[O35](../appendix/optimizations.md#o35-the-per-connection-response-relay-writes-one-response-at-a-time).

*Evidence: read from the source, indicated by the p50/p99 split. Not isolated — the fixed depth
below is an alternative explanation for part of it, and the depth-1 ladder at each width is now what
would tell the two apart: a p99 that collapses at one outstanding query and not at thirty two is a
queue in front of the relay rather than a cost in it.*

### The rest is arithmetic

Past the point where the per-byte term dominates, throughput in queries a second **must** fall
roughly as 1/width, and no amount of optimization changes that. This is why
[the generated page](../performance/row-size.md) draws payload bytes a second alongside queries a
second: a store that moves few large rows quickly is not slow. Read the two charts together, and
treat a falling `queries/s` with a flat `payload/s` as the system working correctly.

## What this is not

Two candidates that the capture rules out, which is worth as much as the ones it supports.

**Not eviction.** The grid sizes its seed to a fixed byte budget, so the widest arms hold **256 MiB**
against the `4Gi` limit `shoal.yml` sets. Nothing is evicted at any width, and the memory sweep on
[Configuration](../performance/configuration.md) is flat across six rungs for the same reason. See
[Memory and Eviction](memory-and-eviction.md).

**Not the frame limit.** `networking.max_frame_bytes` defaults to 64 MiB, sixteen times the widest
row in the sweep, and the `frame` sweep fails the *Real?* gate at 1.02×. A wide row is not close to
being refused, and nothing about the limit changes as rows grow.

## What the capture cannot tell you

Read this section before quoting any number above as a knee.

**The axis has a 64× hole in it.** It goes 8 KiB → 512 KiB with nothing in between. Everything on
this page about *where* the curve bends is an argument from the source that the capture is consistent
with; the location and the sharpness of the knee are not measured. ~~Nothing fills it.~~ Sixteen,
32, 64, 128 and 256 KiB are now arms of the grid on all four tables, so the next capture measures
the knee rather than bracketing it.

**Load depth is fixed at 32 at every width** ([F17](../features/workload-grid.md)). At 4 MiB that is
128 MiB outstanding on one client, and the arm is measuring queueing at least as much as service
time. A latency past the knee of a throughput curve is a measure of queue depth, which is exactly the
condition [Tuning](../operations/tuning.md#start-here) says invalidates the rest of the advice. The
same axis at **one** outstanding query is now an arm at every width, and the difference between the
two is the part that was queue.

**At the wide end the key space is smaller than the load is deep.** The 4 MiB arm seeds **64
partitions** and keeps 32 queries outstanding against them, across twelve shards. Contention on a
handful of partitions is folded into those numbers and cannot be separated out.

**The axis is swept at `r50` only.** The 1 KiB → 8 KiB divergence is a write-path effect being
measured under a mixture that is half reads, so it is being asked half a question. It is now swept
at `r0` and `r100` as well, on all four tables — 120 arms whose whole purpose is that this sentence
stops being true of the next capture.

**Percentiles at the wide end are thin.** Every arm moves roughly the same number of bytes, so the
4 MiB arm runs a few hundred queries where the 64 byte arm runs twenty thousand. Read the p50 out
there; a p99 over two hundred samples is roughly its third-worst observation.

## What to do today

Everything here is still unmeasured at the width it matters at — the benchmarks that would measure
it exist now and have not been run — so treat it as a hypothesis to test on your own workload rather
than as a recommendation with a number behind it.

- **Size `latency_sensitive.buffer_size` above your widest row**, so the intent log can batch again.
  It is a minimum that gets rounded up to the device's O_DIRECT alignment, and a record larger than
  it is never batched with another one. The cost is padding on a partial flush and a larger DMA
  allocation per shard. See [Tuning](../operations/tuning.md#if-your-rows-are-wide).
- **Keep the load depth down when rows are wide.** Thirty-two outstanding 4 MiB queries is 128 MiB
  in flight on one connection, in front of a relay that writes one response at a time.
- **Split a wide row rather than storing it whole**, if the reads do not need all of it.
  [Projections](../features/projections.md) narrow what a get copies and what the wire carries, and
  they are the one lever on this page that is measured.

## What would settle this — built, and not yet run

Six benchmarks, none of which existed when this page was written. **All six exist now**
([F22](../features/row-size-benchmarks.md)), and **none of them has been captured**. The
distinction matters on this page more than on most: everything above is still an argument, and what
changed is that each part of it now has something that would decide it. Ordered as they were filed,
cheapest first.

| # | What it settles | Built as |
| ---: | --- | --- |
| 1 | The per-byte half of O1 and O2, in the micro layer with a confidence interval | 50 `wire_codec/width/*` ids over five widths, bundle size and response cardinality held fixed, with the header decode swept alongside as a flat control |
| 2 | O34 outright — whether `latency_buffer` is a step and whether the step is at the buffer | `macro/conf/storage/latency_buffer/r50/w8192/*` and `.../w65536/*`, the same five rungs above the buffer |
| 3 | Where the knee is, as a measurement | 16, 32, 64, 128 and 256 KiB on all four tables, closing the 64× hole |
| 4 | How much of a wide arm's latency was queue rather than service | `macro/grid/depth/1/<width>`, the width axis at one outstanding query |
| 5 | Which half of the mixture the per-byte cost is on | The whole width axis again at `r0` and `r100`, on all four tables |
| 6 | **Which** of the nineteen stages grows with bytes | The stage layer pointed at 1 KiB, 8 KiB and 512 KiB instead of one workload at 256 bytes |

Until a capture is taken, [the generated page](../performance/row-size.md) prints, in place of each
new section, a sentence saying which question that capture cannot answer. That is deliberate: a
section that drew whatever it found would have turned the five existing `r0`/`r100` arms at 1 KiB
into a curve and read as though the axis had been swept at six mixtures.

**What none of the six closes.** The grid is still a cross rather than a cube. `r0` and `r100` are
swept at a load depth of 32, and the depth-1 ladder is swept at `r50`, so a cost that appears only
at one query outstanding under a pure write mixture remains invisible to both. The depth-1 ladder is
one table, so the persistent-against-ephemeral subtraction that makes a width effect attributable to
storage does not exist for the depth axis. And the stage breakdown is taken at the mean of every
query rather than at a rank, so *which* stage makes the tail is a different question again. All
three are filed in [TODOs](../appendix/todos.md#the-row-size-axis).

## Related

- [Row size](../performance/row-size.md) — the generated numbers this page explains
- [The Intent Log](../storage/intent-log.md) — the staging buffer and the durability barrier
- [Memory and Eviction](memory-and-eviction.md) — why eviction is not what makes a wide row slow
- [Partitions](partitions.md) — where the copies on the read path happen
- [Request Lifecycle](../architecture/request-lifecycle.md) — the path the payload is copied along
- [Tuning](../operations/tuning.md) — what to set, and what rests on a measurement
- [F22](../features/row-size-benchmarks.md) — the six benchmarks this page asked for, built
- [Optimizations](../appendix/optimizations.md) — O1, O2, O11, O29, O34, O35
