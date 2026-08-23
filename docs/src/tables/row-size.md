# Row size and what it costs

[Row size](../performance/row-size.md) draws what an even read/write mixture costs across sixteen row
widths. This page says why the curve has the shape it has, what is a property of the system and what
is a property of the measurement, and which entry in [Optimizations](../appendix/optimizations.md)
would change each part of it.

The split is the same one [Tuning](../operations/tuning.md) makes against
[Configuration and what each setting is worth](../performance/configuration.md): the numbers are
generated from a capture and are re-rendered whenever a new one is taken, while the account of what
they mean is written by hand and revised when it is wrong.

**This page has now been revised that way once.** Every section below used to end in a note saying
the benchmark that would settle it was built and had not been run. `f22-row-size` ran five of the
six ([F22](../features/row-size-benchmarks.md)). Two of the four mechanisms survive with a
measurement behind them, one survives with its *shape* corrected, one is **refuted** — the tail it
was filed on turns out to be queueing — and the sixth benchmark did not run at all
([item 76](../appendix/known-issues.md#76-the-stage-layer-joins-nothing-for-any-grid-arm-and-reports-it-as-a-layer-that-ran)).
Where a claim changed, the old one is struck through and kept beside what replaced it.

## The shape

From `f22-row-size` (2026-08-23), the unsorted pair at an even mixture and a load depth of 32. The
persistent and ephemeral arms differ in the storage engine and in nothing else
([F9](../features/ephemeral-tables.md)), so the gap between the two columns is what storage costs at
that width. The five infill widths that closed the axis's 64× hole are the rows from 16 KiB to
256 KiB.

| Row | Persistent q/s | Ephemeral q/s | Persistent write p50 | Persistent read p50 | Persistent read p99 | p99/p50 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B | 58,450 | 359,507 | 961.72 µs | 38.46 µs | 75.54 µs | 2.0× |
| 512 B | 56,643 | 360,512 | 1.02 ms | 38.71 µs | 79.52 µs | 2.1× |
| 1 KiB | 53,172 | 363,535 | 1.09 ms | 40.24 µs | 82.35 µs | 2.0× |
| 8 KiB | 36,700 | 344,761 | 1.56 ms | 48.14 µs | 94.16 µs | 2.0× |
| 16 KiB | 31,551 | 302,397 | 1.74 ms | 50.67 µs | 125.34 µs | 2.5× |
| 32 KiB | 25,738 | 238,006 | 2.05 ms | 53.98 µs | 359.24 µs | 6.7× |
| 64 KiB | 18,611 | 158,785 | 2.80 ms | 60.45 µs | 2.26 ms | **37.4×** |
| 128 KiB | 14,286 | 79,285 | 3.47 ms | 83.36 µs | 4.33 ms | **52.0×** |
| 256 KiB | 8,020 | 33,611 | 5.85 ms | 163.28 µs | 7.49 ms | 45.8× |
| 512 KiB | 4,117 | 12,366 | 10.34 ms | 360.04 µs | 16.85 ms | 46.8× |
| 1 MiB | 2,226 | 4,930 | 16.43 ms | 1.98 ms | 23.75 ms | 12.0× |
| 4 MiB | 454 | 792 | 60.54 ms | 20.72 ms | 152.42 ms | 7.4× |

Three regimes, and the boundaries are not in the same place for the two halves.

**Flat, to about a kilobyte.** A fixed per-query cost dominates, so throughput barely moves while the
payload grows sixteenfold. Read service time is ~39 µs on the persistent tables and ~84 µs on the
ephemeral ones, and the ephemeral arms nonetheless do six times the throughput — a *higher* latency
at a *higher* rate is a queue, not a slower path, and it is what a workload sitting past the knee of
its own throughput curve at depth 32 looks like.

**One octave where only the persistent half moves.** Over 1 KiB → 8 KiB the persistent arms lose
**31%** of their throughput and the ephemeral arms lose **5%**. Whatever happens there is in the
storage engine, and the mixture split now says it is in the *write* path specifically:
[the intent log](#the-intent-log-batches-fewer-records-as-rows-widen) is the mechanism, and it is
the one this capture supports most directly.

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
instrumentation [TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have)
records as missing. The rest is a copy budget, and the next section is where it goes.

### Where storage's share is worst, and where it stops mattering

The persistent arm as a percentage of its own ephemeral control — the same table with `NoStorage`
and nothing else changed — is not monotonic, and the shape of it is the clearest single statement
this capture makes about which mechanism owns which part of the axis:

| Row | 64 B | 1 KiB | 8 KiB | **16 KiB** | 64 KiB | 256 KiB | 1 MiB | 4 MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Persistent ÷ ephemeral | 16.3% | 14.6% | 10.6% | **10.4%** | 11.7% | 23.9% | 45.2% | 57.3% |

Storage's *relative* penalty is worst at 16 KiB and then shrinks steadily, until at 4 MiB the
persistent table runs at 57% of a table that never touches a disk. Storage does not get cheaper —
the shared per-byte cost on both halves grows until it swamps the difference. **The near knee
belongs to the storage engine; the far tail belongs to costs both halves pay.**

## Why: four mechanisms

Kept apart because they are not equally established, and because the last one is not a defect at
all — it is the shape any store has once it is moving bytes rather than answering queries.

They are no longer equally *alive*, either. Two are measured, one is measured with its shape
corrected, and the third is struck through: the capture took its evidence away and gave it to load
depth. All four are kept, because a mechanism that turned out not to be the explanation is worth
more on the page than off it — the next reader to notice a serial relay and a 45× tail will
otherwise derive it again.

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
is a quantity **the caller controls**, and this axis is where that becomes the whole story. The
ranking of O1, O2, O11 and O29 against each other does not change; what changes is that all four are
much larger for a caller with wide rows than their scorecards suggest.

**Two of the copies are now measured, and the growth is steeper than the entries implied.** The
micro layer sweeps the codec at five widths from 64 B to 64 KiB with the bundle size and the
response cardinality held fixed, so nothing but the payload varies:

| `wire_codec/width/…` | 64 B | 64 KiB | Growth |
| --- | ---: | ---: | ---: |
| `response/decode/into_aligned` | 30.71 ns | 13.29 µs | **×432.8** |
| `response/decode/access` | 29.11 ns | 6.97 µs | ×239.4 |
| `request/decode/deserialize` | 187.80 ns | 11.55 µs | ×61.5 |
| `request/decode/access` | 22.19 ns | 3.81 µs | ×171.5 |
| `response/encode/framed` | 191.19 ns | 13.81 µs | ×72.2 |
| `request/encode/framed` | 192.32 ns | 7.55 µs | ×39.2 |
| `request/decode/header` *(control)* | 0.7071 ns | 0.7088 ns | ×1.002 |

The control is the row that makes the rest of the table readable: eight bytes is eight bytes at
every width, and it moves by a quarter of a percent across a thousand-fold change in the payload. So
every other row is payload cost and not per-call overhead. (The control was measured at all five
widths and the chart on [Micro benchmarks](../performance/micro.md) does not draw it — filed as
[item 75](../appendix/known-issues.md#75-a-control-that-was-measured-at-every-width-is-not-drawn-and-the-caption-says-it-is).)

The pair worth reading twice is `access` against `into_aligned`. At 64 B they are 1.6 ns apart —
validating an archive in place and copying every string out of it cost effectively the same thing on
a narrow row, which is why the zero-copy read looked like a rounding error when it was filed. At
64 KiB they are a factor of two apart. **The value of not copying is itself a per-byte quantity**,
and it was invisible to every benchmark that held the row width fixed.

**Which half of the mixture pays it** is now answerable, because the whole axis was swept again at
`r0` (no reads at all) and `r100` (no writes), each as a share of its own 64 B rate:

| | 1 KiB | 8 KiB | 64 KiB | 1 MiB | 4 MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `r0` — pure write | 91.1% | **58.8%** | 31.3% | 4.0% | 0.9% |
| `r100` — pure read | 106.5% | 96.7% | 37.9% | 1.2% | **0.3%** |

The two curves cross at about 64 KiB. Below it the write path is the one degrading and the read path
is essentially flat; above it the read path falls roughly three times as fast. **The near knee is a
write-path effect and the far tail is a read-path one**, which is the single most useful thing this
capture says about where to spend effort — and it points at
[O2](../appendix/optimizations.md#o2-every-returned-row-is-copied-at-least-twice), the response
copies, for anyone whose rows are hundreds of kilobytes.

*Evidence: the per-byte growth of the codec is **measured**, with criterion's confidence interval,
and the mixture split is measured against the persistent unsorted table. Which of the nineteen
stages the remaining copies live in is still unknown, and worse than unknown: the per-stage
breakdown at three widths was built, was run, and **joined nothing** — all three reports carry
`joined: 0` and no ops at all, because the client half of a stage record is only written by a driver
the grid arms do not use. Filed as
[item 76](../appendix/known-issues.md#76-the-stage-layer-joins-nothing-for-any-grid-arm-and-reports-it-as-a-layer-that-ran).
O11 and O29 are therefore blocked on a broken instrument rather than a missing one.*

### The intent log batches fewer records as rows widen

**Persistent tables only**, and the mechanism behind the 1 KiB → 8 KiB octave where only they move.
This section used to be titled *The intent log stops batching past the staging buffer*, and the
capture kept the mechanism while correcting the shape — see below.

`shoal.yml` sets `latency_sensitive.buffer_size: 4096`. The writer stages records into one aligned
buffer and flushes it when the next record will not fit (`fs/stream.rs:655-665`):

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

`write` then calls `alloc_buffer(new_usable)` (`:617`), a fresh `alloc_dma_buffer` sized to the
record. Fewer records per buffer means more DMA writes, more DMA allocations, and less for the group
commit to amortize the durability barrier across. Filed as
[O34](../appendix/optimizations.md#o34-a-record-wider-than-the-staging-buffer-defeats-intent-log-batching).

**The mixture split confirms this octave is the write path.** Over 1 KiB → 8 KiB the pure-write arm
falls to **58.8%** of its own 64 B rate while the pure-read arm is still at **96.7%**. The page
previously had to argue this from an even mixture, where a write effect is being asked half a
question.

#### ~~The behaviour is a step at the buffer size, not a slope~~ — it is a slope in records per buffer

The five `latency_buffer` rungs, now run at three widths instead of one
([F22](../features/row-size-benchmarks.md)). `>` marks a buffer larger than the row, which is where
the step was predicted:

| Buffer | 1 KiB rows | 8 KiB rows | 64 KiB rows |
| ---: | ---: | ---: | ---: |
| `512` | 50,492 | 36,433 | 20,885 |
| `4Ki` | > 53,285 | 36,465 | 18,537 |
| `16Ki` | > 52,587 | > 36,319 | 19,726 |
| `64Ki` | > 53,222 | > 38,419 | 20,535 |
| `256Ki` | > 52,499 | > 38,619 | > **22,701** |

**The setting is real and it grows with the row**: 1.06× across the sweep at the reference cell,
1.06× at 8 KiB, and **1.22× at 64 KiB** — `256Ki` at 2.46 ms against `4Ki` at 2.82 ms, on run
intervals that do not overlap. That is O34 adjudicated, and it is the answer the old sweep at 1 KiB
alone could not give.

**But the gain is not at the threshold.** At 8 KiB rows, crossing from a buffer that cannot hold one
record (`4Ki`) to one that holds two (`16Ki`) buys *nothing* — 36,465 against 36,319, which is
inside the noise. The gain appears only at `64Ki` and `256Ki`, where 8 and 32 records share a write.
So what matters is **how many records share an aligned write**, which is a gradual function, and not
**whether the record fits**, which would be a step. The old claim was wrong in the direction that
matters for acting on it: sizing the buffer just above your widest row does nothing, and sizing it
several times the row is what pays.

*Evidence: **measured**. The step-versus-slope claim was read from the source and is now corrected
by the sweep. What is still not measured is the boundary itself — the fixed widths jump 1024 → 8192
with nothing between them, so a discontinuity at 4096 would sit inside that gap unseen. Two arms at
2 KiB and 4 KiB would bracket it and are filed in
[TODOs](../appendix/todos.md#the-row-size-axis).*

### ~~A wide response blocks every narrow one behind it~~ — the tail was the queue

**This is the one mechanism the capture refutes**, and it is kept rather than deleted because the
reasoning that produced it was sound and the discriminator it proposed is what knocked it down.

The source reading stands. `client_tx_relay` (`shard.rs:200`) is one serial loop per connection: it
takes a response off the channel, writes it to completion with `write_vectored`, and only then looks
at the next one. There is one such task per client connection, and every shard's replies for that
client go through it.

~~The consequence is visible in the percentiles rather than in the medians. At 512 KiB the persistent
unsorted arm reads at a 333.55 µs p50 and a 15.19 ms p99 — a 45× spread on a table that is entirely
resident. A read that took 333 µs of work and 15 ms of wall clock spent the difference waiting, and
the thing in front of it was a multi-megabyte write to the same socket.~~

The page proposed its own test: *a p99 that collapses at one outstanding query and not at thirty two
is a queue in front of the relay rather than a cost in it.* The depth-1 ladder now runs at every
width, and it collapses:

| Row | p99/p50 at depth 1 | p99/p50 at depth 32 |
| ---: | ---: | ---: |
| 8 KiB | 1.3× | 2.0× |
| 16 KiB | 1.4× | 2.5× |
| 32 KiB | 1.5× | 6.7× |
| 64 KiB | 1.8× | **37.4×** |
| 128 KiB | 1.8× | **52.0×** |
| 512 KiB | 1.7× | 46.8× |
| 4 MiB | 2.5× | 7.4× |

At one outstanding query the spread stays between **1.3× and 2.5× at every width**, across a
65,536-fold change in row size. A cost inside the relay would still be there — it is per response,
and one query outstanding still writes a 512 KiB response through the same serial loop. It is not
there. The 45× the entry was filed on is the queue that thirty-two outstanding queries build in
front of the relay, and it is not evidence about the relay at all.

The medians say the same thing from the other side: a 512 KiB read is 119.37 µs at depth 1 and
360.04 µs at depth 32, and a 4 MiB read is 1.15 ms against 20.72 ms. At the wide end **roughly
eighteen nineteenths of the observed latency is queueing.**

[O35](../appendix/optimizations.md#o35-the-per-connection-response-relay-writes-one-response-at-a-time)
is not refuted as a mechanism — a wide response does block narrow ones behind it, whenever a queue
exists — but it now has no evidence of its own, and the thing it was ranked on belongs to load
depth. It is moved out of the priority queue for that reason.

*Evidence: **measured**, and the measurement contradicts the reading. This is the one place on the
page where a capture removed a claim rather than sharpening it.*

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

Read this section before quoting any number above as a knee. Four of the six limitations it opened
with have been answered; each is struck through with its answer rather than deleted, because a
reader who learned the old caveat needs to find out it moved.

~~**The axis has a 64× hole in it.** It goes 8 KiB → 512 KiB with nothing in between, so the
location and the sharpness of the knee are not measured.~~ **Filled.** Sixteen, 32, 64, 128 and
256 KiB ran on all four tables, and the knee turns out not to be in throughput at all — that falls
smoothly — but in the **tail**: the p99/p50 spread is 2.0× to 16 KiB, 6.7× at 32 KiB, 37.4× at
64 KiB and 52.0× at 128 KiB, then *falls back* to 7.4× at 4 MiB. Nothing in the old bracketing
predicted a peak in the middle of the axis.

~~**Load depth is fixed at 32 at every width.**~~ **Answered, and it was the dominant term.** The
depth-1 ladder runs at every width; see
[the relay section](#a-wide-response-blocks-every-narrow-one-behind-it--the-tail-was-the-queue).
At 4 MiB, roughly eighteen nineteenths of the observed read latency was queue rather than service.

~~**The axis is swept at `r50` only.**~~ **Answered.** Swept at `r0` and `r100` on all four tables,
and the two halves own different parts of the axis — write below ~64 KiB, read above it.

~~**Nothing says which of the nineteen stages grows with the bytes.**~~ **Still nothing does**, and
for a worse reason than before: the stage layer ran at three widths and joined *zero* queries at all
three, so the question was never asked rather than asked and unanswered
([item 76](../appendix/known-issues.md#76-the-stage-layer-joins-nothing-for-any-grid-arm-and-reports-it-as-a-layer-that-ran)).

What still stands:

**The 1 KiB → 8 KiB octave is not bracketed.** The fixed widths jump from 1024 to 8192 with nothing
between, and the intent log's staging buffer sits at 4096 in the middle of that gap. The octave is
measured; the boundary inside it is not, which is why the step-versus-slope question above is
settled by the `latency_buffer` sweep and not by the width axis.

**At the wide end the key space is smaller than the load is deep.** The 4 MiB arm seeds **64
partitions** and keeps 32 queries outstanding against them, across twelve shards. Contention on a
handful of partitions is folded into those numbers and cannot be separated out.

**Percentiles at the wide end are thin.** Every arm moves roughly the same number of bytes, so the
4 MiB arm runs a few hundred queries where the 64 byte arm runs twenty thousand. Read the p50 out
there; a p99 over two hundred samples is roughly its third-worst observation.

**The grid is a cross, not a cube.** `r0` and `r100` are swept at depth 32, and the depth-1 ladder is
swept at `r50` on one table, so a cost that appears only under a pure write mixture at one query
outstanding is invisible to both — and the persistent-against-ephemeral subtraction that makes a
width effect attributable to storage does not exist for the depth axis.

## What to do today

Two of these three now have a number behind them, and the first one has changed.

- **Size `latency_sensitive.buffer_size` to several times your widest row** — not merely above it.
  ~~Above your widest row, so the intent log can batch again.~~ The sweep says a buffer that holds
  *one* record buys nothing over one that holds none: at 8 KiB rows, `16Ki` and `4Ki` are within
  noise of each other, and the gain arrives at `64Ki` and `256Ki` where 8 and 32 records share a
  write. Worth **1.22×** at 64 KiB rows. It is a minimum that gets rounded up to the device's
  O_DIRECT alignment; the cost is padding on a partial flush and a larger DMA allocation per shard.
  See [Tuning](../operations/tuning.md#if-your-rows-are-wide).
- **Keep the load depth down when rows are wide** — this is the largest lever on this page. At
  512 KiB, dropping from 32 outstanding queries to one takes the read p50 from 360.04 µs to
  119.37 µs and the p99 from 16.85 ms to 201.54 µs. At 4 MiB it is 20.72 ms to 1.15 ms. Nothing else
  here is worth a factor of eighteen.
- **Split a wide row rather than storing it whole**, if the reads do not need all of it.
  [Projections](../features/projections.md) narrow what a get copies and what the wire carries, and
  the codec sweep now says what that is worth per byte: a response's encode grows ×72 and its decode
  ×433 between 64 B and 64 KiB, so narrowing what crosses the wire is a per-byte saving on both ends.

## What it settled — five of six ran

Six benchmarks, none of which existed when this page was first written. All six were built
([F22](../features/row-size-benchmarks.md)); `f22-row-size` ran the capture. **Five answered and one
did not run at all.** Ordered as they were filed, cheapest first.

| # | What it was to settle | What it said |
| ---: | --- | --- |
| 1 | The per-byte half of O1 and O2 | **Answered.** Response decode grows ×432.8 and encode ×72.2 over 64 B → 64 KiB, against a control flat to a quarter of a percent. `access` and `into_aligned` are 1.6 ns apart at 64 B and 2× apart at 64 KiB |
| 2 | O34 — is `latency_buffer` a step, and is the step at the buffer | **Answered, and the shape was wrong.** Worth 1.22× at 64 KiB rows on disjoint intervals, but the gain is in records per buffer rather than at the threshold |
| 3 | Where the knee is | **Answered, and it was not where the question assumed.** Throughput falls smoothly; the tail peaks at 52× at 128 KiB and recovers past it |
| 4 | How much of a wide arm was queue rather than service | **Answered, and it was most of it.** The p99/p50 spread is 1.3–2.5× at every width at depth 1. Eighteen nineteenths of the 4 MiB read latency was queue |
| 5 | Which half of the mixture the per-byte cost is on | **Answered, and it is both, in different places.** Write below ~64 KiB, read above it |
| 6 | **Which** of the nineteen stages grows with bytes | **Did not run.** All three reports joined zero queries — [item 76](../appendix/known-issues.md#76-the-stage-layer-joins-nothing-for-any-grid-arm-and-reports-it-as-a-layer-that-ran) |

**What this changed in the priority queue.** O34 moved from argued to measured with a contained fix
and is now the head of Tier A. O2 gained a measurement on the half that grows in what the caller
controls, and `r100` says the read path owns the wide end, so it is the largest established win
available — still behind a design pass, because it reaches the wire format. O35 lost its evidence
and left the queue. O11 and O29 did not move, and their unblocker is now known to be broken rather
than merely unbuilt. See [the priority queue](../appendix/optimizations.md#the-priority-queue).

**What none of the six closed** is in
[What the capture cannot tell you](#what-the-capture-cannot-tell-you) above, and the new item is
the 1 KiB → 8 KiB octave: filling the 64× hole left the *other* gap, the one with the staging
buffer in the middle of it. Two arms at 2 KiB and 4 KiB would bracket it, and are filed in
[TODOs](../appendix/todos.md#the-row-size-axis).

## Related

- [Row size](../performance/row-size.md) — the generated numbers this page explains
- [The Intent Log](../storage/intent-log.md) — the staging buffer and the durability barrier
- [Memory and Eviction](memory-and-eviction.md) — why eviction is not what makes a wide row slow
- [Partitions](partitions.md) — where the copies on the read path happen
- [Request Lifecycle](../architecture/request-lifecycle.md) — the path the payload is copied along
- [Tuning](../operations/tuning.md) — what to set, and what rests on a measurement
- [F22](../features/row-size-benchmarks.md) — the six benchmarks this page asked for, built
- [Optimizations](../appendix/optimizations.md) — O1, O2, O11, O29, O34, O35, and the queue they
  are ordered in
- [Known Issues](../appendix/known-issues.md) — items 75 and 76, both found in this capture
