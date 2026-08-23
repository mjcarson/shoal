# Tuning

[Configuration](../getting-started/configuration.md) says what every setting in `shoal.yml` **is**.
This page says what to set it to, and every piece of advice on it points at the row of
[Configuration and what each setting is worth](../performance/configuration.md) that it rests on.

That page is generated from a capture. This one is written by hand, and the split is deliberate: the
numbers go stale on their own schedule and are regenerated with `shoal-bench render`, while the
judgement about what they mean is written once and revised when it is wrong.

## Read the *Real?* column first

Nine sweeps produce nine recommendations whether or not any of them means anything. The last column
of the *What the data says* table is the gate: a setting whose fastest and slowest arms have
**overlapping** run intervals has not been shown to have a fastest and slowest arm, however far apart
their medians look. The frozen baseline spread 10.5% over five identical runs of identical code, so a
gap smaller than that is a coin toss with a decimal point.

**A `no` in that column is a finding, not a gap.** It says this workload's time does not go there,
which is worth more than a recommendation would have been: it tells you which knob not to spend the
afternoon on.

## Start here

Do not tune anything until you know which half of the mixture you are bound by. The isolating
workloads on [Table types](../performance/table-types.md) separate the read path from the write path;
[Access patterns](../performance/access-patterns.md) says whether you are past the knee of the
throughput curve, which is the condition under which **none** of the advice below applies, because
past the knee a latency is a measure of queue depth and not of work.

## If your writes are small and latency-bound

The intent log is the whole write path from the client's point of view: a write is acknowledged when
the durability barrier says so, and everything else is behind that.

- **`durability`** is the largest single decision. `Fsync` waits for the fdatasync; `Async`
  acknowledges once the kernel has taken the write, which is faster and means a write can be
  acknowledged and then lost to power loss. At most one fdatasync is in flight at a time, so
  concurrent writers group-commit behind the one already running and **the cost per write falls as
  load rises** — measure it under your own load, not under one writer.
- **`latency_sensitive.write_behind`** is the io_uring queue depth for that path. The writer stalls
  until a completion drains once this many writes are outstanding, so a low value serialises the
  write path outright. The shipped 128 is not a number anybody measured before F20; read the
  write-behind ladder and take the value where it flattens.
- **`latency_sensitive.buffer_size`** is a **floor** that gets rounded up to your device's O_DIRECT
  alignment, so setting it below the block size does nothing at all. It is no longer the buffer
  size: `StreamWriter` sizes each staging buffer to hold about eight of the widest record the last
  one held, between this floor and the `max_buffer_size` ceiling below
  ([F23](../features/self-sizing-staging-buffer.md)). ~~A record larger than the buffer is never
  batched with another one at all.~~ ~~Size it to a multiple of your widest row, not just past
  it.~~ **That is what the writer now does for you**, so raising this is only worth it if you want
  a larger *minimum* write at low load. The measurement behind the rule is the same five rungs run
  at 8 KiB and 64 KiB ([F22](../features/row-size-benchmarks.md)): the setting was worth **1.22× at
  64 KiB rows** (`256Ki` against `4Ki`, on run intervals that do not overlap) against 1.06× at
  1 KiB, and at 8 KiB rows a buffer holding *two* records was worth nothing over one holding none —
  the gain arrived at 8 to 32 records per buffer, which is why the rule is eight
  ([O34](../appendix/optimizations.md), [Row size](../tables/row-size.md)).
- **`latency_sensitive.max_buffer_size`** is the ceiling that sizing stops at, 256 KiB by default,
  and it is the setting that matters if your rows are wide. Above it the writer is back to one
  record per DMA write and one DMA allocation per insert, with nothing left for the group commit to
  group — so if your rows are wider than a quarter of a mebibyte, this is the first setting to move.
  What it costs is memory: a writer may hold up to `write_behind + 1` buffers of this size at once,
  per table, per shard, and `write_behind` defaults to 128. Setting it equal to `buffer_size` turns
  the sizing off entirely, which is the way back to the pre-[F23](../features/self-sizing-staging-buffer.md)
  behaviour. **The sizing is captured** (`f23-staging-buffer`: +22.0% at 64 KiB rows for the shipped
  floor, and the whole `latency_buffer` sweep flattening from 1.225× of spread to 1.010×). **Its
  interaction with `write_behind` is not**, and nothing sweeps `max_buffer_size` at all.

## If you are ingesting in bulk

Throughput, not service time, so read the `queries/s` column rather than the percentiles.

- **`intent_log_size`** decides how often compaction runs. A larger log compacts less often and holds
  more unarchived data, which is a recovery-time cost rather than a steady-state one.
- **`throughput_sensitive.*`** is, today, not the dial it looks like:
  [item 71](../appendix/known-issues.md) records that it reaches the archive *map's* intent log and
  not the archive writers themselves, which use glommio's defaults. The sweep is on the page and it
  is expected to be flat. Do not spend time on this section until that item is closed.
- **`networking.max_frame_bytes`** bounds one batch, and a frame length is used as an allocation size
  before the body arrives — so it is a bound on what one client can make a shard allocate as much as
  it is a bound on a batch. Lower it if you do not trust your clients; the sweep says what it costs
  a client that batches.

## If your rows are wide

Read [Row size and what it costs](../tables/row-size.md) first — it says which parts of this are
measured and which are argued. ~~For this section the honest answer is *none of it is measured at the
width it matters at*.~~ **Most of it is measured now**, by `f22-row-size`: the width axis with its
64× hole filled, swept at three mixtures and two load depths, plus the `latency_buffer` rungs above
the buffer ([F22](../features/row-size-benchmarks.md)). The order of these two bullets has swapped,
because the capture said the second one is much the larger lever — and the second one has since
mostly stopped being your job at all
([F23](../features/self-sizing-staging-buffer.md)), which is what the numbers behind it bought.

- **Keep the load depth down — this is the biggest thing on the page.** At 512 KiB rows, going from
  thirty-two outstanding queries to one takes the read p50 from 360.04 µs to 119.37 µs and the p99
  from **16.85 ms to 201.54 µs**. At 4 MiB it is 20.72 ms down to 1.15 ms. Nothing else here is worth
  a factor of eighteen. The p99/p50 spread at depth 32 peaks at **52×** around 128 KiB rows and is
  1.3–2.5× at every width at depth 1, so if your wide-row tail looks pathological, look at your queue
  before you look at the server. (This bullet used to blame the response relay
  ([O35](../appendix/optimizations.md)); the depth ladder showed the queue accounts for all of it,
  and that entry has been demoted.)
- ~~**Size `latency_sensitive.buffer_size` to a multiple of your widest row.**~~ ~~Above your widest
  row.~~ **The writer sizes its own buffer now** ([F23](../features/self-sizing-staging-buffer.md)),
  to about eight of the widest record it last held. What is left for you is
  `latency_sensitive.max_buffer_size`, the ceiling that sizing stops at — **and only if your rows
  are wider than its 256 KiB default**, because above the ceiling the old behaviour is still exactly
  what happens: one DMA write and one DMA allocation per insert. Raising it costs up to
  `write_behind + 1` buffers of that size per table per shard. The measurement that produced the
  rule is the same one this bullet used to quote: above your widest row is not enough — at 8 KiB
  rows `16Ki` and `4Ki` are within noise of each other and the gain shows up at `64Ki` and `256Ki`,
  worth **1.22×** at 64 KiB rows.
- **Read `payload/s`, not `queries/s`.** Past the point where the per-byte cost dominates, queries a
  second must fall as the rows widen and that is arithmetic rather than a regression. A falling
  `queries/s` with a flat `payload/s` is the system working.
- **Consider a projection instead of a narrower row.** [Projections](../features/projections.md)
  reduce what a get copies and what the wire carries without changing what is stored, and they are
  the only lever here that is measured.

## If you are read-mostly with a working set larger than memory

- **`resources.memory` is a per-shard budget**, not a node-wide one. A twelve-shard server at `4Gi`
  is holding 48 GiB. This is the single most common way to size a deployment wrong by an order of
  magnitude.
- The memory sweep is a **cliff**, not a curve: flat while the working set fits, stepping once a read
  has to find its partition on disk. The number to take off it is *where the step is*, and if no rung
  steps, the capture's working set fit inside the smallest limit measured and the cliff is somewhere
  the sweep did not go.
- Exceeding the limit evicts 40% of current usage, and a partition cannot be evicted until its
  generation has been compacted — so a server that is evicting is doing compaction work as well as
  read work, and both show up in the same latency.

## If you are sizing a machine

- **`resources.cores`** sets the shard count. Read the scaling curve for where it stops rising rather
  than for what it reaches, and note the confound the page states: the benchmark client shares the
  machine, so the low end of that curve runs under less contention than the high end and the curve
  flatters small configurations. On a machine where the client is elsewhere, expect the high end to
  do better than the chart shows.
- **`exclude_cores` filters on the physical core id**, so excluding one removes both of its SMT
  threads. The committed `shoal.yml` excludes four cores to leave the benchmark client somewhere to
  run; a production deployment usually should not.
- Changing the shard count against an existing data directory is **refused** — `StorageMeta::claim`
  keys a directory to the count that wrote it. Plan the count before the first write, or migrate.

## What none of this can tell you

**How two settings behave together.** Each knob is swept against a fixed reference of every other, so
an interaction — a write-behind depth that only pays off at a large buffer — appears as two flat
sweeps. This is the same cross-not-cube trade [F17](../features/workload-grid.md) makes for the grid.

**Anything about your hardware.** A buffer size is rounded to your device's alignment and the shard
curve is a curve in one box. The shapes may transfer; the numbers do not.

**What your workload costs.** The sweep runs one reference mixture — 1 KiB rows, an even read/write
share, thirty-two outstanding queries. If yours is 4 MiB rows at `r95`, start from
[Read/write mixtures](../performance/grid.md) and [Row size](../performance/row-size.md), and treat
the tuning advice here as the shape of the answer rather than the answer.

## Measuring your own

The sweep is a group, so you can re-run one half of it without spending a day:

```bash
# what exists, and what each would cost
cargo run -p shoal-bench --release -- list --groups

# the writer knobs alone, against your own shoal.yml
cargo run -p shoal-bench --release -- run --label mine --group conf/storage --conf my-shoal.yml

# then regenerate the page
cargo run -p shoal-bench --release -- render
```

Capture on a clean tree with the CPU governor set to `performance`; see
[Benchmarking](../performance/benchmarking.md) for why, and for what a capture taken any other way is
worth.

## Related

- [Configuration](../getting-started/configuration.md) — what each setting is
- [Configuration and what each setting is worth](../performance/configuration.md) — the measurements
- [F20. What each setting is worth](../features/configuration-sweeps.md) — how the sweep is built
- [Memory and eviction](../tables/memory-and-eviction.md) — what the limit actually governs
- [Row size and what it costs](../tables/row-size.md) — where the numbers behind the advice above come from, and which parts still have none
- [Benchmarking](../performance/benchmarking.md) — how to take a capture that means something
