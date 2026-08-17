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
- **`latency_sensitive.buffer_size`** is a **minimum** that gets rounded up to your device's O_DIRECT
  alignment, so setting it below the block size does nothing at all. Above it, a larger buffer means
  fewer, larger writes and more padding per partial flush.

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
- [Benchmarking](../performance/benchmarking.md) — how to take a capture that means something
