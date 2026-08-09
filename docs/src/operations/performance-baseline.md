# Performance Baseline

What Shoal currently does, on known hardware, so that a later change can be shown to have
helped rather than argued to have. [Benchmarking](benchmarking.md) is how to run one;
[F3](../features/performance-harness.md) is why the harness has the shape it does.

Everything on this page is measured. Where a number is quoted from before the XFS migration it
says so.

## Hardware

Every figure below came from this machine. None of them transfer to a different one, and one
detail matters more than the rest: **the storage is an Intel Optane SSD**, whose fsync latency
is roughly an order of magnitude below a consumer NVMe. Any conclusion drawn here about whether
the fsync is worth optimizing does not carry to a machine with an ordinary drive.

| | |
| --- | --- |
| **CPU** | AMD Ryzen 9 9950X — 16 physical cores, 32 threads, 1 socket, 1 NUMA node, boost to 5,756 MHz |
| **Cache** | 768 KiB L1d, 512 KiB L1i, 16 MiB L2, **64 MiB L3 across 2 instances** — two shards on opposite CCDs do not share L3 |
| **Memory** | 59 GiB, 8 GiB swap |
| **Storage** | `/opt/shoal` is on `/` = `/dev/nvme1n1p2` = **Intel Optane SSD `SSDPED1D480GAH`**, 447 GB. The machine also holds a Samsung 990 PRO; Shoal is not on it. |
| **Filesystem** | XFS — `rw,relatime,inode64,logbufs=8,logbsize=32k,noquota`, bsize 4096, sectsz 512, 32 allocation groups, internal log, `crc=1 reflink=1` |
| **Kernel** | Linux 7.0.0-29-generic |
| **Toolchain** | rustc 1.99.0-nightly (`1a98b1e13`, 2026-08-07), LLVM 23.1.0, `-Ctarget-cpu=native` |
| **CPU governor** | `performance` / EPP `performance` / `amd-pstate-epp`, boost enabled. This is a **precondition of the baseline**, not an observation — see [Benchmarking](benchmarking.md#prerequisites). |
| **Mitigations** | Enhanced/Automatic IBRS, IBPB conditional, STIBP always-on, SSB via prctl |
| **Allocator** | `mimalloc`, in the benchmark client |

> The baseline was originally captured under the `powersave` governor, on the reasoning that a
> baseline depending on an unrecorded system setting is not reproducible. That was answered by
> recording the setting rather than by accepting it. **The re-capture is what makes this
> section worth reading**: the governor turned out not to matter, which was not the expectation.
> See [what the governor changed](#what-the-governor-changed).

### Server configuration

The committed `shoal.yml`. Changing it invalidates everything below.

```yaml
resources:
  cores: 12
  exclude_cores: [12, 13, 14, 15]
  memory: "4Gi"
storage:
  default:
    filesystem:
      latency_sensitive:
        path: "/opt/shoal"
        buffer_size: 4096
```

Twelve shards on twelve distinct physical cores, cpu 0 left to the coordinator, and physical
cores 12–15 — cpus 12–15 and 28–31 — left entirely to the client, which pins its tokio workers
to 28–31. `durability` is at its default of `Fsync`.

## B1 — the frozen baseline

Captured 2026-08-09 by `scripts/bench.sh B1-performance`, under the `performance` governor.
Frozen at `docs/perf/baselines/B1-performance.json`; the raw run is in `docs/perf/runs/`.

The earlier `powersave` capture is kept as `B0-powersave.*` — not as a superseded baseline but
as the other half of a controlled comparison. Nothing in the source tree changed between the
two, so the governor is the only variable.

### End to end

The `tmdb` example over the full 99,999-row dataset, `--warmup 5000`, storage wiped before each
run, median of 5.

| | |
| --- | --- |
| Wall clock (median of 5) | **1,801.5 ms** |
| Run-to-run spread | **10.5%** (1,699.7 ms – 1,877.6 ms) |
| Rows moved | 447,251 inserted, 99,999 read |
| Throughput | **≈ 303,800 rows/sec** |

| Operation | min | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| insert | 423.6 µs | 49.7 ms | 183.3 ms | 262.3 ms | 345.1 ms |
| get | 21.4 µs | 128.7 µs | 374.5 µs | 0.65 ms | 1.07 ms |

Read these with [what is actually measured](benchmarking.md#what-is-actually-measured) in mind —
timing is per batch, so `min` is roughly the true service time and `p50` is dominated by
queueing inside the batch.

The 10.5% spread is worth noting on its own. The previously recorded figure was **±30%**, at a
fifth of the scale. Three things changed: the run is 5× longer, there is a warmup phase, and
shard placement is no longer decided by a hash seed
([Resolved #18, #50](../appendix/resolved/excluded-cores-typo.md)). It is still far too wide to
resolve the kind of change most of [Optimizations](../appendix/optimizations.md) proposes, which
is what the micro layer is for. Note that the governor did **not** contribute to it — the
powersave capture's spread was 10.8%.

### Micro — the partition layer

Criterion, mean of the sampled distribution. Read
[what this layer can actually resolve](#what-the-micro-layer-can-actually-resolve) before
treating any small difference as real — the noise band is 5–9% depending on how fast the
benchmark is.

**Insert**, into a partition already holding *n* rows:

| n | time |
| --- | --- |
| 16 | 98.9 ns |
| 256 | 169.0 ns |
| 1,024 | 141.0 ns |
| 4,096 | 181.4 ns |

**Get**, resident partition:

| n | one key | whole partition | 64-row range |
| --- | --- | --- | --- |
| 16 | 23.1 ns | 518.4 ns | 374.5 ns |
| 256 | 35.1 ns | 12.79 µs | 3.073 µs |
| 1,024 | 38.3 ns | 51.86 µs | 3.091 µs |
| 4,096 | 51.3 ns | 208.9 µs | 3.100 µs |

**The range column is flat.** 3.07 µs, 3.09 µs, 3.10 µs across a sixteen-fold increase in
partition size. That is [F1](../features/sort-key-ranges.md)'s central claim — paging a large
partition costs a page, not a partition — measured rather than argued, for the first time.

**Get**, archived partition (evicted, read where it lies):

| n | access + one row | walk everything | `access` alone |
| --- | --- | --- | --- |
| 16 | 148.0 ns | 543.7 ns | 117.7 ns |
| 256 | 1.912 µs | 14.70 µs | 1.890 µs |
| 1,024 | 7.611 µs | 59.89 µs | 7.558 µs |
| 4,096 | 30.43 µs | 240.1 µs | 29.89 µs |

Two findings, in opposite directions.

**~~A single-row cold get is catastrophically worse than a resident one, and it is all
validation.~~ It was, and it is not any more.** At 4,096 rows this read 51.3 ns resident against
30.43 µs archived, a factor of 590, and the third column said why — `access` alone was 29.89 µs of
that 30.43 µs. rkyv validated the entire buffer before anything could be sought in it, so the query
paid for the size of the partition it landed in and the seek was rounding error. Filed as
[O23](../appendix/optimizations.md), taken as [F4](../features/validated-archives.md): a cold keyed
get is now **138 ns at 4,096 rows** and flat across partition size. The table above is kept because
it is what the numbers were when the entry was filed, and because those three ids still measure
`RkyvSupport::access` directly — they are now the *control*, not the read path. The read path is
`partition_sorted/maybe_loaded/*`.

**A full scan of a cold partition is barely worse than a resident one.** 240.1 µs against
208.9 µs at 4,096 rows — 15%. Amortised over every row, the validation is nearly free. This one
survived F4 unchanged in shape: `maybe_loaded/get_all/4096` improved only 10%, because a walk that
visits every row was already amortising what a keyed get was paying in full.

Those two together said something the eviction policy still does not know: what eviction costs a
reader depends almost entirely on the access pattern, not on the partition. F4 narrowed the range
but did not close it — a cold `get_all` is still 225 µs against a resident 212 µs, and a cold
`get_key` is now 138 ns against a resident 51 ns.

**Codec**, per partition:

| n | serialize | access |
| --- | --- | --- |
| 16 | 297.6 ns | 117.7 ns |
| 256 | 2.344 µs | 1.890 µs |
| 1,024 | 8.947 µs | 7.558 µs |
| 4,096 | 37.24 µs | 29.89 µs |

**`SeekBytes::new`** — paid once per query that touches an archived partition:

| Selection | B1 | after [F4](../features/validated-archives.md) |
| --- | --- | --- |
| one key | 54.5 ns | 74.4 ns |
| a bounded range | 103.5 ns | 122.0 ns |
| sixty-four keys | 3.049 µs | 3.646 µs |

A range archives at most two values however wide it is; a set of keys archives all of them.

The second column is a deliberate regression. `SeekBytes` now validates each key it archives, so
that a seek does not validate it again per partition — *k* validations for a query naming *k* keys
across *p* archived partitions, against *k × p*. This is the same trade F4 made for the partition
itself, on the key.

### What the governor changed

Almost nothing, which was not the expectation. The `powersave` capture and the `performance`
capture are the same source tree with the same config; the governor is the only variable.

| Layer | powersave | performance | Change |
| --- | --- | --- | --- |
| Macro wall clock (median of 5) | 1,814.9 ms | 1,801.5 ms | **−0.7%** |
| Macro throughput | 301,500 rows/sec | 303,800 rows/sec | **+0.7%** |
| Macro run-to-run spread | 10.8% | 10.5% | **−0.3 pt** |
| Micro, median across 35 benchmarks | — | — | **+0.14%** |
| Micro, range across 35 benchmarks | — | — | −3.0% to +5.1% |

Every one of those sits inside the noise band of the layer it belongs to, and the micro
benchmarks moved in **both directions** — which is what noise looks like and what a real
speed-up does not.

This page previously asserted that the governor widened the spread. **That was a guess, and it
was wrong.** Two reasons it was wrong, in hindsight:

- `amd-pstate-epp` ramps to boost clocks quickly under sustained load, and both layers apply
  sustained load. Criterion warms up for 3 s and measures for 5 s; the macro run is 1.8 s of
  saturation. Neither gives the governor an idle moment to save power in.
- The macro workload is not CPU bound at all. `write_helper` averages 30–33 ms per call
  against 344 ns for the partition insert it persists, so the wall clock is set by the storage
  device. Making the CPU faster does not move a number that is waiting on an `fdatasync`.

The `hotpath` profile does show a consistent 6–9% drop across most scopes. **That is not being
counted as evidence**, and the rule it runs into is one this page set for itself: a hotpath
build is attribution only, it is a single capture rather than a median of five, and its
instrumentation perturbs exactly the CPU-bound work the governor would affect. It is
suggestive; it is not a result. If the governor really is worth 6–9% on instrumented CPU work,
the way to show it is a micro-benchmark repeat, and the micro benchmarks say +0.14%.

The governor stays on `performance`. Not because it was shown to help — it was not — but
because it removes a variable at no cost, and the objection to setting it (that it makes the
baseline depend on an unrecorded setting) is answered by recording it.

### What the micro layer can actually resolve

Criterion reports a confidence interval of ±0.02%–1.7% on these benchmarks. **That interval is
not the reproducibility of the measurement, and using it as one is a mistake.** It describes how
stable the samples were *inside one process*. Everything that differs *between* processes —
allocator layout, code and data alignment, ASLR, thermal state — is invisible to it by
construction. The CPU governor was on that list until it was measured; see
[what the governor changed](#what-the-governor-changed).

Running the same build four times, changing nothing:

| | Cross-run spread |
| --- | --- |
| Median benchmark | **3.0%** |
| p90 benchmark | **5.7%** |
| Worst benchmark (`get_key/1024`) | **8.8%** |
| Benchmarks moving more than 7% | 2 of 35 |

**Instability scales inversely with how long the benchmark takes**, and cleanly enough to act
on:

| Benchmark duration | n | median spread | worst |
| --- | --- | --- | --- |
| under 100 ns | 6 | 4.6% | 8.8% |
| 100 ns – 1 µs | 10 | 3.4% | 7.8% |
| 1 – 20 µs | 12 | 1.8% | 3.8% |
| over 20 µs | 7 | 1.2% | 4.3% |

That shape makes sense: a fixed-cost perturbation — a cache line landing differently, a branch
predictor entry, a page boundary — is a large fraction of a 25 ns benchmark and a rounding error
in a 200 µs one. A single global threshold is therefore either too loose for the slow
benchmarks or too tight for the fast ones, so `scripts/compare.sh` applies a tiered band:

- **±9%** below 1 µs
- **±5%** at or above 1 µs

Checked against the four repeats, that produces **3 false positives out of 140 comparisons**
(2.1%) — `codec/serialize/4096` twice and `get_key/16` once.

A prior capture under the `powersave` governor showed a median of 1.7% and a single 22.4%
excursion on `get_key/4096`. That excursion did not recur. It should not be read as the governor
having fixed anything: one extreme event in a set of four runs against another set of four is
not a comparison, and the [governor measurement](#what-the-governor-changed) found no effect
anywhere else. The honest summary is that the band is somewhere around 5–9% depending on
benchmark duration, that individual excursions well beyond it happen, and that the number of
samples behind all of this is small.

What follows for using this layer:

- The band is a **screen, not a verdict**. It does not bound the worst case.
- **Confirm any apparent win by repeating the whole capture.** A single capture has produced a
  22% false signal. Two captures agreeing is the actual evidence.
- **A tight confidence interval is not evidence.** The 22.4% excursion was reported with a
  ±0.2% interval, tighter than any of the runs it disagreed with.
- Be most suspicious of the fastest benchmarks, which is where the noise concentrates.

This is worse than the ±1% the confidence intervals suggest, and it is still more useful than
the macro layer — not because the number is smaller, but because it is per benchmark. A movement
here names the function that moved; an 11% movement end to end names nothing.

Four repeats per governor remains a thin basis for a number this load-bearing. Characterising it
properly, and teaching `compare.sh` to take several captures per side rather than one, is filed
in [TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have).

The eight captures behind this section are kept in `docs/perf/repeats/`, four per governor, so
the claim can be recomputed rather than taken on trust.

### Profile — where the time goes

A separate `--features hotpath` build over the same workload. **These are attribution only and
are not comparable to the numbers above** — the instrumented binary takes two timestamps around
every scope, and its own `tmdb::main` reads 12.06 s against the uninstrumented run's 1.81 s.

Percentages exceed 100% because twelve shards run concurrently: the figure is the summed
duration of a scope against wall clock, so anything above 100% is work happening in parallel.

| Scope | Calls | Avg | % of wall clock |
| --- | --- | --- | --- |
| `stream::write_helper` | 49,633 | 30.5 ms | 12,530% |
| `shard::handle_query` | 617,175 | 12.2 µs | 62% |
| `stream::write` | 49,633 | 104.7 µs | 43% |
| `stream::flush_oldest_write` | 49,633 | 103.5 µs | 43% |
| `fs::commit` | 512,175 | 9.0 µs | 38% |
| `unsorted::handle` | 209,998 | 21.4 µs | 37% |
| `unsorted::insert` | 104,999 | 40.4 µs | 35% |
| `stream::prep` | 512,175 | 8.2 µs | 35% |
| ~~`shard::handle_flushed`~~ | ~~705,886~~ | ~~1.8 µs~~ | ~~11%~~ |
| `sorted::handle` | 407,177 | 2.7 µs | 9% |
| `fs::flush` | 204,190 | 4.7 µs | 8% |
| `sorted::insert` | 407,176 | 1.9 µs | 7% |

`write_helper` — the DMA write and the `fdatasync` behind it — dominates everything, at 30.5 ms
per call against roughly 350 ns for the partition insert it is persisting. That is five orders
of magnitude, and it is the single most important fact on this page: **the write path is waiting
on storage, not on CPU.** `flush_oldest_write`, the back-pressure wait when the writer is at its
`max_write_behind` cap, is another 103.5 µs per call.

It is also the reason the governor changed nothing end to end. A workload whose wall clock is
set by an `fdatasync` does not get faster when the CPU does.

The practical consequence is that most of [Optimizations](../appendix/optimizations.md)'s write
path entries are proposing to remove work from a path that is already waiting. Read path
entries are a different matter — `handle_query` at 62% is real, and O23 sits inside it.

That conclusion is now load-bearing rather than an aside: it is what orders
[Tier B of the priority queue](../appendix/optimizations.md#the-priority-queue), where every
write-path entry sits below the two whose cost scales with data on disk instead of with the request
rate. `shard::handle_flushed` on the row above is the other thing this table decided — 705,886
calls against 617,175 queries put it at rank **A2**.

**That row is struck because [F5](../features/flushed-sweep-gate.md) closed it.** The shard now
sweeps its tables on a wakeup rather than on every message, and the scope reads **21,279 calls at
21.3 µs, 375% of wall clock** — 97.0% of the calls and 66% of the time gone. The rise in the average
is not a regression: the calls that survive are the ones that release responses or rotate a log, and
the no-ops that used to drag the average down are what was removed. The row is kept struck rather
than updated because this table is the `B1` capture and `B1` is never overwritten.

**This is also the clearest case on the page of a change the macro layer could not see.** The whole
scope was ~112 ms per shard against a 1.8 s run, on the instrumented build that inflates it, and the
write path's wall clock is set by the `fdatasync` two rows up. The call count was the result; the
macro capture was taken to show nothing regressed, which is all it was ever going to say.

## History

Each accepted optimization adds a row carrying **both** deltas: against the frozen baseline,
which says what has been gained in total, and against the trailing baseline, which says what
that change alone did. See [the comparison protocol](benchmarking.md#comparing-runs).

| Date | Change | vs frozen | vs trailing | Notes |
| --- | --- | --- | --- | --- |
| 2026-08-08 | **B0-powersave** — first capture | — | — | XFS, 12 shards, deterministic placement, `-Ctarget-cpu=native`, `powersave` governor. Superseded before any optimization was measured against it; kept as the control for the governor comparison. |
| 2026-08-09 | **B1-performance** — frozen baseline | — | — | Identical tree, `performance` governor. Macro −0.7%, micro median +0.14% — [inside the noise on both layers](#what-the-governor-changed). |
| 2026-08-09 | **F5** — [the flushed sweep runs on a wakeup, not on every message](../features/flushed-sweep-gate.md) ([O17](../appendix/optimizations.md)) | micro unchanged — no id can reach this code | micro 57 of 59 ids inside the band in each capture, and **the two outliers were different ids each time**; macro indistinguishable under an interleaved A/B | The result is the profile: `shard::handle_flushed` **711,638 → 21,279 calls**, 1.344 s → 0.453 s summed over twelve shards. Trailing promoted from `o17-after-repeat`. The macro layer needed [an extra experiment to be believed](../features/flushed-sweep-gate.md#performance) — three sequential captures drifted upward monotonically (1.800 → 1.833 → 1.859 s), which a change that only removes work cannot cause, so both binaries were run interleaved minutes apart: gated 1,823 ms, ungated 1,820 ms, gated 1,857 ms. The two gated passes differ by more than either differs from the ungated one. The drift was the machine over a long session, and it is recorded rather than smoothed over. Captured under `powersave`/EPP `performance`, as F4 was. |
| 2026-08-09 | **F4** — [archives validated once, not once per read](../features/validated-archives.md) ([O3 + O23](../appendix/optimizations.md)) | new ids | `maybe_loaded/get_key/4096` **−99.5%**, `exists_key/4096` **−99.6%**, `get_range_64/4096` **−89%**, `get_all/*` −10 to −11%; `seek_bytes/new/one_key` **+37%** by design; macro +0.8% (inside spread) | Trailing promoted from `o3-after-repeat`, the confirming second capture. A cold keyed get stopped being O(partition): 106→138 ns across 16→4,096 rows, against 218→28,764 ns. `seek_bytes/new` pays one validation per key so that a seek pays none per partition. The macro layer [cannot see this change](../features/validated-archives.md#performance) — the `tmdb` workload evicts nothing, so `SortedPartition::get` fires once in a whole run. Captured under `powersave`/EPP `performance`; unrelated ids sit within noise of B1, so the environments agree. Two control ids drifted +9–13% — [O24](../appendix/optimizations.md#o24-two-benchmarks-move-with-the-shape-of-the-binary-around-them). |

## Reproducing this

```bash
scripts/bench.sh B1-performance
scripts/compare.sh docs/perf/runs/B1-performance.micro.json \
    --against docs/perf/baselines/B1-performance.json
```

`docs/perf/baselines/B1-performance.json` is never overwritten. If a capture on the same tree
differs from it by more than the noise band, something about the machine changed and the
difference is not Shoal's — the first thing to check is the CPU governor, which is the one
precondition that lives outside the repository.

## What this does not cover

- **One machine, one filesystem, one device**, and that device is an Optane.
- **No btrfs counterpart.** The migration happened before any baseline existed, so there is no
  before-and-after for the filesystem change that prompted all of this. The btrfs-era numbers
  that survive are the two tables in [Benchmarking](benchmarking.md#latency-that-is-just-the-queue-you-asked-for),
  taken at a fifth of the scale on 4 shards, and are not comparable.
- **No `Async` vs `Fsync` comparison yet**, which is the one comparison that would isolate the
  cost of the durability barrier `write_helper` spends its time in. Filed in
  [TODOs](../appendix/todos.md).
- **No storage write path micro-benchmark.** The layer that dominates the profile is the one
  layer with no confidence interval around it.
- **No client-side profile.** `client.rs` has neither `tracing` spans nor `hotpath` scopes, so
  the share of measured latency that is the harness's own is unknown.
- **The governor comparison is one capture per side on the macro and profile layers.** The micro
  layer has repeats behind it; the macro layer does not, so "−0.7%" is a difference between two
  medians of five and not a characterised effect. It is well inside the spread either way.
