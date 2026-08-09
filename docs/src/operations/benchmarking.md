# Benchmarking

How to run a benchmark. The numbers themselves, and the hardware they came from, are in
[Performance Baseline](performance-baseline.md); the design of the harness is
[F3](../features/performance-harness.md).

Most of the work in benchmarking Shoal is not running the harness. It is making sure the two
runs you are holding up against each other differ in exactly one thing.

## The three layers

They answer different questions and they interfere with each other, so they are captured
separately and never mixed.

| Layer | Question | Spread | How to run it |
| --- | --- | --- | --- |
| Micro (criterion) | did this function get faster | 5–9% by duration; repeat to confirm | `cargo bench -p shoal --features bench` |
| Macro (`tmdb`) | did the system get faster end to end | ~11% whole-system — take a median | `./target/release/examples/tmdb ...` |
| Profile (`hotpath`) | where does the time go | perturbs the run | a separate `--features hotpath` build |

**A `hotpath` build never produces a latency or throughput number.** It installs a collector
and takes two timestamps around every instrumented scope. Its wall clock is not comparable to
an uninstrumented build's, and quoting one as a result is the easiest way to be confidently
wrong.

## Prerequisites

- A release build. `.cargo/config.toml` sets `-Ctarget-cpu=native`; before that file existed
  the flag was in a `[build]` table in the workspace `Cargo.toml`, where cargo silently ignores
  it, so numbers from before and after it are not comparable.
- `shoal.yml` at the repo root. It is committed and it *is* the benchmark configuration —
  changing it invalidates the recorded baseline.
- A writable storage directory at whatever `shoal.yml` points at, `/opt/shoal` by default.
- `jq`, used by the collection scripts.
- **The CPU governor set to `performance`.** This is a precondition of the recorded baseline and
  the one precondition that lives outside the repository:

  ```bash
  cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor   # expect: performance
  ```

  It is worth being honest about why it is required, because the reason is not the obvious one:
  measuring it showed the governor makes **no detectable difference** to either layer
  ([what the governor changed](performance-baseline.md#what-the-governor-changed)). It is
  required so that a capture matches the environment the baseline was taken in, not because it
  is faster.
- **The dataset, which is not in the repository and which no script fetches.** The macro
  harness expects a CSV shaped like `TMDB_movie_dataset_v11_first_100k.csv` (99,999 rows) and
  defaults to `/home/mcarson/datasets/`. Point `--dataset` wherever yours lives. The micro
  layer needs none of this.

## The one command path

```bash
scripts/bench.sh <label> [--limit N] [--runs N] [--skip-hotpath]
```

It does four things, and writes three artifacts into `docs/perf/runs/`:

1. Builds release without instrumentation.
2. Clears `target/criterion` and runs the micro-benchmarks →
   `<label>.micro.json`. The clear matters: criterion keeps a directory per benchmark id
   indefinitely, so a renamed or deleted benchmark keeps reporting its last result into every
   later capture, and a stale number looks exactly like a fresh one.
3. Runs the macro benchmark `--runs` times (5 by default), wiping storage before each, and
   keeps the **median** → `<label>.macro.json`.
4. Rebuilds *with* `--features hotpath`, runs once → `<label>.hotpath.json`, then rebuilds
   without it so a later manual run does not silently measure the instrumented binary.

Expect roughly ten minutes at the default settings. `--limit 20000 --runs 2 --skip-hotpath`
turns that into about one, which is what you want while iterating.

## Running each layer by hand

Most days you want one number, not a capture.

### Micro

```bash
# everything
cargo bench -p shoal --features bench --bench partitions
# one group, quickly
cargo bench -p shoal --features bench --bench partitions -- partition_sorted/insert \
    --warm-up-time 1 --measurement-time 3
```

The `bench` feature is required: the benches reach `SortedPartition` through
`shoal_core::server::tables::bench_exports`, which does not exist without it.

Read the interval, not the point estimate:

```
partition_sorted/insert/256
                        time:   [165.15 ns 166.13 ns 167.06 ns]
```

Those are the lower bound, the estimate, and the upper bound.

**Do not use that interval to decide whether a change is real.** It describes how stable the
samples were inside one process, and everything that differs between two processes is invisible
to it. Repeating an identical build four times, one benchmark moved **22%** — and reported its
outlying value with a **±0.2%** interval, tighter than any of the runs it disagreed with.

`scripts/compare.sh` applies a band tiered by benchmark duration, because the noise is
proportionally worse the faster the benchmark: **±9% below 1 µs, ±5% above**. And **a single
capture is not evidence** — confirm any apparent win by repeating the whole capture; two
captures agreeing is the evidence. See
[what the micro layer can actually resolve](performance-baseline.md#what-the-micro-layer-can-actually-resolve).

Criterion's own `--save-baseline` / `--baseline` work and are useful mid-session, but they live
in `target/criterion`, which is not committed and does not survive `cargo clean` —
`scripts/collect-micro.sh` produces the durable record.

### Macro

```bash
cargo build --release --example tmdb
sudo rm -rf /opt/shoal/*
./target/release/examples/tmdb --conf shoal.yml --no-wait --warmup 5000 \
    --label my-run --json docs/perf/runs/my-run.macro.json
```

| Flag | Default | What it does |
| --- | --- | --- |
| `--workers <N>` | 5 | Client worker tasks, all pulling from one shared job channel |
| `--batch <N>` | 100 | Queries buffered before a batch is sent |
| `--in-flight <N>` | 4096 | Per-worker cap on outstanding queries |
| `--iterations <N>` | 1 | Repeats of the insert + verify cycle |
| `--warmup <N>` | 0 | Rows moved through before sampling starts |
| `--dataset <PATH>` | see above | The CSV to load |
| `--limit <N>` | none | Only load this many rows |
| `--baseline <PATH>` | `.benchmark` | The prior run to diff against |
| `--write-baseline` | off | Record this run as the new baseline |
| `--json <PATH>` | none | Archive this run's result, without touching `--baseline` |
| `--label <NAME>` | none | Name recorded inside the result |
| `--conf <PATH>` | `shoal.yml` | The config to start the server with |
| `--addr <ADDR>` | `127.0.0.1:12000` | Where the client connects |
| `--client-cores <LIST>` | `28,29,30,31` | Cores to pin the client's tokio workers to |
| `--no-wait` | off | Exit when finished instead of waiting on stdin |

Without `--no-wait` the harness blocks on a newline at the end so you can inspect the server.
`shoal_looper.sh` at the repo root does not pass it, so run 1 of that loop hangs forever.

`--in-flight` must be more than four times `--batch`; the harness exits with status 2 if it is
not. That floor is not arbitrary — see [below](#the---in-flight-floor).

### Profile

```bash
cargo build --release --example tmdb --features hotpath
sudo rm -rf /opt/shoal/*
./target/release/examples/tmdb --conf shoal.yml --no-wait 2>/dev/null | tail -1 > profile.json
jq -r '.output | to_entries | sort_by(-.value.total)
       | .[] | "\(.value.calls)\t\(.value.total)\t\(.key)"' profile.json
```

The profile is the last line of stdout; everything before it is the run's own output. It
reports every scope — `limit = 0` — because the default of 15 silently truncates, and a profile
missing entries without saying so reads as "this code was never called".

Two entries need reading carefully. `tmdb::main` is the process lifetime. Long-lived task
loops report their lifetime rather than any work, which is why `FileSystemCompactor::start`
carries `#[hotpath::skip]`; if you instrument another such loop, skip it too or it will swamp
every real entry.

## Comparing runs

```bash
scripts/compare.sh docs/perf/runs/<label>.micro.json \
    --against docs/perf/baselines/B1-performance.json \
    --against docs/perf/baselines/trailing.json
```

Two baselines, always:

- **`B1-performance.json`** is frozen and never overwritten. It says what has been gained in total.
- **`trailing.json`** is the last accepted run. It says what *this* change did.

Either alone misleads. Against B0 only, a fresh regression hides inside an earlier win; against
the trailing baseline only, a series of individually "neutral" changes drifts a long way from
where it started. Rows whose change is smaller than the run's own confidence interval are
marked `(within noise)` — the band is ±9% below 1 µs and ±5% above, and `--noise-pct` overrides
both tiers with one flat value — and are not results. Benchmarks present on one side and not the other
are called out rather than dropped.

**Repeat the capture before accepting anything.** The band screens most benchmarks correctly
and does not catch every case — see the `get_key/4096` example on the baseline page.

**Keep a control and a null in the run**, which is the practice
[F4](../features/validated-archives.md#the-controls-moved-and-it-is-not-this-change) settled on
after finding out why it matters. A *control* is a benchmark that exercises the same underlying
code and that the change cannot reach — `partition_sorted/codec/*` and `archived/*` are the
controls for anything about how a partition is held. A *null* is the other arm of the same
dispatch: `maybe_loaded/loaded_get_key` runs the resident arm of the enum whose archived arm is
being changed. When a control moves, the run is telling you something about the machine or the
binary and not about the change — F4's controls moved 9–13%, reproducibly, and the third baseline
is what showed the *pre*-change capture was the outlier. See
[O24](../appendix/optimizations.md#o24-two-benchmarks-move-with-the-shape-of-the-binary-around-them).

When a change is accepted, promote it:

```bash
cp docs/perf/runs/<label>.micro.json docs/perf/baselines/trailing.json
```

and add a row to [Performance Baseline](performance-baseline.md) carrying **both** deltas.

## Getting a number you can trust

1. **Wipe the storage directory between runs.** Inserting over a populated store changes
   partition faulting, archive map size, and when compaction fires. A directory left over from
   a prior run is a hidden variable.
2. **Hold the config fixed:** same `cores`, `memory`, `buffer_size`, `write_behind`, dataset.
3. **Warm up.** `--warmup 5000` moves connection setup, an empty pool, and cold partition
   faults out of the distribution. The rows it inserts stay behind on purpose — that is what
   makes the measured pass a steady state rather than a second cold start.
4. **Sweep `--in-flight` until throughput plateaus, then leave it there.** That plateau is your
   evidence the client is not the bottleneck.
5. **Change exactly one variable per comparison.**
6. **Discard the first run after a wipe.** It is entirely cold-cache inserts plus forced
   startup compaction.
7. **Take the median of several runs**, never the mean. The macro benchmark's outliers are one
   sided: a run can be arbitrarily slow and cannot be faster than the work.

## What is actually measured

**Macro timing is per batch, not per query.** One `Instant` is taken when a batch is submitted
and copied to every query index in it, so a sample is *batch submit → response processed*.
Query 100 in a batch is charged for the 99 queued ahead of it. This is why `min` and `p50` sit
two orders of magnitude apart:

```
insert (170051 samples)
  max: 47.20ms
  p99: 31.22ms
  p50: 8.14ms
  min: 286.98µs
```

The `min` is roughly the true service time. The `p50` is dominated by queueing inside the batch
and by however deep you set `--in-flight`.

**The clock stops when the worker dequeues the response**, not when it arrives on the socket,
so worker scheduling delay is folded into every sample.

**`total` is whole-run wall clock**, including CSV parsing and worker spawn, and `rows/sec` is
computed against it. It is a throughput figure for the harness as a whole, not a service rate
for the server.

**Insert and get are separate distributions.** A get is roughly 20× faster at the median, so
pooling them made `p99` report where the boundary between two distributions landed.

**Micro timing is per call**, with a confidence interval, and touches no IO and no network.

## Reading it wrong

### The `--in-flight` floor

A worker buffers queries until it has `--batch` of them, sends the batch, and blocks once
`--in-flight` are outstanding. If those two numbers are close, the worker sends a batch,
immediately hits the cap, and cannot build the next one until nearly the whole batch has come
back. The pipeline empties on every cycle.

That bubble is close to free when acknowledgement is instant. It is not free once the ack waits
on an `fdatasync`: the tail of every batch pays full latency while the worker sits idle, and
**group commit cannot amortise across a barrier that drains the pipeline**. You would be
measuring the client's stall and attributing it to the server. The floor makes that
configuration unreachable from the command line.

### Latency that is just the queue you asked for

Sweeping `--in-flight` on btrfs at `--limit 20000`, before the XFS migration:

| `--in-flight` | wall clock | insert p50 | insert p99 |
| --- | --- | --- | --- |
| 512 | 673ms | 8.14ms | 31.22ms |
| 1024 | 717ms | 18.36ms | 53.41ms |
| 2048 | 740ms | 31.32ms | 183.63ms |
| 4096 | 830ms | 40.49ms | 410.08ms |
| 8192 | 552ms | 79.21ms | 257.74ms |

Latency scales almost linearly with concurrency while wall clock does not move. The server is
already saturated at the lowest permitted setting and everything above it is queueing delay —
textbook Little's Law past the knee. **Do not read the extra latency at high `--in-flight` as a
regression.** Pick a setting near the bottom of the valid range and hold it.

### The spread

Repeating one setting three times, same conditions:

| `--in-flight` | run 1 | run 2 | run 3 |
| --- | --- | --- | --- |
| 512 | 605ms | 823ms | 815ms |
| 4096 | 927ms | 675ms | 718ms |

**Roughly ±30% at a fixed setting — larger than any difference between settings above.** At
`--limit 20000` the wall clock cannot resolve anything smaller than about a third of itself.
This is the measurement that made the micro layer necessary: no amount of statistics recovers a
3% effect from a 30% spread, and nearly every entry in
[Optimizations](../appendix/optimizations.md) claims less than that.

At full scale with a warmup and deterministic shard placement it is now **10.5%**
([Performance Baseline](performance-baseline.md#end-to-end)), which is better and is still not
enough to adjudicate most of that backlog.

> Both tables above were taken on **btrfs**, on 4 shards, before the storage filesystem moved
> to XFS and before shard placement was made deterministic. They are kept because the shapes
> they show — the Little's Law knee, and the size of the spread — are properties of the
> harness rather than of the filesystem. Do not compare their absolute numbers against anything
> current. Post-migration numbers are in [Performance Baseline](performance-baseline.md).

### A baseline that loads and is not comparable

A baseline carries a `version`. One written against a different definition of a sample is
**refused**, loudly, rather than compared. This exists because of a real failure: the old
`.benchmark` was written when the insert acknowledgement was inert — the client was told
"inserted" before any IO happened — and it would still load and diff cleanly against a build
where the ack was real, reporting a catastrophic regression that was two different operations
being subtracted from each other.

A file that fails to parse costs you a comparison. A file that parses and is not comparable
costs you a wrong answer.

### Things about the machine that are not in the repository

`Conf::from_file` marks the config `required(false)`, so a typo in `--conf` does not fail — it
silently runs on defaults. Confirm the config printed at startup is the one you meant.

> ~~The CPU governor on the development machine is `powersave` / `amd-pstate-epp`, not
> `performance`. That widens the spread.~~ **Both halves of that were wrong.** The machine now
> runs `performance`, and re-capturing the whole baseline under it moved the macro wall clock by
> −0.7% and the micro median by +0.14% — noise on both layers, in both directions. The governor
> was never a meaningful variance source here, because `amd-pstate-epp` reaches boost clocks
> almost immediately under the sustained load both layers apply, and because the macro workload
> waits on `fdatasync` rather than on the CPU. See
> [what the governor changed](performance-baseline.md#what-the-governor-changed).

The governor is still pinned to `performance` and listed as a prerequisite — not because it
helps, but because it costs nothing and removes a variable from the environment the baseline
assumes.

## Core layout

`exclude_cores` filters on the **physical** core id, so excluding a core removes both of its
SMT threads. That is what makes a clean split possible on an SMT part. The committed config:

```yaml
resources:
  cores: 12
  exclude_cores: [12, 13, 14, 15]
```

with `--client-cores 28,29,30,31`. On the 9950X that gives the server twelve distinct physical
cores, the coordinator cpu 0, and cores 12–15 entirely to the client — cpus 28–31 are the SMT
siblings of physical cores 12–15, so excluding those cores is what stops the client from
contending with a shard.

> Until recently this did not work in either direction. The shipped config spelled the key
> `exluded_cores`, which serde accepted and dropped; and underneath that, `Resources::cpus`
> took cpus off a `HashSet`, so shard placement changed on every process start and routinely
> put two shards on one physical core while another sat idle. Both are fixed — see
> [Resolved #18, #50](../appendix/resolved/excluded-cores-typo.md). Any measurement taken
> before that fix carries an unbounded and invisible variance term.

## Filesystem

Shoal's storage now lives on **XFS**. The btrfs guidance that used to be here is kept below
because the reasoning still applies to anyone running on btrfs, and because the migration
changed a failure mode rather than removing it.

> ~~**Before changing the filesystem**, capture both `Async` and `Fsync` on btrfs over a clean
> directory first. Once the filesystem is reformatted those numbers cannot be
> reconstructed.~~ — the migration happened, and they were not captured. B0 is therefore an
> XFS-only baseline with no btrfs counterpart, and the btrfs-era numbers that survive are the
> two tables above.

btrfs is a poor host for a write-ahead log. It is copy-on-write, so every overwrite relocates,
and each `fdatasync` forces a log-tree commit — metadata work that ext4 and XFS do not do on
this path.

It also **silently downgrades misaligned O_DIRECT writes to buffered IO**, where ext4 and XFS
return `EINVAL`. This was not hypothetical: an intent log written by an earlier version of the
writer was 6,670,680 bytes, not a multiple of 512, and btrfs wrote it without complaint. **On
XFS that same write fails outright.** The migration turned a silent correctness problem into a
loud one, which is an improvement — but do not read a post-migration `EINVAL` as a regression
caused by the new filesystem. It is the old bug becoming visible.

Finally: mounting with `nobarrier` will make every fsync number look better by disabling the
cache flush that is the entire thing being measured. Do not.

Comparisons that are valid:

- **`durability: Async` vs `Fsync`**, same build, same clean directory → the cost of the fsync.
- **Same `durability`, different filesystem**, same build, same clean directory → the cost of
  the filesystem.

## Remaining caveats

Things the harness still does not do, which bound what you can conclude from it:

- **Macro timing is per batch.** There is no per-query service time.
- **`--workers` workers race on one shared job channel**, so batch composition varies run to
  run. This is part of why the spread is as wide as it is.
- **There is no micro-benchmark of the storage write path.** Timing `StreamWriter::write` and
  `start_sync` needs a glommio `LocalExecutor` inside criterion's sampling loop. `hotpath`
  covers it instead, which gives attribution but no confidence interval.
- **The client and server share a machine.** Disjoint physical cores, shared L3 and memory
  controller.
- **One machine, one filesystem, one storage device**, and that device is an Intel Optane whose
  fsync latency is not representative. See
  [Performance Baseline](performance-baseline.md#hardware).
