# Benchmarking

How to run a benchmark. The numbers themselves are on
[Benchmark Results](overview.md), which is generated from the captures this page tells
you how to take; the frozen `B1` capture and the hardware it came from are in
[Performance Baseline](baseline.md). The design of the harness is
[F3](../features/performance-harness.md), and of the tool that drives it,
[F7](../features/bench-runner.md).

Most of the work in benchmarking Shoal is not running the harness. It is making sure the two
runs you are holding up against each other differ in exactly one thing.

## The four layers

They answer different questions and they interfere with each other, so they are captured
separately and never mixed.

| Layer | Question | Spread | How to run it |
| --- | --- | --- | --- |
| Micro (criterion) | did this function get faster | 5–9% by duration; repeat to confirm | `cargo bench -p shoal --features bench` |
| Macro (workloads) | did one path through the system get faster | ~11% whole-system — take a median | `shoal-bench run --layer macro` |
| Profile (`hotpath`) | which scopes cost the most | perturbs the run | a separate `--features hotpath` build |
| Stages ([F6](../features/stage-breakdown.md)) | where did one query's latency go | perturbs the run | a separate `--features stage-profile` build |

**Neither instrumented build ever produces a latency or throughput number.** `hotpath` installs
a collector and takes two timestamps around every instrumented scope; a stage build takes about
ten clock readings per query. Neither wall clock is comparable to an uninstrumented build's, and
quoting one as a result is the easiest way to be confidently wrong.

**The two attribution layers answer different questions**, which is why both exist. `hotpath`
ranks *scopes* across the whole run — good for "what is this process spending itself on". The
stage profile follows one *query* and reports the breakdown grouped by the total latency of the
queries being asked about — which is what attributes a tail, because the queries at p99 are not
waiting on the same thing as the queries at p50. The macro layer's own section below explains why
its per-batch timestamp cannot answer that.

## Prerequisites

- A release build. `.cargo/config.toml` sets `-Ctarget-cpu=native`; before that file existed
  the flag was in a `[build]` table in the workspace `Cargo.toml`, where cargo silently ignores
  it, so numbers from before and after it are not comparable.
- `shoal.yml` at the repo root. It is committed and it *is* the benchmark configuration —
  changing it invalidates the recorded baseline.
- A writable storage directory at whatever `shoal.yml` points at, `/opt/shoal` by default.
- **A collector, only if `shoal.yml` names one.** The `tracing:` section is honored by a capture
  ([F34](../features/benchmark-tracing.md)). A sink that is unreachable is logged and never fatal,
  so a capture still runs — but the level it was taken at changes what was measured, and that is
  under *Getting a number you can trust* below.
- **The `tls` kernel module, for the eight encrypted transport arms only.** `modprobe tls` — a
  machine that has never used kTLS answers `ENOENT`, `setsockopt` does not autoload it, and a
  workload configured for TLS refuses to start rather than quietly capturing a plaintext number
  under an encrypted label ([F14](../features/encryption-in-transit.md)). Every other workload is
  unaffected.

  **An encrypted capture and a plaintext one are not the same measurement**, the same rule
  `hotpath` and `stage-profile` builds already follow. That is why the arms are separate workloads
  with their own identifiers rather than a flag on the existing eight, and why `ConfFacts` records
  a `tls` field: two captures across that axis have to be distinguishable in the artifact rather
  than looking identical.
- **The CPU governor set to `performance`.** This is a precondition of the recorded baseline and
  the one precondition that lives outside the repository:

  ```bash
  cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor   # expect: performance
  ```

  It is worth being honest about why it is required, because the reason is not the obvious one:
  measuring it showed the governor makes **no detectable difference** to either layer
  ([what the governor changed](baseline.md#what-the-governor-changed)). It is
  required so that a capture matches the environment the baseline was taken in, not because it
  is faster.
- ~~**The dataset, which is not in the repository and which no script fetches.** The macro
  harness expects a CSV shaped like `TMDB_movie_dataset_v11_first_100k.csv` (99,999 rows) and
  defaults to `/home/mcarson/datasets/`.~~ **Nothing, since
  [F8](../features/purpose-built-workloads.md).** Every workload builds its own rows from
  `--seed`, so a clean checkout reproduces the macro layer with nothing fetched. The same seed
  produces byte-identical data on any machine, which is why two captures differ by machine noise
  rather than by what they happened to load.

## The one command path

```bash
cargo run -p shoal-bench --release -- run --label <label>
```

Everything below assumes `shoal-bench` is on your path or that you prefix it with
`cargo run -p shoal-bench --release --`. Building it takes a few seconds and it depends on
nothing else in the workspace, so it never rebuilds because the database changed.

It does five things, and writes five files into `docs/perf/runs/`:

1. Builds release without instrumentation.
2. Clears `target/criterion` and runs the micro-benchmarks →
   `<label>.micro.json`. The clear matters: criterion keeps a directory per benchmark id
   indefinitely, so a renamed or deleted benchmark keeps reporting its last result into every
   later capture, and a stale number looks exactly like a fresh one.
3. Runs **each workload** `--runs` times (5 by default), wiping storage before each, and keeps
   each workload's **own median** → `<label>.macro.json`. Per workload matters: a hiccup during
   one workload's fourth run says nothing about which run of a different workload deserves to be
   kept, so folding them together would let one workload's outlier choose every other workload's
   reported result.
4. Rebuilds *with* `--features hotpath` and runs once → `<label>.hotpath.json`.
5. Rebuilds *with* `--features stage-profile` and runs once → `<label>.stages.json`, printing
   the join counts. Then rebuilds without any instrumentation, so a later manual run does not
   silently measure a profiling binary. That last rebuild happens **even when a phase fails**,
   which is why it is not one of the numbered phases.

Alongside them it writes `<label>.meta.json`, which is what lets a committed number say later
whether it still describes the current code: the commit, whether the tree was dirty, a content
hash of the sources each layer measures, and the machine, governor and toolchain it ran on.

A full capture is now **three hundred and seventy four workloads times five runs**. At two hundred
and nine it ~~budget six to seven hours~~ **took seventy-five minutes** — one minute building,
fourteen in the micro layer, sixty in the macro layer, and under a minute in the two instrumented
ones. That is the first capture anybody put a clock on, `F20-conf` on 2026-08-22; every figure this
page carried before it was an estimate, and the estimate was over by more than four times.
[F22](../features/row-size-benchmarks.md) then added 165 macro arms and 50 micro benchmarks, so the
current figure is **about two hours** and is a projection again — scaled from the measured one by
arm count, and low if the new arms are wider than average, which the ones above 8 KiB are. Nobody
has timed a capture of the current set. Eight of them are the
storage-free controls [F9](../features/ephemeral-tables.md)
added, and they are the cheapest of the set: they have no disk to wait on. Sixteen are the
transport modes [F13](../features/transport-workloads.md) added and
[F14](../features/encryption-in-transit.md) doubled, and the MiB half of those is the most
expensive single thing in a capture — 512 MiB seeded and two gigabytes over the wire, per run.
Forty-eight are F14's encryption sweeps, which are wide but not slow: each holds a fixed *byte*
budget rather than a fixed query count, so a MiB arm runs 256 queries where a 256-byte arm runs
20,000 and no width dominates.

**~~Seventy-four~~ two hundred and twenty-nine are [F17](../features/workload-grid.md)'s grid**,
which is the largest and slowest phase and roughly doubled a capture on its own before
[F22](../features/row-size-benchmarks.md) tripled it — the width axis is now sixteen widths against
four tables at three mixtures, plus a rung at each width with one query outstanding. It holds the
same byte budget the encryption sweeps do, for the same reason, which is what keeps 120 new arms
from costing what their widths suggest. **~~Forty-eight~~ fifty-eight are
[F20](../features/configuration-sweeps.md)'s configuration
sweeps**, which ~~add about ninety minutes~~ were 31% of the macro layer, about nineteen minutes —
they are wide but each one is the 1 KiB reference cell, and the expensive arms in a capture are the
MiB ones. Ten of them are no longer the reference cell: `latency_buffer` is repeated at 8 KiB and
64 KiB, which is where its effect lives. `list --groups` is where that share comes from, and
`FULL_MACRO_CAPTURE_SECS` behind it has not been re-measured since the layer grew — so every
projection it prints is currently low.

While iterating on anything else, ask for a **group** rather than a prefix
([F21](../features/benchmark-groups.md)) — it is the difference between a coffee and an afternoon,
and unlike a prefix it can express "everything that isolates one path":

```bash
# what the groups are, what each answers, and what a capture of each would cost
shoal-bench list --groups

# everything that drives one path and only one - which is what attributes a regression
shoal-bench run --label <label> --group isolating

# or the grid alone
shoal-bench run --label <label> --group grid

# or one half of the configuration sweep
shoal-bench run --label <label> --group conf/storage
```

Groups combine with **or** and intersect with `--layer` and the positional filters, so
`--group conf durability` is the two durability arms. An unknown group name is an error that prints
the real ones, rather than falling through to selecting everything — which is the failure mode the
prefixes this replaces had, and it costs a whole capture to notice.

**A group selects; it never schedules.** A capture runs one `shoal-workload` process at a time
whatever is selected, and must: two servers at once would share a page cache, a device queue and a
set of cores, and each one's numbers would be a measurement of the other.

~~thirty-one workloads~~, ~~eighty-seven workloads~~ and ~~a hundred and sixty one workloads~~ were
the counts before F13, F14, F17 and F20 landed, and each stayed on this page for at least one feature after it stopped being true. If a
number here disagrees with `shoal-bench list --layer macro | wc -l`, that command is right.
`--scale smoke --runs 2` cuts the data two orders of magnitude and is what you want while
iterating on a workload; the scale is recorded in the artifact, and a `smoke` capture is never
compared against a `full` one.

**It refuses a dirty tree.** A measurement of bytes that exist in no commit cannot be located in
history afterwards, so `run` stops unless you pass `--allow-dirty` — and records that you did.

**Read the join counts.** The stage step prints `joined`, `server only`, `client only` and
`duplicates`. A report whose `joined` is far below the query count is not a report about that
run — `shoal-bench` refuses one below half — and a non-zero `duplicates` means a query was
answered twice, which is [known issue 52](../appendix/known-issues.md) rather than a rounding
detail.

## Running a subset

Filtering works the way `cargo test` does. A positional argument is a substring of a benchmark's
identifier, any of them matching selects it, and `--exact` switches to equality:

```bash
shoal-bench list                       # every benchmark, 59 micro plus one per other layer
shoal-bench list get_key               # what a filter would select
shoal-bench run --label o28 get_key    # capture only those
shoal-bench run --label o28 --layer micro   # or a whole layer
```

The micro list is **discovered** from criterion rather than written down, so a benchmark added to
`shoal/benches/partitions.rs` is selectable immediately. A filter that matches nothing is an
error, not an empty capture.

A filtered capture is recorded as `partial`. It cannot be promoted to a baseline — a baseline
missing benchmarks silently narrows every comparison taken against it afterwards — and any
comparison involving one says so before it prints a table.

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

`shoal-bench compare` applies a band tiered by benchmark duration, because the noise is
proportionally worse the faster the benchmark: **±9% below 1 µs, ±5% above**. And **a single
capture is not evidence** — confirm any apparent win by repeating the whole capture; two
captures agreeing is the evidence. See
[what the micro layer can actually resolve](baseline.md#what-the-micro-layer-can-actually-resolve).

Criterion's own `--save-baseline` / `--baseline` work and are useful mid-session, but they live
in `target/criterion`, which is not committed and does not survive `cargo clean` —
`shoal-bench run` folds them into the durable record. It also ignores anything in there written
before the capture started, so a benchmark you ran by hand an hour ago cannot leak into one.

### Macro

```bash
cargo build --release --bin shoal-workload
sudo rm -rf /opt/shoal/*
./target/release/shoal-workload run --id macro/insert_unsorted \
    --label my-run --json docs/perf/runs/my-run.macro.json
```

`shoal-workload list` prints every workload this build carries, with the timing mode and a line
saying what each one isolates.

| Flag | Default | What it does |
| --- | --- | --- |
| `--id <ID>` | required | Which workload to run. An unknown one lists what does exist |
| `--conf <PATH>` | `shoal.yml` | The base config. Each workload gets its own subdirectory under the configured storage root |
| `--json <PATH>` | required | Where to write what this run measured |
| `--seed <N>` | 42 | The seed every row derives from. The same seed produces byte-identical data on any machine |
| `--scale <smoke\|full>` | `full` | How much data to build. `smoke` is two orders of magnitude smaller, proves a workload runs, and measures nothing |
| `--port <N>` | 12000 | The port this workload's server binds |
| `--label <NAME>` | none | Name recorded inside the result |
| `--stage-json <PATH>` | none | Write a stage breakdown. Needs `--features stage-profile`; a build without it refuses **before starting a server** rather than writing an empty file |
| `--stage-sample <N>` | 1 | Keep one record in every `N`. Taken on the query index, so both halves keep the same queries. `4` keeps the record volume manageable on a full run |
| `--server <ADDR>` | none | Drive a server somebody else started rather than starting one ([F36](../features/cluster-harness.md)). `--conf` is still read for the client's TLS settings and the facts recorded; `--port` is ignored; a workload that restarts its server between phases is refused |
| `--cluster-facts <PATH>` | none | A json `ClusterFacts` record to carry on the capture verbatim; only with `--server` |

The other half of `--server` is `shoal-workload serve --id <ID> [--conf] [--scale] [--port 0]`,
which starts the named workload's server — resolved exactly as `run` would resolve it — and prints
`SHOAL_WORKLOAD_SERVING <addr>` once every shard answers, then holds it until killed. A workload's
port is its position in `workload_ids::IDS` counting up from 12000, frozen in `docs/perf/ports.json`
and held there by a test; cluster arms, when they exist, take a block each from 20000.

There is no `--dataset` and no `--limit`: a workload builds its own rows. There is no `--no-wait`
either, because nothing blocks on stdin any more.

**The batch and in-flight settings are no longer flags.** They are constants of the driver
(`BATCH` 100, `IN_FLIGHT` 4096) because they define what a `per_batch` sample *is*, and a
benchmark whose measurement changes with a command-line flag is not a benchmark. The floor
relating them is asserted in code — see [below](#the---in-flight-floor).

**Read the timing mode before reading a percentile.** A `per_batch` workload saturates and takes
one timestamp per batch, so every query in it is charged for the ones ahead of it; its percentiles
are batch completion times and the number worth quoting is its wall clock. A `per_query` workload
runs at a bounded concurrency with each query stamped on its own, so its percentiles are service
times and its wall clock is *not* a throughput figure. The two are never comparable, in either
direction.

### Profile

```bash
cargo build --release --bin shoal-workload --features hotpath
sudo rm -rf /opt/shoal/*
./target/release/shoal-workload run --id macro/insert_unsorted \
    --json /dev/null 2>/dev/null | tail -1 > profile.json
```

Or `shoal-bench run --label <label> --layer hotpath`, which does the same and stores the result
where the results page can read it. Rank the scopes by `total`, never by the `percent_total`
`hotpath` reports: that field is not normalised across concurrent scopes, and the committed `B1`
profile puts one scope at over 12,000%
([known issue 53](../appendix/known-issues.md)).

The profile is the last line of stdout; everything before it is the run's own output. It
reports every scope — `limit = 0` — because the default of 15 silently truncates, and a profile
missing entries without saying so reads as "this code was never called".

Two entries need reading carefully. `tmdb::main` is the process lifetime. Long-lived task
loops report their lifetime rather than any work, which is why `FileSystemCompactor::start`
carries `#[hotpath::skip]`; if you instrument another such loop, skip it too or it will swamp
every real entry.

## Comparing runs

```bash
shoal-bench compare <label>
```

With no `--against`, that compares against both baselines, which is always what you want:

- **`B1-performance`** is frozen and never overwritten. It says what has been gained in total.
  `shoal-bench promote` refuses to write it, with no flag to override.
- **`trailing`** is the last accepted run. It says what *this* change did.

Either alone misleads. Against the frozen baseline only, a fresh regression hides inside an
earlier win; against the trailing baseline only, a series of individually "neutral" changes drifts
a long way from where it started. Rows whose change is smaller than the band are marked
`(within noise)` — ±9% below 1 µs and ±5% above, with `--noise-pct` overriding both tiers — and
are not results. Benchmarks present on one side and not the other are called out rather than
dropped.

The **macro layer is compared too**, which the shell scripts this replaced could not do. Its band
is not a percentage: each side has an observed interval across its runs, and a difference is a
result only when the two intervals are **disjoint**. A fixed percentage would be wrong here — the
frozen baseline spread 10.5% across five identical runs, wider than most changes worth making.
Percentiles captured before `runs_detail` existed have no interval and are reported as
`no error bar` rather than screened.

`--fail-on-regression` exits 3 when anything moves outside its band in the slower direction, and
`--format json` prints the whole comparison for something else to read.

**Repeat the capture before accepting anything.** The band screens most benchmarks correctly
and does not catch every case — see the `get_key/4096` example on the baseline page.

**Keep a control and a null in the run**, which is the practice
[F4](../features/validated-archives.md#the-controls-moved-and-it-is-not-this-change) settled on
after finding out why it matters. A *control* is a benchmark that exercises the same underlying
code and that the change cannot reach — `partition_sorted/codec/*` and `archived/*` are the
controls for anything about how a partition is held. A *null* is the other arm of the same
dispatch: `maybe_loaded/loaded_get_key` runs the resident arm of the enum whose archived arm is
being changed. When a control moves, the run is telling you something about the machine or the
binary and not about the change — F4's controls moved 9-13%, reproducibly, and the third baseline
is what showed the *pre*-change capture was the outlier. See
[O24](../appendix/optimizations.md#o24-two-benchmarks-move-with-the-shape-of-the-binary-around-them).

When a change is accepted, promote it:

```bash
shoal-bench promote <label>
```

which refuses a partial capture, refuses one that no longer describes the current code, and
copies the capture's provenance alongside the baseline so a later comparison can say where it
came from. Then add a row to [Performance Baseline](baseline.md) carrying **both**
deltas.

## What has been captured, and whether it still holds

```bash
shoal-bench status
```

One line per capture and layer, saying whether it is `fresh` (taken at this commit on a clean
tree), `unaffected` (the commit moved but nothing that layer measures did), `stale` (that layer's
sources changed since), `uncommitted` (the measured bytes are in no commit), or `no provenance`
(captured before `shoal-bench` recorded any).

The digests can only ever narrow `stale` to `unaffected`, never widen anything to `fresh`, because
the list of sources each layer measures lives in `docs/perf/sources.json` and is maintained by
hand. A path missing from it produces a capture wrongly called *unaffected* — which still shows
the commit distance — and never one wrongly called fresh.

## The results pages

```bash
shoal-bench render          # regenerate every page under docs/src/performance/
shoal-bench render --check  # fail if any committed page is out of date, writing nothing
```

Eleven pages, listed on [Performance](overview.md), generated from the committed artifacts and
committed themselves — `create-missing = false` means the book will not build without them and
regenerating them needs this machine. **`render` writes all eleven or none**: a tree holding four
current pages and seven stale ones is worse than one holding eleven stale ones, because nothing on a
page says which kind it is. `--check` reports every page that is out of date rather than the first,
and it fails after any commit, because each page states which commit it was rendered against and
every staleness verdict on it is relative to that commit.

Every chart on those pages draws **one** capture. To see a metric across several — which is what
says whether it is improving or regressing — use the explorer instead:

```bash
shoal-bench explore --serve   # then open http://127.0.0.1:8321
```

[F29](../features/benchmark-explorer.md) draws any number of captures together, against either a
swept fact (the read share, the row width, the load depth) or the capture timeline. On a machine
reached over SSH `--serve` is the form that works, because a native window needs a display; forward
the port and open it in a browser. It reads the same artifacts these pages are generated from, and
draws the macro layer only.

It opens dark, on a One Dark palette; the sun/moon switch at the left of the toolbar changes that,
and the `☰` beside it folds the picker away once you have finished selecting and want the chart to
have the width. The four blocks that say how to read what is on screen are the bar along the bottom
— shut on first sight, and the same four the results pages open with.

It opens on the chart at the top of [the grid page](grid.md), and a button beside it redraws the one
at the top of [the row width page](row-size.md) — those two are declared as presets because selecting
the width sweep by hand is fifty-two checkboxes. **Which workloads may share a chart is decided by
their axis units** ([F30](../features/plot-axis-units.md)): a workload measured at the smoke scale, a
percentile stamped per batch rather than per query, or a row width that is a *mean* over a declared
distribution rather than a measurement, is not offered. Nor is one that carries no value for the
metric on screen at all ([F31](../features/metric-availability.md)) — half the corpus counts no
queries, so *queries answered per second* is not a chart every arm has. The first
workload ticked sets the units and `clear selection` is how they change. Everything that is admitted
is still drawn as **one curve per set of held facts**, so four tables at one width are four lines
and not one line through all four.

The metric control is three lists rather than one: a kind, and — under `latency` alone — an
operation and a rank. Each holds only what **every** ticked workload can answer, so the list shrinks
as the selection narrows and never offers a choice that would draw nothing. With nothing ticked it
is the whole corpus. The picker shrinks the same way, families included; each header says how many
of its members it is not showing, and a line under the tree gives the total. The capture list
underneath does **not** shrink: a capture carrying no value for the metric is **greyed**, with the
reason on hover, and stays tickable ([F33](../features/chart-readout.md)) — a capture is an identity
you know by name out of twenty-seven, where a workload is one row of three hundred and seventy-four.

**Hovering the chart reads out the column**, not the point: every drawn line's value at that position
on the key axis, largest first, with `absent` for the ones that measured nothing there and never a
zero. The numbers are the measurements, so the log toggles do not reach them.

Which page a workload lands on is decided by its **family**, in
`shoal-bench/src/render/family.rs`. A workload belonging to no family fails a test rather than
landing on no page, which is [F18](../features/results-pages.md)'s whole mechanism: before it, a
new sweep appeared on the one big page as an unexplained chart and nobody noticed.

## Getting a number you can trust

1. **Wipe the storage directory between runs.** Inserting over a populated store changes
   partition faulting, archive map size, and when compaction fires. A directory left over from
   a prior run is a hidden variable.
2. **Hold the config fixed:** same `cores`, `memory`, `buffer_size`, `write_behind`, and the same
   `--seed` and `--scale`. All five are recorded per workload in the artifact, so a capture taken
   under a different one is visibly rather than silently incomparable.
2b. **Trace at `Warn`, or know that you are not measuring the same program.** A capture honors the
   `tracing:` section of `shoal.yml` ([F34](../features/benchmark-tracing.md)), and `#[instrument]`
   defaults to `INFO` — so `level: Info` or finer puts a span per query through `tracing`'s registry
   on three per-query callsites in the server. `sample_ratio` does not take that back: a sampler
   decides after the span has been built. The committed file names `Warn` for this reason, the level
   and whether spans were being exported are both recorded in the artifact, and `compare` names a
   pair that disagrees rather than comparing them silently.
3. **Warm up.** Every workload discards its first few percent of samples, for the same reason:
   connection setup, an empty pool and cold partition faults belong to starting up rather than to
   the steady state. The warmup is a property of the workload rather than a flag, so it cannot be
   set to a value that leaves nothing to measure — a test asserts that for each of them.
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

**The stage layer is what separates these.** Everything above is a property of *this* timestamp,
not of the server. A stage profile stamps each query at nineteen points and reports the breakdown
of the queries at each latency rank, so the queueing that dominates a p50 shows up as
`exec_queue` and `shard_queue_in` rather than being folded into a single number.
[F6](../features/stage-breakdown.md) has the stage list and the first capture. Its own caveats
apply: four of its stages are batch level, and it needs an in-process server to join its halves.

**Insert and get are separate distributions.** A get is roughly 20× faster at the median, so
pooling them made `p99` report where the boundary between two distributions landed.

**Micro timing is per call**, with a confidence interval, and touches no IO and no network.

## Reading it wrong

### The `--in-flight` floor

Since [F8](../features/purpose-built-workloads.md) these are constants of the per-batch driver
rather than flags — `BATCH` 100 and `IN_FLIGHT` 4096 — because they define what a `per_batch`
sample *is*. The relationship between them still matters and is asserted in a test rather than
enforced at the command line. The measurements in this section were taken with the old flags and
are kept because the effect they show is a property of the pipeline, not of the harness.

The driver buffers queries until it has `BATCH` of them, sends the batch, and stops topping up
once `IN_FLIGHT` are outstanding. If those two numbers are close, the driver sends a batch,
immediately hits the cap, and cannot build the next one until nearly the whole batch has come
back. The pipeline empties on every cycle.

That bubble is close to free when acknowledgement is instant. It is not free once the ack waits
on an `fdatasync`: the tail of every batch pays full latency while the worker sits idle, and
**group commit cannot amortise across a barrier that drains the pipeline**. You would be
measuring the client's stall and attributing it to the server. The floor makes that
configuration unreachable.

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
textbook Little's Law past the knee. **Do not read the extra latency at high concurrency as a
regression.** This is also why the `per_query` workloads run at a concurrency of 8 or 16 rather
than at saturation: past the knee, every extra sample of latency is queueing delay and a
percentile stops describing what one query cost.

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
([Performance Baseline](baseline.md#end-to-end--retired)), which is better and is still not
enough to adjudicate most of that backlog.

> Both tables above were taken on **btrfs**, on 4 shards, before the storage filesystem moved
> to XFS and before shard placement was made deterministic. They are kept because the shapes
> they show — the Little's Law knee, and the size of the spread — are properties of the
> harness rather than of the filesystem. Do not compare their absolute numbers against anything
> current. Post-migration numbers are in [Performance Baseline](baseline.md).

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
> [what the governor changed](baseline.md#what-the-governor-changed).

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
  [Performance Baseline](baseline.md#hardware).
