# Benchmarking

Shoal's benchmark harness is the `tmdb` example (`shoal/examples/tmdb.rs`). It inserts a TMDB
movie dataset, reads every row back, and reports latency distributions for the two operations
separately.

This page is about producing a number you can *compare*. Most of the work in benchmarking Shoal
is not running the harness — it is making sure the two runs you are holding up against each
other differ in exactly one thing.

## Running it

The dataset is not in the repository and no script fetches it. The harness expects a CSV of the
same shape as `TMDB_movie_dataset_v11_first_100k.csv` (99,999 rows) and defaults to
`/home/mcarson/datasets/`. Point `--dataset` wherever yours lives.

The example starts its own server in-process from `--conf`, so you do not need a separate
`shoald`:

```bash
cargo build --example tmdb --release
./target/release/examples/tmdb --conf shoal.yml --limit 20000 --no-wait
```

| Flag | Default | What it does |
| --- | --- | --- |
| `--workers <N>` | 5 | Client worker tasks, all pulling from one shared job channel |
| `--batch <N>` | 100 | Queries buffered before a batch is sent |
| `--in-flight <N>` | 4096 | Per-worker cap on outstanding queries |
| `--iterations <N>` | 1 | Repeats of the insert + verify cycle |
| `--dataset <PATH>` | see above | The CSV to load |
| `--limit <N>` | none | Only load this many rows |
| `--baseline <PATH>` | `.benchmark` | The prior run to diff against |
| `--write-baseline` | off | Record this run as the new baseline |
| `--conf <PATH>` | `shoal.yml` | The config to start the server with |
| `--addr <ADDR>` | `127.0.0.1:12000` | Where the client connects |
| `--client-cores <LIST>` | `28,29,30,31` | Cores to pin the client's tokio workers to |
| `--no-wait` | off | Exit when finished instead of waiting on stdin |

Without `--no-wait` the harness blocks on a newline at the end so you can inspect the server.
`shoal_looper.sh` at the repo root does not pass it, so run 1 of that loop hangs forever —
use `--no-wait` if you script it.

`--in-flight` must be more than four times `--batch`. The harness exits with status 2 if it is
not. That floor is not arbitrary; see below.

### The `--in-flight` floor

A worker buffers queries until it has `--batch` of them, sends the batch, and blocks once
`--in-flight` queries are outstanding. If those two numbers are close, the worker sends a batch,
immediately hits the cap, and cannot build the next batch until nearly the whole batch has come
back. The pipeline empties on every cycle.

That bubble is close to free when acknowledgement is instant. It is not free once the ack waits
on an `fdatasync`: the tail of every batch pays full latency while the worker sits idle, and
**group commit cannot amortise across a barrier that drains the pipeline**. You would be
measuring the client's stall and attributing it to the server. The floor exists to make that
configuration unreachable from the command line.

## What is actually measured

Read the output with these four facts in mind.

**Timing is per batch, not per query.** One `Instant` is taken when a batch is submitted and
copied to every query index in it. A sample is therefore *batch submit → response processed*,
not per-query service time. Query 100 in a batch is charged for the 99 queued ahead of it. This
is why `min` and `p50` sit two orders of magnitude apart in a typical run:

```
insert (170051 samples)
  max: 47.20ms
  p99: 31.22ms
  p50: 8.14ms
  min: 286.98µs
```

The `min` is roughly the true service time. The `p50` is dominated by queueing inside the batch
and by however deep you set `--in-flight`.

**The clock stops when the worker dequeues the response**, not when it arrives on the socket, so
worker scheduling delay is folded into every sample.

**`total` is whole-run wall clock**, including CSV parsing and worker spawn. It is the only
throughput-ish figure the harness emits — there is no rows/sec counter, just `total` and the
`Inserted:` / `Retrieved:` counts.

**Insert and get are separate distributions.** They were pooled into one until recently, which
made `p99` a mix of two different operations. A get is roughly 20× faster at the median in the
run above, so pooling them was not a small distortion.

## Core layout on this machine

The development machine is a Ryzen 9 9950X: 16 physical cores, 32 logical. `Conf::to_cpu_set`
takes the first `cores` online CPUs, so `cores: 16` puts one shard thread on every physical
core — and the client's default `--client-cores 28,29,30,31` are the **SMT siblings of physical
cores 12–15**. At `cores: 16` the client contends with four of the sixteen shards for execution
resources.

`exclude_cores` filters on the *physical* core id, so excluding a core removes both of its
threads. That makes a clean split easy:

```yaml
resources:
  cores: 12
  exclude_cores: [12, 13, 14, 15]   # removes both threads of each
```

with `--client-cores 28,29,30,31`. The server now has physical cores 0–11 and the client has
12–15 to itself.

At `cores: 16` no such split exists on a 16-core part. Either accept the contention and hold it
constant across every run you intend to compare, or move the client to another machine.

> **The shipped `shoal.yml` spells it `exluded_cores`, which is silently ignored.** Serde
> accepts the unknown key and core exclusion never happens. Check your spelling before trusting
> an isolated run. See [Known Issues #18](../appendix/known-issues.md#18-exluded_cores-is-silently-ignored) and
> [Configuration](../getting-started/configuration.md#the-exluded_cores-typo).

Note also that `Conf::from_file` marks the file `required(false)`, so a typo in `--conf` does
not fail — it silently runs with defaults. Confirm the printed config at startup is the one you
meant.

## Protocol

1. **Wipe the storage directory between runs.** Inserting over a populated store changes
   partition faulting, archive map size, and when compaction fires. A directory left over from a
   prior run is a hidden variable.
2. **Hold the config fixed:** same `cores`, `memory`, `buffer_size`, `write_behind`, and dataset.
3. **Sweep `--in-flight` until throughput plateaus, then leave it there.** That plateau is your
   evidence the client is not the bottleneck.
4. **Change exactly one variable per comparison.**
5. **Use named baselines** — `--baseline btrfs-fsync.benchmark --write-baseline` — rather than
   overwriting `.benchmark`.
6. **Discard the first run after a wipe.** It is entirely cold-cache inserts plus forced startup
   compaction.
7. **Repeat every measurement.** See the variance numbers below; a single run at small scale
   cannot resolve anything.

## Establish your noise floor first

On this machine, 4 shards, btrfs, `durability: Fsync`, `--limit 20000` (170,051 inserts +
20,000 gets), sweeping per-worker `--in-flight`:

| `--in-flight` | wall clock | insert p50 | insert p99 |
| --- | --- | --- | --- |
| 512 | 673ms | 8.14ms | 31.22ms |
| 1024 | 717ms | 18.36ms | 53.41ms |
| 2048 | 740ms | 31.32ms | 183.63ms |
| 4096 | 830ms | 40.49ms | 410.08ms |
| 8192 | 552ms | 79.21ms | 257.74ms |

Latency scales almost linearly with concurrency while wall clock does not move. The server is
already saturated at the lowest permitted setting, and everything above it is pure queueing
delay — textbook Little's Law past the knee. **Do not read the extra latency at high
`--in-flight` as a regression; it is the queue you asked for.** Pick a setting at or near the
bottom of the valid range and hold it.

Repeating a single setting three times:

| `--in-flight` | run 1 | run 2 | run 3 |
| --- | --- | --- | --- |
| 512 | 605ms | 823ms | 815ms |
| 4096 | 927ms | 675ms | 718ms |

**Run-to-run spread at a fixed setting is roughly ±30% — larger than any difference between
settings in the table above.** At `--limit 20000` the wall clock cannot resolve anything smaller
than about a third of itself. Either raise `--limit` substantially or take the median of several
runs before you claim a filesystem or durability change moved throughput. Latency percentiles
are far steadier than wall clock and are the better signal at this scale.

## Comparisons that mean something

The `.benchmark` file at the repo root is dated 2025-11-13 and was produced when the ack was
inert — the client was told "inserted" before any IO happened. Comparing today's numbers against
it does not show a regression; it shows two different operations. `.benchmark-old` is an even
older four-field artifact that today's `BenchResult` cannot deserialize at all. (Loading a
baseline is now non-fatal — a stale or corrupt file warns and the run continues with no
comparison — but a file that *does* load and is not comparable is the more dangerous case.)

Comparisons that are valid:

- **`durability: Async` vs `Fsync`**, same build, same clean directory → the cost of the fsync.
- **Same `durability`, btrfs vs ext4/XFS**, same build, same clean directory → the cost of the
  filesystem.

## Before changing the filesystem

Capture **both** `Async` and `Fsync` on btrfs over a clean directory first. Once the filesystem
is reformatted those numbers cannot be reconstructed.

btrfs is a poor host for a write-ahead log. It is copy-on-write, so every overwrite relocates,
and each `fdatasync` forces a log-tree commit — metadata work that ext4 and XFS do not do on
this path.

It also **silently downgrades misaligned O_DIRECT writes to buffered IO**, where ext4 and XFS
return `EINVAL`. This is not a hypothetical: an intent log written by an earlier version of the
writer was 6,670,680 bytes, not a multiple of 512. On btrfs it wrote without complaint. The same
code on ext4 would have failed the write outright. So a filesystem migration can turn a silent
correctness problem into a loud one — budget for that, and do not read a post-migration error as
a regression caused by the new filesystem.

Finally: mounting with `nobarrier` will make every fsync number look better by disabling the
cache flush that is the entire thing being measured. Do not.

## Remaining caveats

Things the harness still does not do, which bound what you can conclude from it:

- **Per-batch timing granularity**, as described above. There is no per-query service time.
- **No warmup phase.** The first batches of a run include connection establishment and cold
  partition faults, and they are in the distribution.
- **No throughput figure.** Only wall clock and total counts.
- **`--workers` workers race on one shared job channel**, so batch composition varies run to
  run. This is part of why the variance is as wide as it is.
- **`PersistentSortedTable` is not `hotpath`-instrumented**
  ([Observability](observability.md)), so a `hotpath` profile of this workload misses the table
  layer — even though `MovieByKeyword` fan-out is roughly 88% of the inserts.
