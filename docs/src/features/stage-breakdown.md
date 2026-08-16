# F6. A per query stage breakdown

## Context

[F3](performance-harness.md) built three layers that answer *how long*. None of them answer
*where*.

The macro benchmark takes one `Instant` per batch in `MovieWorker::send_batch` and stops it
when the worker dequeues the response. The frozen [B1 baseline](../performance/baseline.md)
that comes out of it says an insert's p50 is 49.7 ms against a min of 423.6 µs, and a get's p50
is 128.7 µs against a min of 21.4 µs. Two orders of magnitude of spread, and the number itself
cannot say what any of it was.

The reason that gap is structural rather than accidental is that a get and an insert do not
finish the same way. A get is answered inside the shard's message loop and
`PersistentSortedTable::get` returns its response. An insert returns nothing: it appends to the
intent log, parks its response in `PendingResponse::add` keyed by the byte offset it will be
durable at, and is released only once an `fdatasync` watermark passes that offset — or once a
log rotation drains the queue. That parked interval is where the 49.7 ms lives, and until this
feature it was one opaque bucket that no layer of the harness could see into.

Three outcomes were wanted: attribute the insert tail to a named phase, give each stage its own
number so a regression lands on a stage rather than on an aggregate, and stop guessing which
entry in [Optimizations](../appendix/optimizations.md) is worth taking.

## What it does

A `stage-profile` build records, for every query, when it reached each of nineteen points
between the client handing it to `send` and the response coming back. ~~`scripts/bench.sh`~~
`shoal-bench run` captures one as a fifth phase and archives it to
`docs/perf/runs/<label>.stages.json`, where [Benchmark
Results](../performance/overview.md) draws it as a stacked bar per latency rank.

| Stage | From → to | Per |
| --- | --- | --- |
| `client_serialize` | `send` entry → after `rkyv::to_bytes` | batch |
| `client_pool` | → after `pool.get()` | batch |
| `client_write` | → after the write loop | batch |
| `net_in` | → last byte read off the server's socket | query |
| `shard_queue_in` | → the shard dequeues the bundle | query |
| `decode` | → `Queries::access` and deserialize done | batch |
| `route` | → handed to the shard owning its partitions | query |
| `exec_queue` | → that shard dequeues it | query |
| `execute` | → synchronous work done (`commit` returned, or the response was built) | query |
| `durable_staged` | → the write carrying it is submitted to io_uring | query |
| `durable_write` | → that write lands | query |
| `durable_sync_wait` | → the covering `fdatasync` claims its slot | query |
| `durable_sync` | → that `fdatasync` returns | query |
| `durable_rotated` | → the whole wait, for a response a rotation released | query |
| `release_wake` | → `PendingResponse::get` releases it | query |
| `reply_serialize` | → response `rkyv::to_bytes` done | query |
| `reply_queue` | → queued to the client relay | query |
| `socket_write` | → last byte handed to the socket | query |
| `net_out` | → response read by the client's proxy | query |

Records are ranked by their **total** latency, a window of records is taken around each rank,
and the mean of each stage over that window is reported. What the slow queries were waiting on,
not what the slowest instance of each stage was.

**What a first capture said.** A 20,000 row smoke run, insert `all` bucket, 57.0 ms mean:
`durable_write` 56.7%, `durable_sync_wait` 19.3%, `durable_sync` 14.4%. At p99, 253 ms:
`durable_write` 52.9%, `durable_sync_wait` 32.3%. Durability is the insert tail, as expected —
but `durable_write` being the largest of the three was not expected, and it is not device
latency. It is `write_submitted → write_completed` with `write_behind` at 128, so it is mostly
queue depth at the device.

Two things the design expected to find were not there:

- **The staging buffer is not the p50.** `StreamWriter::sync` only runs when the shard's channel
  is empty, so the plan for this feature named `durable_staged` as a live suspect for the insert
  p50. It is **0.6% at p99** and does not appear in the top six anywhere. Under sustained load
  the 4 KiB buffer fills long before the channel drains.
- **`reply_serialize` is not the get path.** A whole wide `Movie` through `rkyv::to_bytes` was
  the largest unmeasured get-side suspect. It is **666 ns**, 0.8% of an 80 µs get. The get path
  is `socket_write` 21.4 µs, `net_out` 17.9 µs, `exec_queue` 15.8 µs, `net_in` 15.3 µs — network
  and queueing. `execute`, the actual database work, is 3.2 µs.

Both were plausible from reading the code and both are wrong, which is the case for having the
layer at all.

## Design choices

**A zero sized type is the seam.** Stamps have to reach the point where a response leaves the
server, which means riding in the tuples the tables return and the ones parked in
`PendingResponse` — about fifty sites. `StageStamps` is defined twice: the real struct under the
feature, and `pub struct StageStamps;` without it, with
`const _: () = assert!(size_of::<StageStamps>() == 0)` holding that claim. The tuple arity never
changes, only what one element costs, so not one of those fifty sites carries a `#[cfg]`.

**Offsets, not stamps.** `QueryMetadata` is cloned per query and again per blocked partition, so
a record holds one base `Stamp` and thirteen `u32` nanosecond offsets rather than thirteen
`Instant`s. `Offset::UNSET` and `Offset::SATURATED` are distinct values: a get never reaches the
durability stages, and unset cannot be spelled zero because a stage really can land in the same
nanosecond as the base.

**Durability is a timeline, not a stamp.** The four phases between `commit` returning and a
response coming back are intervals of the intent log, not properties of a query — a query cannot
stamp them because by the time it knows it is durable the sync that made it so has returned. So
`FlushState` keeps a bounded deque of `DurabilityWindow`s appended in submission order, and a
released response looks its phases up by the offset it parked at. `start_sync` group commits, so
when a sync claims its slot it stamps **every** window below its target, not the newest one —
attributing a group commit to one write would report the rest as having no sync at all.

**Records are keyed on `(id, index)`.** Every query in a worker's stream shares one `id`, so the
pair is the key — the same pair `Shard::gathering` and the tables' pending maps already use.

**A bucket is a window, and the residual is printed.** Percentiles of individual stages do not
add to the percentile of a total, so a table of per-stage percentiles reads like an explanation
while being none. Buckets are windows (0.5% of records, floor 100) around each rank, and every
bucket states `unaccounted_ns` — the part of its mean total the stages did not explain. In the
smoke capture that residual is under 100 ns on totals of 57 ms.

**A warmup's records are dropped by epoch.** Threads buffer records before handing them over, so
by the time the warmup ends most of its records are not yet reachable. Each record carries the
epoch it was emitted in; `reset()` bumps the epoch, and records from an old one are discarded
whenever they arrive.

## Alternatives rejected

**Aggregate histograms on the shard instead of threading stamps.** This was the original plan,
chosen to avoid touching the derive macro. It would have made the bucketing impossible: an
aggregate histogram cannot be joined to a query's total, so there would be no way to ask what
the p99 queries were waiting on. Threading cost five `quote!` sites in
`shoal-derive/src/traits/db.rs` and about fifty ordinary ones. Not worth avoiding.

**`#[cfg]` at every threaded site.** With a changed tuple arity this needs a `#[cfg]` pair at
each of the fifty sites. The ZST does the same job in one place.

**Handing records over on `ServerMsg::Shutdown`.** Shards only see that when `pool.exit()` runs,
which is long after the workload has finished measuring. A shutdown-only handoff produces an empty
report. Records are flushed in batches as the run proceeds, with the tail handed over at
shutdown, and the report is built in `main` after `pool.exit()`.

**Per-stage independent percentiles.** They do not add up, and they invite reading the worst
`durable_sync` and the worst total as the same query. They usually are not.

**A TSC clock (`minstant`, `quanta`).** Would take the floor from about 20 ns to about 12, which
only improves the queue-hop stages — the least interesting ones, given the tail is in
milliseconds. `quanta` also falls back to `Instant` without an invariant TSC, so the win is not
portable. Every reading goes through a `Stamp` newtype instead, so buying it later is a change
to one file.

**Sampling independently on each side.** Nothing would join. `--stage-sample` is taken on the
query index, and the server reads the same rate from `SHOAL_STAGE_SAMPLE`.

## Limitations

- **The joined report requires an in-process server.** Stamps are `CLOCK_MONOTONIC` readings.
  They are comparable across threads, which is what makes the shard and client halves joinable in
  a workload, where `ShoalPool::start` runs in the same process. Against a remote server `net_in` and
  `net_out` are meaningless and the two halves cannot be joined at all.
- **Four stages are batch level.** `client_serialize`, `client_pool`, `client_write` and `decode`
  are paid once per bundle and charged to every query in it. They are labelled `per_batch` in the
  JSON; a reader who ignores that will read a large `decode` as one query's cost.
- **Some stages sit at the clock floor.** `route` and `reply_queue` are tens of nanoseconds,
  differenced from two clock reads of about 20 ns each. They are marked `at_floor` rather than
  reported as measurements. `reply_queue` measured 48 ns in the smoke capture — that is the
  instrument.
- **A rotated response has no durability breakdown.** A rotation fdatasyncs the old log and
  throws its timeline away, so those records carry `durable_rotated` — the whole wait, unsplit —
  and are counted in `OpReport::rotated`.
- **A `stage-profile` build is attribution only.** Ten extra clock reads per query. Its absolute
  latencies are not comparable to a shipping build's, exactly as with `hotpath`. It must never be
  the source of a baseline number.
- **Memory.** About 2.6 M records at ~96 bytes is roughly 250 MB for a full 100k-row run, plus
  the client side. `--stage-sample 4` brings that under 65 MB.
- **`durable_write` is not device latency.** With `write_behind` at 128 it is mostly queue depth.
  Reading it as "the SSD took 32 ms" would be wrong.

## Invariants to uphold

- **`StageStamps` must stay zero sized with the feature off.** The const assert holds this. It is
  the only thing making fifty threaded sites acceptable in a shipping build — delete it and the
  plumbing becomes a real cost with nothing checking that it is not.
- **`Offset::UNSET` must never be spelled zero.** A get has no durability stages. Reporting them
  as zero says a get syncs very fast rather than that it never syncs.
- **A group commit stamps every window it covers.** `FlushState::mark_sync_stage` walks from the
  front to the first window past the target. Stamping only the newest would leave every other
  write in the group with no sync stage.
- **Sampling must be computed identically on both sides, from the index.** Anything else fails to
  join.
- **The report is built after `pool.exit()`.** Before that, the shards' tails are still in
  thread-local buffers.
- **A build without the feature must refuse `--stage-json`, not write an empty file.** A stage
  report missing its entries reads as "this code was never called" — the same failure the
  `hotpath` `limit = 0` note records.
- **`unaccounted_ns` must stay in the output.** Stages that stop adding up mean one is missing or
  one is double counted. Rounding that away hides the bug.

## Performance

Ten to thirteen `Instant::now()` calls per query at roughly 20 ns each, about 250 ns total. Against
the numbers this layer exists to explain: 1.1% of a get's 21.4 µs min, 0.19% of its 128.7 µs p50,
0.0005% of an insert's 49.7 ms p50. Whole-run cost is around 0.6 CPU-seconds spread over 12
shards.

With the feature off the ZST and the no-op methods leave nothing behind. That is a claim about
codegen, not a guarantee — Rust promises a ZST occupies no space, not that the surrounding code
is identical. **It has not yet been verified by capture.** The check is a feature-off macro run
before and after the plumbing, confirming the delta sits inside the ~10.5% run-to-run spread
~~`bench.sh`~~ `shoal-bench run` already reports — and which
[`shoal-bench compare`](bench-runner.md) will now screen for you, since a macro difference is a
result only when the two captures' observed intervals are disjoint.

## Tests

| Test | Breaks if |
| --- | --- |
| `stage_profile::tests::unset_is_not_zero` (`shoal-core`) | an unreached stage stops being distinguishable from an instant one |
| `stage_profile::tests::a_long_stage_saturates` | a wrapped offset prints as a fast query |
| `stage_profile::tests::stamps_are_monotonic` | a stamp measured against a later one wraps instead of saturating |
| `stage_profile::tests::clock_overhead_is_measurable` | the floor the report marks stages against stops being measurable |
| `stages::tests::bucket_means_reconcile` | stage means stop summing to their bucket total |
| `stages::tests::a_bucket_is_a_window_not_a_point` | a percentile bucket degenerates to one sample |
| `stages::tests::unset_is_not_zero` | a get reports zeroed durability stages |
| `stages::tests::a_write_reports_its_durability_stages` | the four-way durability split stops being emitted |
| `stages::tests::join_accounts_for_every_record` | one-sided or duplicate records are dropped instead of counted |
| `stages::tests::a_stage_at_the_clock_floor_is_marked` | a stage the size of a clock read is reported as a measurement |
| `stages::tests::batch_stages_are_labelled` | a batch level cost is reported as a per query one |
| `stages::tests::a_report_from_another_version_is_refused` | a report from another schema version is compared anyway |

## Related

- [Benchmarking](../performance/benchmarking.md) — the runbook, and where this sits among the layers
- [Performance baseline](../performance/baseline.md) — the B1 numbers this explains
- [F3](performance-harness.md) — the three layers this is a fourth of
- [F5](flushed-sweep-gate.md) — the sweep whose delay `release_wake` measures
- [O26](../appendix/optimizations.md) — the `QueryMetadata` clone found while building this
- [Known issue 52](../appendix/known-issues.md) — the double reply `join.duplicates` counts
