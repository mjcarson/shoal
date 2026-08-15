# F8. Purpose-built workloads, in the crate that judges them

## Context

[F3](performance-harness.md) built three measurement layers and [F7](bench-runner.md) turned the
scripts that drove them into a tool. Both inherited the same workload: `shoal/examples/tmdb.rs`,
which was an example and a load generator at the same time and was not very good at being either.

It was 1,247 lines. About a fifth of it taught Shoal — the derive macros, `#[shoal::db]`,
projections, `stream_unordered`, one SHQL query. The rest was harness: an eighteen-flag command
line, a worker pool with an in-flight gate, warmup, iterations, hand-pinned tokio core affinity,
latency histograms, baseline archiving, and both halves of two profiling instrumentations.

That cost three things.

**A new developer opening the example met a benchmark.** The thing they came to read was buried
under machinery that has nothing to do with using a database.

**A clean checkout could not run the macro layer at all.** The example needed a 65 MB TMDB CSV at
the hard-coded absolute path `/home/mcarson/datasets/TMDB_movie_dataset_v11_first_100k.csv`, which
is not in this repository and which no script fetches. This was filed in
[Todos](../appendix/todos.md) and is the plainest of the three: the layer could be *read* by
anyone and *reproduced* by one machine.

**One workload cannot isolate anything.** Every macro number was one blended insert-and-get run, so
a change to the read path and a change to the write path moved the same figure and neither could be
attributed. [Todos](../appendix/todos.md) lists what fell out of that: no per-query service time,
no `PersistentSortedTable` coverage — so **O5**, **O12** and **O13** were listed as adjudicable by
benchmarks that do not build a table at all — no `Async`-vs-`Fsync` capture, and no way to see
O13's quadratic term as anything but an argument from the source.

## What it does

Fifteen purpose-built workloads live in `shoal-bench`, each isolating one path through the engine,
each building its own rows from a seed. [F9](ephemeral-tables.md) has since added eight more —
storage-free controls for the workloads below — and [F13](transport-workloads.md) eight more again,
over the client's four transport modes at two row widths, bringing the set to thirty-one.

| Workload | Timing | Isolates |
| --- | --- | --- |
| `macro/insert_unsorted` | `per_batch` | the write path: route, append to the intent log, wait on the durability barrier |
| `macro/get_resident` | `per_query` | a keyed get answered from a partition held in memory |
| `macro/get_archived` | `per_query` | the same get when the partition must be read off disk |
| `macro/fanout/resident/{1,2,4,16,64,256}` | `per_query` | a get over *n* partition keys, every partition resident |
| `macro/fanout/evicted/{1,2,4,16,64,256}` | `per_query` | the same curve when every partition must be read |

`get_resident`/`get_archived` and the two fanout arms are **control-and-null pairs**, the shape
[F4](validated-archives.md) settled on and that caught
[O24](../appendix/optimizations.md). Each pair differs in exactly one axis — whether the data is in
memory — and tests assert that their plans are otherwise identical.

Three things follow that did not exist before.

**A service time.** `per_query` workloads run at a bounded concurrency with one query per slot, so
the time from send to response is the time that query took. Every percentile committed before F8 is
a *batch completion* time: the old harness took one `Instant` per batch and installed it for every
query in it, so each query was charged for the ones ahead of it. Both kinds are still produced,
because saturating is the only way to measure throughput, and the artifact records which is which.

**No dataset.** Rows come from a hand-rolled SplitMix64 seeded by `--seed`, so the same seed
produces byte-identical data on any machine. A clean checkout reproduces the macro layer with
nothing fetched.

**A readiness probe instead of a sleep.** The old harness slept five seconds after
`ShoalPool::start`; `shoal/tests/utils.rs` sleeps two. A workload now retries a real query at 25 ms
intervals until one is answered. That is not only faster — it is *correct* in a way a sleep is not.
On a machine slower than the one the constant was tuned on, the queries sent before the shards were
up measured server startup and reported it as query latency, with nothing in the artifact saying so.

## Design choices

### The workloads live in `shoal-bench`, in a second binary

`shoal-bench/Cargo.toml` carried an invariant: it depended on nothing else in the workspace,
because it is the tool that judges a change and must not rebuild as part of one. The workloads have
to link `shoal` — that is what makes an API change a compile error instead of a benchmark that
quietly stops describing the code.

Both hold, because they fall on a boundary that already existed. `Registry` — the list of what
exists — is used only by `list` and `run`. The four judging commands (`compare`, `status`,
`render`, `promote`) import only the `Layer` enum, which is a plain enum with no engine behind it.
So:

```toml
[features]
default = ["workloads"]          # --all-targets compile-checks every workload
workloads = ["dep:shoal", ...]
```

`cargo build -p shoal-bench --no-default-features` builds the runner alone, with no glommio, and
compares and renders captures taken earlier. That matters precisely when the engine is mid-refactor
and will not compile, which is not a rare moment for a performance harness.

The workloads are a **second binary of the same crate**, not a second crate. Only one thing forces
that split, and it is enough: `hotpath` must annotate a `main`, and `stage-profile` changes the
code being measured. If the workloads ran inside the runner, taking a profile would mean rebuilding
the tool that is currently executing, and the profile would report the runner's own argument
parsing and chart rendering as scopes alongside the server's. `cargo build --bin shoal-workload
--features hotpath` leaves `target/release/shoal-bench` untouched.

### The artifact stopped being a mirror

The macro artifact's structs used to be a hand-kept copy of `shoal::bencher::BenchResult`, with a
`serde(flatten)` catch-all and a test over every committed file to notice when the copy drifted.
The workloads now build `shoal-bench`'s own artifact structs, so the writer and the reader
are the same types. The same happened to the stage report when `shoal/src/stages.rs` moved in.

That deletes a category of bug rather than detecting it, and it is most of the argument for the
workloads living in this crate rather than anywhere else.

### The archived arms restart the server

Reaching the archived read path from a client needs the partitions to be on disk and not in memory.
The obvious lever is `resources.memory`, squeezed until the LRU evicts, which is what
`shoal/tests/utils.rs::build_pressured_config` does.

That is not usable here. A partition cannot be evicted until its generation has been compacted, so
how much ends up on disk depends on how the run happened to interleave with compaction — the
workload would measure a different mixture of resident and archived reads every time it ran, and
nothing in the artifact would say so. Restarting flushes and compacts everything and brings back a
server holding nothing, which is reproducible.

That is why `Workload::seed` is a phase of its own, and why **nothing in it is timed**. A read
workload's numbers must describe reading, not the writing that had to happen first.
`insert_unsorted` reports the cost of inserting because inserting is its subject; `get_resident`
inserts exactly the same rows and reports none of it.

### One storage directory per workload

`StorageMeta::claim` keys a directory to the shard count that wrote it, and the fanout curve pins
one shard while everything else takes twelve. Each workload writes to `<root>/<slug(id)>/`, derived
mechanically from its identifier, so two workloads meeting in one directory is structurally
impossible rather than something to remember.

This changed the wipe guard. `/opt/shoal` itself no longer carries `shoal-meta.json` — the
directories inside it do — so `check_wipeable` now accepts a directory whose immediate children are
stores. Only one level down, deliberately: a guard on a recursive delete must not get easier to
satisfy the deeper the tree goes.

## Alternatives rejected

**A separate `shoal-workloads` crate.** The first design. It keeps `shoal-bench` literally
dependency-free, and buys nothing else: the second binary is required either way, and a separate
crate would additionally need the runner to *discover* the workload list by running
`shoal-workload list` and caching it, the way criterion's list is discovered. In one crate the list
is a `const` the runner reads at compile time. Rejected as more machinery for less.

**Running workloads in-process in the runner.** Simplest to invoke, and broken by `hotpath::main`
as described above.

**An optional-off `workloads` feature.** `cargo check --workspace --all-targets` does not enable
optional features, so the workloads would compile only when someone remembered to ask — and a
benchmark that silently stops compiling is exactly what F8 exists to prevent. Default-on gives the
guarantee; `--no-default-features` gives back the dependency-free runner.

**Rewriting the seven committed version 1 artifacts into the new shape.** They are the historical
record. Rewriting them means the bytes on disk are no longer the bytes that were captured. They are
lifted on read instead, into a single workload named `macro/tmdb`.

**Building a `tmdb`-shaped replacement so the macro baseline stays live.** It could not be
comparable: the dataset is gone, the row was TMDB's twenty-four-field shape, the fan-out was
`keywords.len() + 1` per movie, and the timing was per-batch. A workload merely *shaped* like it
would produce numbers that look comparable and are not — the exact failure the old
`BASELINE_VERSION` comment described. Naming any new workload `tmdb` would be actively harmful.

**Holding query count fixed across the fanout curve.** Would make the widest point cost 256 times
the narrowest. The curve trades query count against key count instead, which has its own cost —
see Limitations.

## Limitations

**`macro/fanout` is not the table-layer benchmark [Todos](../appendix/todos.md) asked for.** That
page asks for a criterion benchmark over `PersistentSortedTable::get`, and says it is blocked on
driving a glommio `LocalExecutor` from inside criterion's sampling loop. That is true from below
and false from above: driving the same method through a live server and a real client needs no
executor in criterion at all. But every sample here includes the wire, the routing,
`split_by_shard`, the response merge and the client. **A number from this workload is not a cost of
`PersistentSortedTable::get`; it is a cost of a query that reaches one.** What it can do is answer
the question O13 poses, because a quadratic term shows up as a curve that bends against a flat
control at *n* = 1 whatever constant sits on top of it. The criterion gap stays open.

**The archived arms warm up as they run, and the wide points warm up fastest.** A restart empties
memory, but the first query to touch a partition faults it back in and it stays. `get_archived`
reads 48,000 of 200,000 partitions, so most of its reads really are cold; the fanout arms read
4,096 partitions repeatedly, and at *n* = 256 a full run touches each about fifteen times, so
roughly 93% of its reads hit something already resident. **The evicted arms therefore measure a
progressively warmer table as *n* rises**, which is why their medians converge on the resident
arms' while their p99s separate by up to 11×. The cold reads are in the tail. Fixing it means
either far more partitions or evicting between queries, and both change what the curve is
measuring; the honest reading for now is the p99 ratio rather than the median gap.

**The fanout curve's wide points have few samples.** Query count falls as key count rises so that
every point reads about the same number of partitions in total. The consequence is that at
`n = 256` a full run takes 238 samples, so its p99 is roughly its third-worst and is not a
percentile in any useful sense. Read the median there — while remembering the point above about
what the median at wide *n* is measuring.

**`macro/tmdb` history cannot join with anything new.** The seven captures taken before F8 still
parse, chart, tabulate and compare *against each other*. They share no workload with any capture
taken after it, and a comparison across that boundary says so rather than printing an empty table.

**The instrumented layers cover one workload.** `hotpath` emits one profile per process, so
attributing a profile to a workload costs one instrumented run each. Only `insert_unsorted` opts
in. A second workload opting in would double the cost of the two most expensive phases of a capture
for a profile that largely repeats the first.

**A capture is much longer than it was.** Fifteen workloads times five runs is 75 server
lifecycles, where before it was five. [F9](ephemeral-tables.md) took it to twenty-three and 115,
and [F13](transport-workloads.md) to thirty-one and 155 — the largest step in wall clock of the
three, because its large arm seeds 512 MiB and moves two gigabytes over the wire per run.
[F14](encryption-in-transit.md) took it to **eighty-seven and 435**, which is the largest step in
*count* and not in wall clock: sixteen of its arms are F13's doubled by a wire axis, and the other
forty-eight hold a fixed byte budget rather than a fixed query count, so the widest of them runs
256 queries and costs about what the narrowest does.

**No workload covers** sort-key selection, projections, ~~transport modes,~~ durability, update,
delete, exists, compaction or recovery. Transport modes are covered by
[F13](transport-workloads.md). Each of the rest is filed in [Todos](../appendix/todos.md) with its
reason.

## Invariants to uphold

**Workload identifiers are join keys.** A comparison joins on them and an artifact is written under
them, exactly as criterion's `full_id` is for the micro layer. **Renaming one orphans every capture
taken before the rename** — the old name stops appearing on the new side and the comparison
correctly reports it as missing, which is not what anybody wanted. Add and deprecate; do not rename.

**`workload_ids::IDS` must equal `workloads::all()`.** Two lists name the same set because the
runner has to know it without the engine linked. A test under the `workloads` feature asserts they
agree, so forgetting one is a test failure rather than a workload that is never run.

**The judging commands must keep building under `--no-default-features`.** If `compare`, `status`,
`render` or `promote` ever reach for `Registry` or for anything under `workloads`, that property is
gone and with it the ability to judge a capture while the engine is broken.

**The per-workload `timing` field must never be dropped.** It is what stops a per-query p99 being
compared against a per-batch one. The two are not comparable in either direction and nothing else
in the artifact distinguishes them.

**A workload's storage directory must stay derived from its identifier.** That is what keeps
`StorageMeta::claim` out of the way when workloads run different shard counts.

**`Workload::seed` must stay untimed.** The moment seeding lands inside the measurement, every read
workload starts reporting the cost of the write path it was built to exclude.

**The stage sample rate must be read from the same environment variable by both halves.** The
client and the server sample on the query index so they keep the *same* queries; if they disagreed,
each would keep a disjoint set and the join would produce nothing.

## Performance

F8 does not change the engine. It changes what can be measured about it. All figures below are
from `f8-powersave`, a full capture on a `powersave` governor and a dirty tree — see the caveat at
the end.

### What the archived read costs

The clearest result, and the one the resident/archived pair was built for:

| | p50 | p99 |
| --- | ---: | ---: |
| `macro/get_resident` | 58.8 µs | 124.7 µs |
| `macro/get_archived` | 68.2 µs | 165.9 µs |

**A partition that has to be read costs about 9 µs more than one already in memory**, on a
median-of-five capture with a 12% and 9% run-to-run spread respectively. That is the right order
for one small `O_DIRECT` read on this hardware, and it is the first time the archived read path has
been priced end to end from a client — [F4](validated-archives.md) shipped without it, because the
`tmdb` workload never evicted a partition and so never entered that path at all.

### The fanout curve

| *n* | resident p50 | evicted p50 | resident p99 | evicted p99 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 40.7 µs | 52.1 µs | 57.7 µs | 89.2 µs |
| 2 | 41.6 µs | 41.6 µs | 59.4 µs | 113.6 µs |
| 4 | 42.9 µs | 43.2 µs | 62.8 µs | 162.7 µs |
| 16 | 56.1 µs | 52.5 µs | 84.1 µs | 483.1 µs |
| 64 | 96.0 µs | 98.1 µs | 182.1 µs | 1,919.1 µs |
| 256 | 311.2 µs | 299.1 µs | 578.6 µs | 6,303.9 µs |

**On the median, the curve is convex but only just.** A chord through *n* = 1 and *n* = 256 has a
slope of 1.06 µs per partition and predicts 108.6 µs at *n* = 64 against 96.0 µs measured, and
57.6 µs at *n* = 16 against 56.1 µs — below the chord at both, which is what a superlinear term
looks like. But the marginal cost per partition between adjacent points is 0.91, 0.64, 1.11, 0.83,
1.12 µs, which does not rise monotonically and is consistent with noise around a straight line.

**A smoke-scale run suggested a much stronger effect and was wrong.** It showed marginals of 0.72,
1.01 and 1.33 µs — a clean rise — off forty samples at the wide end. The full capture, with an
order of magnitude more samples, does not reproduce that. This is exactly the failure the harness
exists to prevent, and it is worth recording that it caught itself: **the curve does not yet
establish O13, and a smoke run must never be read as if it did.**

**The evicted arm separates in the tail, not the median.** At *n* = 256 its p99 is eleven times
the resident arm's, and the ratio grows monotonically with *n* (1.5×, 1.9×, 2.6×, 5.7×, 10.5×,
10.9×). The medians converge instead — and the reason is a limitation of the workload rather than
a property of the engine, recorded above: the table warms as the run proceeds, so at wide *n* the
great majority of reads hit partitions an earlier query already faulted in. The cold reads are
still there; they are the tail.

### The write path

`macro/insert_unsorted` moves 200,000 rows in 1,423 ms with a 4.9% spread — about 140,000 rows a
second. Its percentiles (p50 23.5 ms, p99 150.5 ms) are `per_batch` and describe batch completion
under saturation, not the service time of an insert.

### The stage layer

Reproduced from the new harness at full scale: **200,000 records joined, 1 server-only, 0
client-only, 0 duplicates**. The insert p99 breaks down as `durable_write` 46.5%,
`durable_sync_wait` 35.4%, `durable_sync` 14.7%, with zero unaccounted — the same finding
[F6](stage-breakdown.md) established, from a completely different workload.

### The caveat on all of it

This capture was taken on a `powersave` governor and a **dirty tree**, and its provenance records
both. `docs/perf/baselines/B1-performance.json` was taken on `performance`, so `f8-powersave` is a
first capture of the new workloads rather than a new frozen reference. Establishing one means a
`performance`-governor run on a committed tree, which is recorded in [Todos](../appendix/todos.md).

## Tests

| Test | What breaks if F8 is reverted |
| --- | --- |
| `workload_ids::the_declared_ids_are_the_registered_ones` | the runner's list and the compiled workloads drift, and a workload is never run |
| `workload_ids::every_id_is_namespaced` | a workload id collides with a criterion id in the flat namespace |
| `harness::conf::distinct_ids_slug_distinctly` | two workloads share a storage directory and `StorageMeta::claim` fails mid-capture |
| `harness::seed::a_seed_is_reproducible` | the generated dataset stops being reproducible and captures stop being comparable |
| `harness::seed::named_streams_are_independent` | adding a draw to one stream silently changes another's data |
| `harness::ready::a_dead_server_fails_with_its_last_error` | a broken server hangs a capture instead of failing it |
| `keyed_get::the_two_arms_differ_only_in_residency` | the resident/archived pair stops being a control and its null |
| `keyed_get::the_archived_arm_restarts_rather_than_evicting` | the archived arm measures an unreproducible mixture of resident and archived reads |
| `keyed_get::the_stride_covers_every_key` | reads cycle through a subset and the archived arm measures a warm cache |
| `fanout::the_arms_of_one_key_count_differ_only_in_residency` | the same, for the curve |
| `fanout::a_query_names_distinct_partitions` | a repeated key is deduplicated and the wide points measure fewer partitions than they claim |
| `fanout::the_curve_pins_its_shard_count` | the curve becomes a curve in how keys hashed as well as in *n* |
| `fanout::the_curve_costs_roughly_the_same_at_every_point` | the widest point costs 256 times the narrowest |
| `collect::macro_layer::each_workload_picks_its_own_median` | one workload's outlier chooses every other workload's reported result |
| `collect::macro_layer::a_mismatched_run_file_is_refused` | a leftover scratch file is folded into the wrong workload's numbers |
| `compare::macro_layer::a_workload_on_one_side_only_is_reported_absent` | a workload that stopped being captured looks like one that never regressed |
| `compare::macro_layer::a_capture_sharing_no_workload_is_reported_disjoint` | comparing across the F8 boundary prints an empty table instead of saying why |
| `compare::macro_layer::two_lifted_v1_captures_still_share_their_workload` | the seven pre-F8 captures stop joining with each other and their history is gone |
| `committed_artifacts::every_committed_v1_capture_lifts_to_one_workload` | the lift stops preserving what those captures recorded |
| `committed_artifacts::no_v1_capture_has_an_unmirrored_field` | a field added to the version 1 shape is silently dropped |
| `run::storage::a_directory_of_stores_is_wipeable` | the harness refuses to wipe the tree it just created |
| `run::storage::a_deeply_buried_marker_does_not_unlock_the_guard` | the recursive-delete guard gets easier to satisfy the deeper the tree goes |
| `run::plan::every_run_names_its_workload_and_seed` | a run stops being reproducible from what the plan records |
| `run::plan::the_instrumented_layers_run_only_what_opted_in` | attribution costs more than the rest of a capture |

## Related

- [F3. A three layer performance harness](performance-harness.md) — built the layers this replaces
  the workload of
- [F7. A benchmark runner that renders its own results](bench-runner.md) — built the runner that
  drives them
- [F6. A per query stage breakdown](stage-breakdown.md) — the stage report, which moved into
  `shoal-bench` with the rest of the harness
- [F4. Archives are validated once, not once per read](validated-archives.md) — the
  control-and-null shape the pairs use
- [Benchmarking](../operations/benchmarking.md) — how to take a capture
- [Performance Baseline](../operations/performance-baseline.md) — what the retired `tmdb` numbers
  were, and why they have no replacement
- [Todos](../appendix/todos.md) — the workloads deliberately not built, and the criterion gap that
  stays open
