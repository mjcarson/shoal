# F3. A three layer performance harness

## Context

The storage filesystem moved from btrfs to XFS. That change invalidated every performance
statement in this book, and made it obvious that there was nothing to invalidate them
*against*: no recorded numbers, no way to tell a real improvement from run-to-run noise, and no
working profiler.

Four things were wrong at once, and each of them made the others harder to see.

**The profiler was compiled out.** `shoal`'s `hotpath` feature enabled `hotpath/hotpath` but
not `shoal-core/hotpath`, and every `#[cfg_attr(feature = "hotpath", ...)]` attribute lives in
`shoal-core`. `cargo build --example tmdb --features hotpath` — the command in `CLAUDE.md` and
in [Building Shoal](../getting-started/building.md) — produced a profiler with nothing to
report. Anyone who ran it saw an empty table and concluded the code was cheap.

**The only harness could not resolve anything.** [Benchmarking](../performance/benchmarking.md)
measured the `tmdb` example's own run-to-run spread at roughly ±30%. Nearly every entry in
[Optimizations](../appendix/optimizations.md) claims a saving far below that, so the harness
could not have confirmed or refuted any of them. The page said as much: *"None of these are
measured."*

**Shard placement was random.** `Resources::cpus` took its cpus off a `CpuSet`, which is a
`HashSet` iterated in an order that depends on a per process hash seed. Three consecutive runs
at `cores: 16` put shards on three different sets of cpus, several of them doubling up on one
physical core while other cores sat idle. Two runs of the same binary were not comparable to
each other, and nobody knew.

**Baselines could not be trusted or read.** They were unversioned rkyv archives. `.benchmark`
was written when the insert acknowledgement was inert and would still load and compare against
a build where it was not; `.benchmark-old` could not be deserialized at all. Neither file was
present anyway.

## What it does

Three measurement layers, kept deliberately separate because they answer different questions
and interfere with each other.

| Layer | Question it answers | Spread | Artifact |
| --- | --- | --- | --- |
| Criterion micro-benchmarks | did this function get faster | 5–9% by duration, per benchmark | `docs/perf/runs/<label>.micro.json` |
| ~~The `tmdb` macro benchmark~~ Purpose-built workloads since [F8](purpose-built-workloads.md) | did **one path through** the system get faster | ~11% whole-system; median of several runs, per workload | `docs/perf/runs/<label>.macro.json` |
| A `hotpath` profile | which scopes cost the most | perturbs the run | `docs/perf/runs/<label>.hotpath.json` |

~~`scripts/bench.sh <label>` captures all three.~~ A fourth layer was added by
[F6](stage-breakdown.md) — a per query stage breakdown, `docs/perf/runs/<label>.stages.json`,
from its own `--features stage-profile` build. ~~`bench.sh` now captures four.~~ The three shell
scripts were replaced by the `shoal-bench` crate in [F7](bench-runner.md); `shoal-bench run
--label <label>` captures all four, and can capture a subset of them.
The claim below that this page's `hotpath` layer answers "where does the time actually go" was
too strong for what it does: it ranks scopes over a whole run, which cannot say what the queries
at p99 were waiting on. That is F6's question, and it needed a different instrument.

~~`scripts/bench.sh <label>` captures all four. `scripts/compare.sh` diffs a run against
baselines.~~ Both are now `shoal-bench` — see [F7](bench-runner.md), which also gave every capture
a record of the tree it was taken from and made the results a generated page.
[Benchmarking](../performance/benchmarking.md) is the runbook;
[Benchmark Results](../performance/overview.md) is the current result and
[Performance Baseline](../performance/baseline.md) is the frozen one.

**Two baselines, not one.** Each optimization is judged against both
`docs/perf/baselines/B1-performance.json`, which is frozen, and `docs/perf/baselines/trailing.json`,
which advances by one accepted change at a time. The first says what has been gained overall;
the second says what this change did. Either alone misleads — against the frozen baseline a
fresh regression hides inside an earlier win, and against the trailing baseline a run of
individually "neutral" changes can drift a long way from where it started.

## Design choices

**Criterion for anything CPU bound, and nothing else.** The partition layer is 2,243 lines,
is the hottest code in the tree, and is what most `O` entries are about. It cannot see
cross-shard behaviour, IO, or queueing, so the macro layer stays.

The gain is smaller than it first appeared. Criterion's confidence intervals on these
benchmarks are ±0.02%–1.7%, and taking that as the resolution would have been wrong: four
identical repeats moved one benchmark by 22%, which it reported with a ±0.2% interval. The
working threshold is 5–9% depending on benchmark duration, *plus a confirming repeat*, against
10.5% end to end. The larger part
of the gain is not the number — it is that a movement here names the function that moved, where
an 11% movement end to end names nothing.

**A feature gated facade instead of a wider public API.** `server/tables.rs` declares `mod
partitions` privately, so a criterion bench — a separate binary linking `shoal-core` from
outside — cannot see `SortedPartition` at all. `tables::bench_exports` re-exports what the
benches need under a `bench` feature, is `#[doc(hidden)]`, and says in its own docstring that
it promises nothing.

**JSON baselines carrying a version.** A baseline outlives the build that wrote it and has to
be readable by a person deciding whether two numbers are comparable. `BASELINE_VERSION` is
checked on load: a file that parses but was written against a different definition of a sample
is **refused**, loudly, rather than compared. That is the failure `.benchmark` actually caused.

**The median of several macro runs, never the mean.** The macro benchmark's outliers are one
sided — a run can be arbitrarily slow and cannot be faster than the work — so a mean is dragged
by the tail.

**A warmup phase whose rows stay behind.** `--warmup N` moves rows through the system before
sampling starts, and leaves them in place. That is the point: the measured pass then reads
partitions that exist, which is a steady state rather than a second cold start.

**The report shows every scope.** `hotpath`'s default `limit` is 15, which silently truncates
to the fifteen costliest scopes. A profile that is missing entries without saying so reads as
"this code was never called", so the harness sets `limit = 0`.

## Alternatives rejected

**Criterion alone.** It cannot reach the shard mesh, the intent log, group commit, or eviction
— which is where a distributed database's time actually goes. It would have measured the parts
that were easy to measure and called that the system.

**The `tmdb` example alone**, extended with better statistics. This was the tempting option
because it needed no new dependency. It does not work: no amount of statistics recovers a 3%
effect from a measurement whose spread is 30%, and the entire `O` backlog lives below that
floor.

**Making `partitions` fully `pub`.** One line instead of a gated module, and permanent. The
partition types would then be public API, and the next person to change `SortedPartition`
would have to reason about downstream users that do not exist.

**Keeping the rkyv baseline format**, adding a version field to it. Rejected because the
format's real problem is not the missing version — it is that a baseline nobody can read
without running a program is a baseline nobody checks.

**Criterion's own `--save-baseline` as the durable record.** It is used, but only as a
convenience. Its data lives in `target/criterion`, which is not committed and does not survive
`cargo clean`. The committed JSON is the record.

**~~Setting the CPU governor to `performance`.~~** ~~It would narrow the spread. It was not done,
because B0 would then be reproducible only by someone who knew to change a system setting that
is not in the repository. The governor is recorded in the hardware block instead, as
`powersave` / `amd-pstate-epp`.~~

**Reversed.** The machine now runs `performance` / EPP `performance`, and the baseline was
re-captured under it as **B1**. The original objection was real and is answered by writing the
requirement down rather than by declining to meet it: the governor is a stated precondition of
the baseline in [Benchmarking](../performance/benchmarking.md#prerequisites), and the
[hardware block](../performance/baseline.md#hardware) records it. What the objection
got wrong was treating an unrecorded setting and an unmet one as the same problem — the fix for
"nobody knows to set this" is documentation, not accepting avoidable noise. The powersave
capture is kept as `B0-powersave` so the difference between the two is itself measured.

**Instrumenting `PendingGets::resume` and `park`.** Both are a single map operation. The
`hotpath` guard — two `quanta` reads and a channel send — would cost more than the work it
measured and would distort the profile it appeared in. What is actually wanted there is how
long a get sits parked, which is a span between events rather than the duration of a function,
and `hotpath` cannot express it.

## Limitations

- **One machine, one filesystem, one storage device.** Everything here is a Ryzen 9 9950X with
  `/opt/shoal` on an Intel Optane SSD. The Optane matters more than the CPU does: its fsync
  latency is roughly an order of magnitude below a consumer NVMe, so a conclusion drawn here
  about whether the fsync is worth optimizing does not transfer to other hardware.
- **The client and the server share a box.** They are on disjoint physical cores, but they
  share L3, the memory controller, and the kernel.
- ~~**The archived read path is benchmarked one step removed.**~~ **Fixed**, by
  [F4](validated-archives.md). It read: *`MaybeLoaded::Accessible` holds a glommio `ReadResult`
  whose constructors are crate private — one can only come from a real DMA read.
  `partition_sorted/archived/*` therefore measures the access, the walk in archived key order, and
  the projection, but not the enum dispatch around them.* That was true, and it was the reason F4
  gave `MaybeLoaded` a defaulted buffer parameter: `partition_sorted/maybe_loaded/*` now calls the
  real `get` and `exists`. `archived/*` is kept beside it as a control on the codec itself.
  **A benchmark of `PersistentSortedTable::get` is still missing**, which is one layer further up
  and is what O5, O12 and O13 need.
- **There is no micro-benchmark of the storage write path.** Timing `StreamWriter::write` and
  `start_sync` needs a glommio `LocalExecutor` inside criterion's sampling loop. The `hotpath`
  layer covers it instead, which gives attribution but not a confidence interval. Filed in
  [TODOs](../appendix/todos.md).
- **Macro timing is still per batch**, not per query. `--per-query` was not built; see
  [Benchmarking](../performance/benchmarking.md#what-is-actually-measured).
- **The macro benchmark needs a dataset that is not in the repository** — a 65 MB TMDB CSV. The
  micro layer is self contained; the macro layer is not.
- **`bench_exports` is unsupported API.** Nothing outside `shoal/benches` may use it.
- **The micro layer's reproducibility is 5–9%, not the ±1% its confidence intervals imply**, and
  that band does not bound it: across four identical repeats `get_key/4096` moved 22%, reporting
  its outlying value with a ±0.2% interval. Cross-process variance — allocator layout, code and
  data alignment, ASLR, thermal state — is invisible to an interval computed inside one
  process, so a tight interval is not evidence of anything. (The CPU governor is *not* on that
  list: it was suspected and then measured, and it makes no detectable difference.)
  `shoal-bench compare` applies a duration-tiered band as a screen — ±9% below 1 µs, ±5% above,
  because a fixed-cost perturbation is a large fraction of a 25 ns benchmark and a rounding
  error in a 200 µs one — and **a single capture is not sufficient to accept a result**: the
  protocol requires a confirming repeat.
- **The comparison takes one capture per side.** It cannot use the repeats that the point above
  makes necessary, so the confirmation is still a manual step. ~~It also could not compare the
  macro layer at all.~~ [F7](bench-runner.md) closed the second half of that: a macro comparison
  is judged on whether two observed intervals are disjoint. The micro half remains open, in
  [TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have).

## Invariants to uphold

- **A `hotpath` build never produces a baseline number.** It installs a collector and takes two
  timestamps around every instrumented scope. ~~`scripts/bench.sh`~~ `shoal-bench run` builds
  separately for this reason, and rebuilds without the feature at the end — **including when a
  phase fails**, which the script could not guarantee — so a stray manual run afterwards does not
  silently measure the instrumented binary.
- **`docs/perf/baselines/B1-performance.json` is never overwritten.** It is the fixed point
  every later number is quoted against. It replaced `B0-powersave.json` only because the
  environment changed before any optimization had been measured against that one; once a single
  row exists in the history table, re-freezing is no longer available and an environment change
  means a new baseline alongside the old, not instead of it.
- **`BASELINE_VERSION` is bumped whenever `BenchResult` changes shape *or whenever what a
  sample measures changes*.** The second half is the one that gets forgotten, and is exactly
  what made the old `.benchmark` dangerous.
- **`shoal.yml` is the benchmark configuration and is committed.** Changing it invalidates B0.
  It was untracked while four pages described it as checked in.
- **`Resources::cpus` must stay deterministic.** It selects one cpu per physical core before it
  doubles up on an SMT sibling, from a sorted candidate list. Reverting either half silently
  reintroduces run-to-run placement noise that looks like a performance change.
- **`target/criterion` is cleared before a capture.** Criterion keeps a directory per benchmark
  id indefinitely, so a renamed or deleted benchmark keeps reporting its last result into every
  later capture.
- **The three artifacts of a run are never mixed.**

## Performance

The harness is not on any serving path. What it cost, and what it bought:

| | Effect |
| --- | --- |
| `hotpath` feature off | No change. Every attribute is `cfg_attr`'d and `measure_block!` expands to its own argument. |
| `hotpath` feature on | A collector thread, plus two `quanta::Instant` reads and a channel send per instrumented scope. Not comparable to an uninstrumented build; that is what the separate artifact is for. |
| `bench` feature | Re-exports only. No code generated. |
| `-Ctarget-cpu=native` | Now actually applied. It was claimed by the docs and silently ignored by cargo, so every earlier number was built without it. |
| Deterministic cpu selection | Removes a variance source that was invisible and unbounded. |

Two design claims that had never been measured now are. `partition_sorted/get_range_64` is flat
at ~3.03 µs across partitions of 256, 1,024 and 4,096 rows, which is
[F1](sort-key-ranges.md)'s "paging costs a page, not a partition" shown rather than argued. And
`partition_sorted/archived/access_and_one_row` is *not* flat — it grows with the partition,
because `access` validates the whole buffer before anything can be sought in it. That is a cost
a cold single row get pays for the size of the partition it landed in, and it is filed as
[O23](../appendix/optimizations.md).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `conf::tests::misspelled_resource_key_is_rejected` | A typo in a resource setting is silently ignored again, and a config file stops being a record of what ran |
| `conf::tests::exclude_cores_is_honored` | The correctly spelled key stops being parsed |
| `conf::tests::excluded_cores_leave_the_cpuset` | An excluded core can be scheduled on again |
| `conf::tests::cpu_selection_is_deterministic` | Shard placement goes back to depending on the hash seed, and two runs of one binary stop being comparable |
| `conf::tests::cpu_selection_fills_physical_cores_first` | Two shards share a physical core while another sits idle |
| ~~`bencher::tests::a_written_baseline_is_loaded_back`~~ | — |
| ~~`bencher::tests::a_baseline_from_another_version_is_refused`~~ | — |
| ~~`bencher::tests::throughput_counts_rows_not_samples`~~ | — |
| ~~`bencher::tests::throughput_of_an_instant_run_is_zero`~~ | — |

**The four struck-through tests were deleted by
[F8](purpose-built-workloads.md), along with the code they covered.** They pinned
`shoal::bencher`'s baseline file: loading a prior result, refusing one from another schema version,
and computing throughput. That was a second comparison engine, and `shoal-bench` has owned
comparison since [F7](bench-runner.md) — two engines can only disagree, and only one of them is
read. `shoal::bencher` no longer exists; its percentile and summary statistics moved to
`shoal-bench`'s workload harness and are still tested there.

The lesson those tests encoded did not go with them. **A file that parses and is not comparable
costs a wrong answer, where one that fails to parse costs only a comparison** — which is exactly
why F8's macro artifact dispatches on its version field rather than letting serde guess, and why a
capture that shares no workload with its baseline says so instead of printing an empty table.

The benches themselves are not tests and are not run by `cargo test`. `cargo check --workspace
--all-targets` compiles them; ~~`scripts/bench.sh`~~ `shoal-bench run` runs them. What
`shoal-bench` itself is tested by is on [F7](bench-runner.md#tests).

## Related

- [Benchmarking](../performance/benchmarking.md) — how to run one
- [Performance Baseline](../performance/baseline.md) — the recorded numbers and the hardware they came from
- [Observability](../operations/observability.md) — what `hotpath` covers
- [Optimizations](../appendix/optimizations.md) — the backlog this exists to adjudicate
- [Resolved #18](../appendix/resolved/excluded-cores-typo.md) — the config typo, and the placement defect found underneath it
