# 229. The `hotpath` build of the engine did not compile

The `hotpath` build attributes a run's time to the engine's functions
([F3](../../features/performance-harness.md)). It is a capture's hotpath layer and the other half
of the attribution beside `stage-profile` ([Resolved #227](stage-profile-build.md)).
[F80](../../features/object-wire.md) ran the runbook's feature lines and found it had not compiled
since F76. The object storage plan lists the fix as an optional prerequisite
([S1](../../object-storage/prerequisites.md#optional)): M13's baseline and every later gate's
evidence ask what a run's time went to, and a profile that cannot be built answers nothing. The fix
changes no format, and it could have landed at any time.

## Symptom

`cargo check -p shoal-bench --features stage-profile,hotpath --all-targets`, the runbook line
[Test Coverage](../test-coverage.md) gives for the feature-gated binaries, fails in `shoal-core`:

```text
error: queries overflow the depth limit!
  |
  = help: consider increasing the recursion limit by adding a `#![recursion_limit = "256"]` attribute to your crate (`shoal_core`)
  = note: query depth increased by 130 when computing layout of `{async fn body of server::tables::storage::fs::compactor::<impl at shoal-core/src/server/tables/storage/fs/compactor.rs:477:1: 478:33>::fold()}`

error: queries overflow the depth limit!
  |
  = help: consider increasing the recursion limit by adding a `#![recursion_limit = "256"]` attribute to your crate (`shoal_core`)
  = note: query depth increased by 130 when computing layout of `{async block@shoal-core/src/server/tables/storage/fs/compactor.rs:476:33: 476:53}`
```

Nothing built with the feature compiled, so `shoal-bench run --layer hotpath`, which builds
`shoal-workload --features hotpath` first, could not take a profile. Raising the limit in
`shoal-core` moved the error into the bench schema's crate, onto its generated `new`, which no crate
attribute in the engine reaches. A default build and `cargo test --workspace` passed throughout.

## Cause

**Laying out a future lays out every future it awaits.** An awaited future is a field of its
caller's state machine, so the compiler computes a caller's layout by computing each awaitee's
first, depth first, and every level is a nested query. On this toolchain (rustc 1.100.0-nightly
2026-09-04) a level costs four: the future itself, then `MaybeDangling`, `ManuallyDrop` and
`MaybeUninit` around the awaitee. The default limit, `recursion_limit`, is 128.

**The engine's methods are several futures each.** A method under `#[instrument(err(Debug))]` is
four: its body, the `Instrumented` wrapper and two async blocks. `hotpath::measure_all` wraps every
method of an impl in `async { guard; body }.await`, one more. So a chain of instrumented methods
costs sixteen frames a call, and twenty under `hotpath`.

**F76 hung about fifty frames under the archive map's commit.** Before the paged archive map
([F76](../../features/paged-archive-map.md), `8439cb6`), committing a map wrote nothing deep.
Since F76, `ArchiveMap::commit` calls `PagedIndex::commit`, which stages the delta through
`RunWriter::finish` into glommio's `remove` and `sync`, its blocking pool and flume's send. Every
chain that commits a map carried all of it:
- a compactor's job, through `settle_map`;
- a table's start, through `compact_map`;
- a rehome, through the fold and the staged map.

The compactor's own chain, from `fold` through `compact_intent`, `write_partition` and `settle_map`
to the map's commit, ran to 130 frames under `hotpath` before the compiler stopped. Collapsed to
one line a future, with the paths shortened:

```text
#128 {async fn body of compactor::fold()}
#124   Instrumented<async block@compactor.rs:920>        #instrument
#113   async block@compactor.rs:476                      hotpath's wrapper
#109 {async fn body of compactor::compact_intent()}
#90  {async fn body of compactor::write_partition()}
#71  {async fn body of compactor::settle_map()}
#63  {async fn body of map::compact_map()}
#52  {async fn body of map::commit()}
#48  {async fn body of index::commit()}
#33  {async fn body of index::stage()}
#18  {async fn body of index::run::finish()}
#14  {async fn body of glommio::io::remove()}
#6   {async block@glommio::sys::blocking::BlockingThreadPool::push}
#1   flume::r#async::SendFut<BlockingThreadReq>
```

**A schema's crate lays out the same chain deeper.** In `shoal-core` the compactor is generic,
and a layout stops where a type parameter's does. The schema's crate lays it out over concrete
types, through to glommio's `DmaStreamWriter::flush_inner`. That is why raising the limit in
`shoal-core` moved the failure into the bench schema's generated `new` (table `new`,
`FileSystem::new`, `spawn_intent_compactor`, `with_capacity`, `compact_map`, then the same tail)
rather than ending it.

**The default build was one future from the same error.** Without `hotpath`, the compactor's
`fold` was 123 or 124 frames deep, four or five short of the limit.

## Evidence

**Reproduced** on europa at `043cbb4` with `cargo check -p shoal-core --features hotpath`, which
gave the two errors above. The stack is rustc's own: the check rerun as
`cargo rustc -p shoal-core --lib --features hotpath --profile=check -- -Z treat-err-as-bug=1` under
`RUST_BACKTRACE=full`, which turns the overflow into a panic that prints every query. The bench
schema's stack came the same way, with the limit raised for `shoal-core` alone through
`-Z profile-rustflags`.

**Bisected to F76.** Each commit was checked out in a worktree beside the repository and built in
a target directory of its own, one after the other:

| Commit | `cargo check -p shoal-core --features hotpath` | Deepest chain |
| --- | --- | --- |
| `ad03c43`, F76's parent | compiles | 105 to 112 frames |
| `8439cb6`, F76 | the two errors above | at least 137 frames |

**Measured, before and after.** A crate's depth was found by compiling it under a lowered limit,
`cargo rustc ... --profile=check -- -Z crate-attr='recursion_limit="N"'`. A pass at N means its
deepest chain is no deeper than N:

| Crate and build | Before | After |
| --- | --- | --- |
| `shoal-core`, default | 123 to 124 | at most 88 |
| `shoal-core`, `hotpath` | 129 to 140 | 97 to 104 |
| `shoal-bench` (the bench schema), `hotpath` | past 128, on the generated `new` | 97 to 104 |
| `shoal-bench` (the bench schema), default | not measured | 89 to 96 |
| `tmdb-dataset`, default | not measured | 89 to 96 |

The first box, on the map's commit, took the engine's default build from 123–124 to at most 88. It
left the bench schema with `hotpath` 113 or more deep, on its generated `fold_intents`. That chain
is the rehome's: `FileSystem::fold_intents`, the compactor's `fold`, `compact_intent`,
`write_partition`, `bound_staged`, `write_staged`, and glommio's `sync`. The second box, on that
fold, brought it to 97–104.

**On the lab, the hotpath build.** `shoal-workload --features hotpath`, built for znver1 from the
fix, ran on titan (Ryzen Embedded V1756B, `performance` governor, 7.0.0-34). It used the lab's
four-core layout: two shards on cpus 2 and 4, the client under `taskset -c 6,7`, storage on
`/opt/shoal` (ext4 on the 970 EVO), locked memory unlimited, and tracing at `Warn`. The intent log
rotated at 1 MiB and the map committed every 1,024 changed partitions, so the compactor's chain
was driven down to the map's commit. Each persistent table's grid arm ran twice at full scale,
about 6.3 s a run. Every run exited cleanly, with no shard short of its registered buffers. Every
profile reported the chain whose layout overflowed, and none reported the job loop
([Resolved #231](compactor-loop-profiled.md)):

| Arm | scopes | `run_job` | `compact_intent` | `write_partition` | `settle_map` | `sync_job` |
| --- | --- | --- | --- | --- | --- | --- |
| `macro/grid/unsorted/r50/1024` | 79 | 46 calls, 1.65–1.80 s | 38, 1.65–1.79 s | 34, 1.29–1.31 s | 34, 0.66–0.70 s | 34, 0.51–0.52 s |
| `macro/grid/sorted/r50/1024` | 91 | 46 calls, 1.76–1.78 s | 38, 1.76–1.78 s | 34, 1.36–1.37 s | 34, 0.64–0.68 s | 34, 0.48–0.52 s |

The totals sum over both shards. They say where a run's time went on one host and no more: the
build takes two clock reads and a channel send at every measured call, so nothing in it is a
latency to compare with another build ([F3](../../features/performance-harness.md)). The client's
`next` and the intent log's `write_helper` were the costliest scopes by total.

**On the lab, the shipped build.** The boxes are in every build, so the cluster in
`tmdb_cluster.yaml` (europa, titan and hyperion, a factor of three) was upgraded to the fix. It ran
F80's build of `33d1cbb` and held the whole dataset as F80 left it: 2,434,118 partitions and about
965 MiB archived on each member, maps committed many times over at 16,384 entries a delta.
- **`verify --read quorum`, before.** All 1,187,691 movies, 1,188,009 release rows by their
  composite key and 58,418 keyword partitions were found, none missing or different.
- **`upgrade` to the fix**, built for znver1, one node at a time with the leader last, in 30 s.
  Every node came back and caught up. Each start opened every table's map and committed it through
  `compact_map`, the boxed commit.
- **`verify`, after.** At quorum, the same counts, none missing or different, in 10.0 s for the
  movies against 4.0 s before: every node had restarted, so its rows were read back from the
  archives through the paged index. Then each member read alone (`--read one --member N`) through
  its own copy gave the same counts: europa in 7.5 s, hyperion and titan in 20.3 s.
- **A second load, on the fix.** The dataset again, 3,382,336 rows in 41.9 s (80,742 a second).
  Once every member's merges had settled, `MovieByKeyword` held 189.2 MiB with 9,708 chained
  partitions, where it had held 205.5 MiB and 14,516. The compactor had folded chains and
  committed its maps in service. `verify` at quorum again found everything, none missing or
  different. `status` showed 3 of 3 copies with writes admitted.

No before-and-after timing was taken. Nothing on a query's path changed: a commit, a stage and a
rehome's fold each make one allocation more, at a rate of one a map delta, one a rehome step, and
one a table folded.

[Resolved #227](stage-profile-build.md) recorded the line compiling at `ce7ddaa`, its own commit.
What that run built is not recorded, and F76 is older than `ce7ddaa`, so whatever it compiled was
not `shoal-core` with `hotpath`.

## The fix

- **The archive map's commit reaches the index through a boxed future.** `ArchiveMap::commit` is
  `Box::pin(self.index.commit(self.state())).await`, and `ArchiveMap::stage_if_full`, the rehome's
  early stage, boxes `self.index.stage()` the same way. These are the map's only two calls into the
  index's write half. A boxed future is laid out on its own, so the index's chain is no longer part
  of any caller's future.
- **The rehome's fold is boxed.** In service a compactor's work is a task of its own, spawned and
  laid out apart from whatever started it. `FileSystem::fold_intents` is the one place a caller
  awaits a compactor inline, and it now awaits `Box::pin(compactor.fold(logs))`.
- **In every build, not only under `hotpath`.** The default build was one future from the same
  failure, and a profile measures the code that ships. Each box is one allocation: a commit at a
  map's delta flush, at a table's start and on a log rotation; a stage in a rehome; a fold for
  each table of each executor a rehome folds. None of them is on a query's path.
- **`hotpath_profile.rs` runs the build.** It runs a grid arm at smoke scale through
  `shoal-workload` built with the feature, the binary a capture's hotpath phase runs, against a
  scratch configuration whose intent log rotates at 16 KiB and whose map commits after every job.
  It holds the profile to the compactor's scopes, from `run_job` down to `settle_map`, and to a
  table's and the storage engine's. The scratch configuration the two server-starting tests share
  moved from `stage_join.rs` to `tests/common/mod.rs`.
- **The room under the limit is a runbook line.** `CLAUDE.md` and [Test Coverage](../test-coverage.md)
  compile `shoal-core` and the bench schema's crate with `hotpath` at a limit of 112, which fails
  once a chain comes within four futures of 128.
- **Found on the way:** the compactor's job loop had lost its `#[hotpath::skip]`, so a profile
  reported its lifetime as work. It is skipped again, and the new test holds it out
  ([Resolved #231](compactor-loop-profiled.md)).

## Alternatives rejected

**`#![recursion_limit = "256"]` in `shoal-core`.** The compiler's own suggestion. It moves the
error into every schema's crate, whose generated code lays out the deepest chains and which no
attribute in the engine reaches. It also hides the growth that caused this, until the next limit.

**An attribute the derive documents for a schema built with `hotpath`.** The item's second fix
direction. Every schema author would pay for the engine's depth: the bench, tmdb, `bench-dataset`
and each spike's schema. And a default build would still have sat one future from the limit.

**`#[hotpath::skip]` on the compactor's thin methods.** `settle_map` and `sync_job` cost four
frames each under `hotpath`. Skipping them loses their scopes, removes nothing from the default
build, and buys back a future or two of a chain that grows with the next feature.

**Boxing only under the feature.** The item's first fix direction says so. It leaves a default
build byte for byte as it was, but that build stood one future from the same error, and a profile
would measure a different shape of the code from the one that ships at exactly the calls it is
read for. Decided with the user in planning.

**Boxing the generated `new`'s table opens.** A boxed future is laid out on its own, but its own
chain is no shorter. Boxing the table opens in the generated code splits off only the schema's
own four frames, and the table's future is laid out with the same tail beneath it.

**Boxing in the compactor's `fold` alone.** That cuts the chain the error named and not the one
the bench schema's `new` overflowed on, which reaches the same tail through a table's start. The
tail is the map's, so the cut is the map's.

**A test that runs cargo with a lowered limit.** A test that shells out to cargo takes the
target directory's lock its own run holds, or needs a second target directory and minutes of
compile time inside a test. [Resolved #227](stage-profile-build.md#alternatives-rejected) and
[Resolved #97](stage-join-storage.md) rejected the same for the same reason. The runbook line is
the mechanism this repository has.

## Invariants to uphold

- **Every awaited future is four of the compiler's layout frames on this toolchain**, and
  `hotpath` adds one future to every measured method. The depth of a chain is the sum of its
  calls, so a new instrumented method in a deep chain costs sixteen frames, twenty under
  `hotpath`.
- **The archive map's commit and stage reach the paged index through a box.** Unboxing either
  puts about fifty frames back on every chain that commits a map, and a default build stood four
  or five frames from the limit without them.
- **A compactor's work is laid out apart from its caller.** In service it is a spawned task; in a
  rehome it is boxed. A third caller that awaits a compactor's `fold` or a job inline carries its
  whole chain.
- **Both builds keep four futures of room under 128.** `shoal-core` and the bench schema's crate
  with `hotpath` compile at a limit of 112 (the runbook lines). A chain that fails there is boxed
  at its own subsystem's boundary, where it leaves the caller's code for a subsystem's work, not at
  the method the error names.
- **The compactor's job loop stays skipped**, and so does any loop that lives as long as its shard
  ([Resolved #231](compactor-loop-profiled.md)).

## Still open

- Nothing builds the feature automatically. `hotpath_profile.rs` runs only when the runbook line
  does, as `stage_join.rs` does. Whoever adds a deep chain still has to run the lines.
- The frames a future costs are the compiler's. A toolchain that wraps an awaitee once more costs
  a frame a level, about twenty-five on the deepest chain, and the room under the limit is measured
  against the toolchain named above.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_grid_arm_profiles_the_compactor` | `shoal-bench/tests/hotpath_profile.rs` | The binary does not compile: the errors above. A fix that compiled by skipping the compactor's methods fails it, naming the scopes it dropped |
| The runbook line, `cargo check -p shoal-bench --features stage-profile,hotpath --all-targets` | `CLAUDE.md`, [Test Coverage](../test-coverage.md) | The errors above |
| The runbook lines, `cargo rustc -p shoal-core` and `-p shoal-bench --lib --features hotpath --profile=check -- -Z crate-attr='recursion_limit="112"'` | `CLAUDE.md`, [Test Coverage](../test-coverage.md) | Both fail: `shoal-core` on `fold`, the bench schema on `new`. With only the rehome's box reverted, the bench schema fails on `fold_intents` |
| The map's and index's tests, the rehome's tests, `cluster_fixture`'s rehome tests | `shoal-core`, `shoal` | Nothing: they hold what a commit, a stage and a fold do, which the boxes do not change |

## Related

- [F76](../../features/paged-archive-map.md), whose index write under the map's commit made the
  chain deep.
- [F80](../../features/object-wire.md), whose run of the runbook's feature lines found this.
- [F3](../../features/performance-harness.md), the hotpath layer.
- [Resolved #227](stage-profile-build.md), the `stage-profile` build's own break, found the same
  way a change earlier.
- [Resolved #231](compactor-loop-profiled.md), found on the way.
- [Resolved #97](stage-join-storage.md), whose scratch configuration both server-starting tests
  share.
- [S1](../../object-storage/prerequisites.md#optional), which lists this as optional.
