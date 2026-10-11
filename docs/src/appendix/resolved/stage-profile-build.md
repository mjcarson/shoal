# 227. The `stage-profile` build of the engine had not compiled since F75

The `stage-profile` build breaks a query's latency into stages
([F6](../../features/stage-breakdown.md)). It is how a regression is attributed to a stage,
and how the object store's later baselines will be. [F79](../../features/bucket-schema.md) ran the
runbook's feature-gated lines and found the build had not compiled for two changes. The object
storage plan lists the fix as an optional prerequisite
([S1](../../object-storage/prerequisites.md#optional)): M13's baseline and every later gate's
evidence ask what a query's time went to, and this build is what answers that. The fix changes no
format, and it could have landed at any time.

## Symptom

`cargo check -p shoal-bench --features stage-profile,hotpath --all-targets` fails in `shoal-core`.
That is the runbook line [Test Coverage](../test-coverage.md) gives for the feature-gated
binaries. It fails with two `E0308`s, one in each persistent table's `get_flushed`. Nothing built
with the feature compiled: not `shoal-workload --features stage-profile`, not
`stage_join.rs`, and none of the fourteen tests that only run under the feature. A default build
and `cargo test --workspace` passed throughout, because neither compiles the lines that failed.

## Cause

[F75](../../features/client-cancel.md) (`1a106bd`) added the coordinator's attempt to every
answer a shard releases, so that a cancel can be bounded by it. A table's `flushed` vector went
from five-field tuples to six: `(client, query, attempt, span, stamps, response)`. Every site that
builds or destructures one was updated, and the default build checks all of them, because
`StageStamps` is a zero-sized type without the feature and keeps its place in the tuple
([F6](../../features/stage-breakdown.md#invariants-to-uphold)).

One loop in each table is compiled only with the feature. After `get_flushed` moves what a
watermark released into `flushed`, it fills each answer's durability phases from the intent log's
timeline:

```rust
#[cfg(feature = "stage-profile")]
for (_, _, _, stamps, _) in self.flushed.iter_mut() {
    self.storage.fill_durability(stamps);
}
```

F75 left the pattern at five fields. A default build never sees it, and F75 did not run the
feature's runbook line, so the break surfaced one change later.

## Evidence

**Reproduced.** On europa, against the tree at `1492439`, the runbook line fails with exactly the
two errors the item named:

```text
error[E0308]: mismatched types
    --> shoal-core/src/server/tables/persistent/sorted.rs:2739:13
     |
2739 |         for (_, _, _, stamps, _) in self.flushed.iter_mut() {
     |             ^^^^^^^^^^^^^^^^^^^^    ----------------------- this is an iterator with items of type `&mut (Uuid, Uuid, u64, tracing::Span, StageStamps, shoal_proto::shared::responses::Response<R>)`
     |             |
     |             expected a tuple with 6 elements, found one with 5 elements
     |
     = note: expected tuple `(Uuid, Uuid, u64, tracing::Span, StageStamps, shoal_proto::shared::responses::Response<R>)`
                found tuple `(_, _, _, _, _)`

error[E0308]: mismatched types
    --> shoal-core/src/server/tables/persistent/unsorted.rs:2227:13
```

**No test would have caught the loop doing nothing**, which is the second half of this item.
Making it compile could have been done just as easily by deleting it. The only test that runs
a server under the feature, `stage_join.rs`, asserted that the insert bucket had *some* stages.
The report builder's unit tests stamp the durability phases by hand. To check that the new
assertion bites, the fix was applied, the unsorted loop's body was replaced with nothing, and
`cargo test -p shoal-bench --features stage-profile --test stage_join` was run:

```text
test a_grid_arm_joins_its_stage_records ... FAILED
test a_sorted_grid_arm_reports_its_durability_stages ... ok
macro/grid/unsorted/r50/1024's writes have no `durable_write`, so nothing filled their durability phases: ["client_pool", "client_serialize", "client_write", "decode", "durable_rotated", "exec_queue", "execute", "net_in", "net_out", "query_decode", "reply_queue", "reply_serialize", "route", "shard_queue_in", "socket_write"]
```

A write whose phases nobody filled has no `write_submitted`. The report therefore names its whole
wait `durable_rotated`, the stage for a write a rotation released. The bucket still has fifteen
stages, so the old assertion passed. The sorted arm, whose loop was left in, passed beside it.

**On the lab.** The stage-profile `shoal-workload`, built for znver1 from the fix, ran on titan
(Ryzen Embedded V1756B, `performance` governor, 7.0.0-34). It used the lab's four-core layout:
two shards on cpus 2 and 4, the client under `taskset -c 6,7`, storage on `/opt/shoal` (ext4 on
the 970 EVO), locked memory unlimited, and tracing at `Warn`. Each persistent table's grid arm ran
twice at full scale, about 6 seconds a run. Every report joined all 20,000 records with none
one-sided, none missing their window and none rotated. **Every one of the 9,997 inserts in every
run carried all four phases**, and `durable_rotated` appeared in none of them. Means across the
two runs:

| Arm | inserts | `durable_staged` | `durable_write` | `durable_sync_wait` | `durable_sync` | `release_wake` |
| --- | --- | --- | --- | --- | --- | --- |
| `macro/grid/unsorted/r50/1024` | 9,997 of 9,997 | 8.6 µs | 1,178 µs | 2,136 µs | 3,880 µs | 19.3 µs |
| `macro/grid/sorted/r50/1024` | 9,997 of 9,997 | 8.8 µs | 959 µs | 2,306 µs | 4,118 µs | 20.9 µs |

The sync was about half of a write's total, the wait for one another 30%, and the write itself
12 to 16%. These figures say where a write's time went on one host, and no more. The build adds
clock reads to every query, so they are not latencies to compare with any other build. The last
committed capture of the unsorted arm, F24's, came from the development host at twelve shards: all
four phases at 9,997 samples, and no `durable_rotated`. It is the same shape, from another
server.

## The fix

- **Both loops name six fields**, `for (_, _, _, _, stamps, _)`. The comment above each now spells
  out the tuple and says that only a stage-profile build compiles it.
- **`stage_join.rs` holds each table to its phases.** The arm's body moved into `run_arm`, which
  keeps every assertion that holds for any arm. `assert_durability_phases` adds two checks on the
  insert bucket:
  - it has `durable_write` and `durable_sync`, each with samples, under the committed
    configuration's fsync durability;
  - no more inserts are reported whole as `durable_rotated` than the report counts as rotated or
    as missing their window.

  The unsorted arm, `a_grid_arm_joins_its_stage_records`, runs both checks. A new test,
  `a_sorted_grid_arm_reports_its_durability_stages`, runs `macro/grid/sorted/r50/1024` with the
  same checks on port 13873. It is the first test to run the sorted table's loop.
- **The arms hold a lock.** The profiler collects every shard's records into one buffer a
  process, which the harness resets and drains around an arm, so two arms in one test binary at
  once would each take the other's records. The tests serialize on a `Mutex` in the file, and a
  poisoned one is taken anyway, so one failure does not fail the other test.
- **The stale count is corrected.** The file's module doc said eight tests in
  `workloads::stages`, and so did [Test Coverage](../test-coverage.md). There are ten, so both now
  say ten.

## Alternatives rejected

**A struct in place of the tuple.** Named fields would let the loop say `entry.stamps`, and a
field added later would not break it. But the tuple's type is spelled in `storage.rs`'s three
release paths, in both tables and through the shard's sweep. The zero-sized `StageStamps` design
rests on that tuple's arity being the same with and without the feature. A refactor of every
answer's path is a change of its own, not a fix to a profiling build. The test now catches what
the struct would have prevented.

**Turning `stage-profile` on in some workspace member.** Cargo unifies features across a
workspace build, so a member that enabled it would make every `cargo build` and
`cargo test --workspace` an attribution build. That build adds clock reads to every query, and
nothing measured from it is a baseline ([F6](../../features/stage-breakdown.md#limitations)).

**A test that runs `cargo check --features stage-profile`.** A test that shells out to cargo
takes the target directory's lock its own run holds, or needs a second target directory and
minutes of compile time inside a test. [Resolved #97](stage-join-storage.md) rejected a CI job for
the same binaries for a related reason: the runbook line is the mechanism this repository has.

**Making it compile and nothing more.** That is what the item's fix direction asked for. But a
loop that compiled and did nothing would have passed every test that existed. The phases are what
the loop is for, so the test now asserts them.

## Invariants to uphold

- **A field added to a released answer's tuple is added to both tables' stage loops.** Only a
  stage-profile build compiles them. The runbook line,
  `cargo check -p shoal-bench --features stage-profile,hotpath --all-targets`, is what reports a
  miss; run it with any change to `flushed`'s element type.
- **A released write on a standalone server is looked up in its log's timeline before the shard
  sees it.** `stage_join.rs` holds both tables to `durable_write` and `durable_sync`. A report
  where every write is `durable_rotated` and none were rotated is this loop not running.
- **No two arms profile at once in one process.** The profiler's buffer is the process's. A test
  added to `stage_join.rs` takes the lock through `run_arm`, and its port stays clear of 13871,
  13872 (`x9_report.rs`) and 13873.

## Still open

- Nothing compiles the feature-gated binaries automatically. [Resolved #97](stage-join-storage.md)
  said so, and this item is that gap biting once. Whoever changes a type a stage-profile site
  touches still has to run the runbook line.
- A replicated write has no durability breakdown. A cluster node's table writes to the shard's
  shared WAL (`LogSink::Shared`), which keeps no timeline, so `fill_durability` does nothing for
  it. This is by design, not a defect of this fix, and
  [F6's limitations](../../features/stage-breakdown.md#limitations) now say so.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_grid_arm_joins_its_stage_records` | `shoal-bench/tests/stage_join.rs` | The binary does not compile. With the unsorted loop compiling but not run, it fails naming `durable_write` |
| `a_sorted_grid_arm_reports_its_durability_stages` | `shoal-bench/tests/stage_join.rs` | The binary does not compile. With the sorted loop compiling but not run, it fails naming `durable_write` |
| The runbook line, `cargo check -p shoal-bench --features stage-profile,hotpath --all-targets` | `CLAUDE.md`, [Test Coverage](../test-coverage.md) | The two `E0308`s above |

## Related

- [F75](../../features/client-cancel.md), which added the attempt to the tuple.
- [F79](../../features/bucket-schema.md), whose run of the runbook's feature lines found this.
- [F6](../../features/stage-breakdown.md), the stage breakdown, whose invariants now name the
  loop.
- [Resolved #76](stage-join.md), for which `stage_join.rs` was written.
- [Resolved #97](stage-join-storage.md), the last time a stage-profile binary went uncompiled, and
  the runbook line it added.
- [S1](../../object-storage/prerequisites.md#optional), which lists this as optional.
