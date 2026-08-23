# 76. The stage layer joined nothing for any grid arm, and reported it as a layer that ran

Filed out of the `f22-row-size` capture and fixed here. The layer had been pointed at three widths
of the grid by [F22](../../features/row-size-benchmarks.md) so that something could finally say
**which** of the nineteen stages grows with the row. All three produced nothing.

## Symptom

An artifact with the right schema, four reports with the right workload names, a plausible join
block, and no data in three of them. From `docs/perf/runs/f22-row-size.stages.json`:

| Report | joined | server only | client only | ops |
| --- | ---: | ---: | ---: | --- |
| `macro/grid/unsorted/r50/1024` | **0** | 40,001 | 0 | `{}` |
| `macro/grid/unsorted/r50/8192` | **0** | 40,001 | 0 | `{}` |
| `macro/grid/unsorted/r50/524288` | **0** | 1,025 | 0 | `{}` |
| `macro/insert_unsorted` | 200,000 | 1 | 0 | `insert` |

Every server-side record was discarded for want of a client half, and the capture's provenance
recorded `joined: 200000` — all of it from the one workload that is not on the width axis, and none
of it from the three that are. The page that draws the width axis
([Row size](../../performance/row-size.md)) reported *"No stage was measured above the clock's own
cost at more than one width"*, which is what an empty `ops` map looks like once it is prose.

**Nothing failed.** The runs succeeded, the collector accepted the reports, `check` passed, the
freshness table showed a stages layer, and the entries in [Optimizations](../optimizations.md) that
name the per-stage breakdown as their evidence went on naming it.

## Cause

Three things, one of which is the defect and two of which are why nobody saw it.

**The client half of a stage record was built in exactly one place.**
`drive_with` (`shoal-bench/src/workloads/harness/driver.rs`) kept a `submitted` map, filled it as
each bundle went out and closed each record out as the matching response arrived; every
`#[cfg(feature = "stage-profile")]` block in the file was inside it. A grid arm's *measured* phase
does not go through it — it calls `drive_mixed_per_query`, which had no stage wiring at all, so
`measured.stage_records` came back empty. The arm's *seed* phase does call `drive_with`, but the
call site discarded the `Measurement` it returned, so those records were dropped before anything
could join them. The server was unaffected and kept stamping every sampled query, which is why the
failure looks like a layer that ran.

**`send_one` could not be stage profiled at all.** This is the part the original filing did not
reach. `ShoalQueryStream::send` returns `BatchStamps`; `Shoal::send` and `Shoal::send_one` did the
same four things — serialize, take a pooled connection, write, hand back — and recorded none of
them. So the wiring `drive_mixed_per_query` was missing could not simply have been copied in: there
were no client-side stamps on that path to copy.

**The check that a report exists was not a check that it contains anything.**
`collect::stages::check` did refuse `joined == 0`, and did refuse a join ratio below 0.5 — on the
**summed** artifact. `insert_unsorted`'s 200,000 joins against three zeros came to 71% of records
seen having a client half, and the layer reported as healthy. This is the same class of defect as
[item 73](stage-artifact-overwrite.md) and was missed for the same reason.

A fourth thing, found on the way: **`shoal::server::stage_profile::reset()` had no call site
anywhere in the tree**, though its doc comment says it is called at the end of a warmup. The epoch
therefore never advanced and the seed phase's server records were never discarded — which is most
of what those 40,001 server-only records were.

## Evidence

**Reproduced**, then fixed, then reproduced again. The original filing was established by reading
the committed artifact against the page generated from it, and traced to the two call sites; this
is the run that confirms it.

```
$ ./target/release/shoal-workload run --id macro/grid/unsorted/r50/1024 \
    --conf shoal.yml --seed 1 --scale smoke --port 13999 \
    --json before.json --stage-json before.stages.json
stage profile: 0 joined, 401 server only, 0 client only, 0 duplicates, 0 saturated
```

The same command against the fixed tree:

```
stage profile: 200 joined, 0 server only, 0 client only, 0 duplicates, 0 saturated, 0 unanswered
```

and the report it wrote, which the unfixed one had no equivalent of:

```
  get:    count=88   all rank, 50,474 ns
      socket_write         15,450 ns    exec_queue      9,218 ns    net_in    8,061 ns
      net_out               6,979 ns    client_write    3,440 ns    client_pool 3,168 ns
  insert: count=112  all rank, 1,001,667 ns
      durable_write       439,867 ns    durable_sync  397,957 ns
      durable_sync_wait   112,709 ns    socket_write   13,280 ns
```

Both smoke-scale runs; neither artifact was kept. `client_write` and `client_pool` appearing in the
read breakdown is the second half of the fix showing up — those are the stamps `send_one` never
produced.

## The fix

**A stage log both driver families use.** `shoal-bench/src/workloads/stage_log.rs` holds `StageLog`,
declared twice the way `StageStamps` and `ClientStamps` are: the real one under `stage-profile` and
a zero sized one without it, whose methods are all empty and inlined. `Measurement` holds one
unconditionally, so **no driver carries a `#[cfg]` any more**. `drive_with` calls `sent` and
`answered`; the per query drivers call `one`, which needs no pending map because a bundle of one is
opened and closed by the same response.

**The one shot path got its stamps.** `Shoal::send_stamped` holds what `Shoal::send` used to do,
with `BatchStamps` marked at the four points the streaming path already marks them, and `send`
delegates to it and drops the stamps. `Shoal::send_one_stamped` is the same arrangement one level
up. `BatchStamps` is a zero sized type without the feature, so a caller that is not profiling pays
nothing and no signature that existed before changed.

**The seed phase is dropped deliberately, on both halves.** `harness::run` calls
`stage_profile::reset()` after the optional restart and before the measured phase, which gives the
never-called function its call site. The client half was already discarded — a workload's `seed`
returns no measurement and has nowhere to put one — and `Grid::seed` now says so at the call site
rather than leaving it to be inferred from a `?`.

**`check` judges each report on its own count.** It loops the artifact's reports and applies the
same two rules per report, naming the workload in the failure, and describes each one in the line
the runner prints. A layer is only as good as its emptiest report, because every report in it is
drawn as though it were a measurement.

**The attribution page reports the join of the report it drew**, rather than the artifact's sum
across every workload the layer profiled. Correct before this only by coincidence.

## Alternatives rejected

**Copying the block into `drive_mixed_per_query`.** The smallest possible fix and the one the
original filing warned against. It repairs the three arms that were broken and leaves the same hole
for the next driver — there are five `drive_*` entry points across two files, and the property worth
having is that a driver *cannot* be written without the bookkeeping, not that today's four have it.
It also could not have worked as stated, because the stamps were not available on that path.

**Leaving the three batch stamps unmeasured on the one shot path.** `ClientRecord`'s
`serialized`, `pooled` and `written` would become `Option<Stamp>` and a per query record would
honestly report them as not measured, touching no crate outside `shoal-bench`. Rejected because
`client_serialize` and `client_write` at 1 KiB against 512 KiB rows is precisely what
[O11](../optimizations.md#o11-a-fresh-alignedvec-per-write-and-per-response) and
[O29](../optimizations.md#o29-a-request-body-is-zeroed-and-then-immediately-overwritten) are asking
about. A fix that leaves the instrument dark exactly where it is wanted is not a fix.

**Changing `Shoal::send` to return the stamps.** Symmetric with `ShoalQueryStream::send`, and a
breaking change to the most used method on the client for the benefit of one profiler. A second
method that the first delegates to costs one line and breaks nothing.

**Sampling the one shot path.** `--stage-sample` is a no-op there and cannot be made to work by
sampling on the client alone: both halves sample on the query index, and a one query bundle's index
is always zero. Having the client keep a subset while the server keeps everything would turn the
difference into `server_only` records and trip the very check this change tightened. Filed in
[TODOs](../todos.md) rather than papered over.

**Taking a capture.** The change alters nothing a default build measures — `StageLog` is a zero
sized type and the drivers compile identically without the feature. It does move the source digest
for the macro, hotpath and stages layers, because `docs/perf/sources.json` points all three at
`shoal-bench/src/workloads`, so every committed capture is now reported as no longer describing
them. That is correct, coarse, and not worth two hours of machine time to paper over. The stage
layer's first capture with data in it will come from whoever next needs the width axis answered.

## Invariants to uphold

- **Every driver hands its halves to the `StageLog` on its `Measurement`.** This is the whole
  property the fix buys. A driver added to `driver.rs` that does not call `sent`/`answered` or
  `one` reproduces this defect for whatever workload uses it, silently, and the artifact it
  produces will look correct.
- **`StageLog` must stay zero sized without the feature.** It sits on every `Measurement` and is
  called once per query. The `size_of == 0` assertion in `stage_log.rs` is what makes it acceptable
  as an unconditional field, and an unconditional field is what keeps the `#[cfg]`s out of the
  drivers.
- **Both halves sample on the query index and must keep the same queries.** The client reads the
  same environment variable the server does, because `shoal-core`'s reader is private. Two sides
  sampling independently leave disjoint sets and nothing to join. The one shot path's index is
  always zero, so it keeps everything on both sides — which is agreement, not a bug, but it does
  mean `--stage-sample` does not bind there.
- **`StageLog::new` reads the rate; the derived `Default` would not.** The rate is a divisor, and a
  derived `Default` would leave it zero. `Default` is written out and calls `new`.
- **`reset()` goes after the last server restart and before the measured phase.** Earlier and it
  throws away nothing; later and it throws away the phase it was meant to keep. A workload that
  restarts its server between seeding and running has both, in that order.
- **`check` judges each report, never the sum.** The sum is what the provenance records, and it is
  the right thing to record; it is the wrong thing to gate on. One healthy report can carry any
  number of empty ones over the threshold.
- **A page that draws one report quotes that report's own join.** The artifact's `join()` is the sum
  across every workload the layer profiled and describes none of them individually.

## Still open

**The three grid reports in the committed captures are still empty**, and re-rendering does not
change that: the fix repairs the instrument, not the artifacts taken with the broken one. Until
somebody takes a stage capture, [Row size](../../performance/row-size.md) still says no stage was
measured at more than one width, and O11 and O29 are blocked on a capture rather than on a defect —
which is the ordinary state of an entry on that page rather than the pathological one they were in.

**`--stage-sample` does not bind on the per query path.** See
[TODOs](../todos.md). Every query is kept on both halves there, which is correct and is not what the
flag says.

**The warmup is not excluded from stage records** on either half, so a report includes the queries
issued before sampling started. `reset()` now has a caller and is the mechanism that would fix it;
what is missing is a hook between the warmup and the measured queries, which the drivers do not
currently expose. Also in [TODOs](../todos.md).

## Tests

| Test | What it pins |
| --- | --- |
| `stage_join::a_grid_arm_joins_its_stage_records` | The reproduction. A real server, one grid arm at smoke scale, and a report with a join in it, both operations broken down and a bucket with stages in it. Fails against the unfixed tree with `joined: 0`. Behind `--features stage-profile` |
| `collect::stages::tests::a_report_that_joined_nothing_fails_beside_one_that_did` | `f22-row-size`'s shape rebuilt: the sum is healthy and the check fails anyway, naming the empty workload. Passes against the unfixed tree, which is the defect |
| `collect::stages::tests::each_report_is_described_on_its_own` | Every workload appears in what `check` returns, so the runner prints one line per report |
| `workloads::stage_log::tests::an_unanswered_record_is_counted` | A query sent and never answered is reported rather than dropped when the driver returns |
| `workloads::stage_log::tests::absorb_pools_both_halves` | Per-slot logs merge, which is what every per query driver depends on |
| `workloads::stage_log::tests::the_sample_rate_decides_which_queries_are_kept` | The streaming path keeps one query in every `sample`, which is the rule the server also applies |

## Related

- [F22](../../features/row-size-benchmarks.md) — the change that pointed the layer at three widths,
  and filed this as the sixth benchmark that did not run
- [F6](../../features/stage-breakdown.md) — what the nineteen stages are
- [Resolved #73](stage-artifact-overwrite.md) — the same class of defect one layer up, and the
  change that gave the stage layer its own workload list
- [Optimizations](../optimizations.md) — O11 and O29, whose instrument this was
- [Known Issues](../known-issues.md) — item 77, the other way a capture can be taken and not
  reported
