# 231. The compactor's job loop was profiled as work

The `hotpath` build reports every measured function's calls and time
([F3](../../features/performance-harness.md)). A loop that runs for as long as its shard does has
no work of its own to report, only its lifetime, so it is skipped. The compactor's job loop had
been, until a rewrite dropped the attribute. [Resolved #229](hotpath-build.md) found it on the
way: making the `hotpath` build compile again meant reading what it measures, and its test is
what reproduced this.

## Symptom

A `hotpath` profile of a grid arm reports `FileSystemCompactor::start` as one of its costliest
scopes, with a total of about four times the run's length:

```text
the compactor's job loop is measured, so the profile reports its lifetime as work: Some(HotpathScope { calls: 24, avg: 3380003106, p50: 3082813439, p90: 3697278975, p95: 3718250495, p99: 3730833407, total: 81120074555, percent_total: 38600.0 })
```

That is 24 loops, two tables on each of twelve shards, each about 3.4 s long, in a run of 21.0 s.
Ranked by `total`, as [Benchmarking](../../performance/benchmarking.md#profile) says to rank
a profile, it sits above every scope that did work. [Benchmarking](../../performance/benchmarking.md#profile)
said the loop "carries `#[hotpath::skip]`", which had stopped being true.

**No committed capture carries it.** Every `*.hotpath.json` under `docs/perf/runs/` was taken
between 2026-08-09 and 2026-08-24, before the skip was dropped. No profile was taken from the
change that dropped it until the build broke ([Resolved #229](hotpath-build.md)), so none could.

## Cause

`bc913e6` (2026-09-15, [Resolved #91](compaction-retry.md)) rewrote the compactor's job loop so
that a job failing before its first write is retried. The new `start` came in after a new
`reset_job`, and the diff carried the old `start`'s docstring and its
`#[cfg_attr(feature = "hotpath", hotpath::skip)]` away with the old body. The impl is under
`hotpath::measure_all`, which measures every method that carries no skip, so `start` was measured
from then on. Nothing ran the feature to show it.

## Evidence

**Reproduced** by `hotpath_profile.rs` on europa, with [Resolved #229](hotpath-build.md)'s boxes
in place and the skip taken out again. The test failed with the line in the symptom, after its
assertions on the compactor's work scopes had passed. With the skip back it passed, with 79 scopes
over 21.0 s. **Read** in `git log -S "hotpath::skip" -- shoal-core`, which names `bc913e6` as the
change that removed it.

The same arm run by hand from the test's debug build, with the skip in place, reports the
compactor's work under the scopes the loop drives: 88 `run_job`, 40 `compact_intent`, 16
`write_partition` and 16 `settle_map` calls in 20.3 s. Its top scopes are the process,
`shoal_workload::main`, and the client's `next` and `read_frame`.

## The fix

- **The loop is skipped again.** `FileSystemCompactor::start` carries
  `#[cfg_attr(feature = "hotpath", hotpath::skip)]` again, with a docstring that says why and names
  this item.
- **The test holds it out.** `a_grid_arm_profiles_the_compactor` fails if
  `…::fs::compactor::start` is in the profile, beside its assertions that the work under it is.

## Alternatives rejected

**Leaving the loop measured and ranking it out.** The renderer could drop scopes whose total
passes the run's length. A function that is genuinely busy on twelve shards at once passes it too
(the B1 profile's `stream::write_helper` is at 12,530%, [item 53](../known-issues.md)), so the
rule would hide real work. The attribute says which scopes are lifetimes at the one place that
knows.

**`#[hotpath::skip]` written bare.** The attribute is spelled through `cfg_attr` everywhere in
the engine, so a build without the feature does not name `hotpath`'s macros at all.

## Invariants to uphold

- **A loop that lives as long as its shard is skipped**, under `hotpath::measure_all` or a
  `measure` of its own. Its work is measured through the calls it makes. The compactor's is
  `start`. The shard's `start` is measured and is not one: it spawns the shard threads and
  returns, one call of 8 ms in the run above.
- **A rewrite of a measured impl keeps its skips.** `measure_all` measures whatever carries no
  skip, so a moved or rewritten method loses its exemption silently. `hotpath_profile.rs` holds
  the compactor's; any other added later wants an assertion of its own there.

## Still open

Nothing else is held to this. Only the compactor's loop is checked, and only when the feature's
test runs, which a default `cargo test --workspace` never does.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_grid_arm_profiles_the_compactor` | `shoal-bench/tests/hotpath_profile.rs` | It fails naming the loop, with its calls and its total |

## Related

- [Resolved #229](hotpath-build.md), whose test found this.
- [Resolved #91](compaction-retry.md), whose rewrite dropped the skip.
- [Benchmarking](../../performance/benchmarking.md#profile), which says what a profile's
  lifetimes look like.
- [F3](../../features/performance-harness.md), the hotpath layer.
