# 69. Nothing in the workspace ever installed a tracing subscriber

**Partly resolved.** `shoal-workload` now installs one, and the `tracing:` section of a config is no
longer inert for a benchmark capture. `shoalctl` and the tests still install nothing, and the
decision the remainder waits on is unchanged — see [Still open](#still-open) and
[Known Issues](../known-issues.md).

## Symptom

`shoal.yml` carried a `tracing:` section. A capture behaved identically whether it named a level, a
collector, both or neither: silence, on every one of the three hundred and seventy-four workloads.

Underneath, every `#[instrument]` and `event!` in the workspace dispatched to `NoSubscriber` in that
process — the server's `Shard::handle_query`, `Shard::reply` and `Coordinator::send_to_shard`, the
client's spans from [F16](../../features/client-builder.md), and the `event!(Level::ERROR, …)` calls
the error paths use to report a dead connection or a refused frame. An operator debugging a failed
capture got no output from any of them.

## Cause

`shoal-core/src/server/trace.rs` builds the whole pipeline and hands back a `TraceGuard`.
`pub fn setup(conf: &Conf)` is its entry point and **nothing called it** — not `ShoalPool::start`,
not `shard::start`, not `shoal-workload`, not `shoalctl`, not a test. The one exception was
`shoal/examples/tmdb.rs`.

`shoal-bench` compounded it from the other end. `harness/conf.rs` ran

```rust
// a workload's own server is not the thing being observed, and an Info level log per query
// would be
conf.tracing.level = TraceLevel::Warn;
```

on every workload, throwing away whatever level the file asked for. That line was inert — there was
no subscriber for it to filter — but it meant that even once one was installed the config would
still have been overridden.

## Evidence

**Established by reading the source and confirming by grep**, while writing
[F16](../../features/client-builder.md)'s *Performance* section. That feature added spans to the
client and measured them costing nothing across 144 metrics; the question "what do these spans
cost?" turned into "what is listening?" and the answer was nothing. A span whose dispatcher finds no
subscriber is close to free, so the measurement is true and says what those spans cost *in this
configuration* — and this configuration was the only one that existed.

```
$ grep -rn "trace::setup" --include=*.rs .
shoal/examples/tmdb.rs:420:    let traces = shoal_core::server::trace::setup(&conf);
shoal/examples/tmdb.rs:435:    shoal_core::server::trace::shutdown(traces);
```

One caller, in an example.

## The fix

[F34](../../features/benchmark-tracing.md) — the full design is there; the four moving parts:

1. **`trace::setup_with(conf, &TraceOptions)`**, an options form carrying a service name, resource
   attributes, and whether the console layer writes to stderr. `setup` delegates to it with the
   defaults, so the example is untouched.
2. **`workload_main.rs` installs it**, holding the `TraceGuard` across the whole of `harness::run`
   and shutting it down before the artifact line, with `service.name = shoal-workload` and
   `shoal.workload` / `shoal.label` / `shoal.scale` / `shoal.seed` / `shoal.port` on every span.
3. **The override is gone** from `harness/conf.rs`. The file decides.
4. **The artifact records what was on** — `ConfFacts::trace_level` and `ConfFacts::trace_remote` —
   and `compare` names a pair that disagrees rather than comparing them silently. Before this,
   `compare` read no field of `ConfFacts` at all.

Two settings arrived with it because the pipeline was unusable at benchmark volume without them:
`sample_ratio` on `OtlpTracing`, and an `OtlpMetrics` sink that `Tracing::metrics_sink()` will
derive from the trace endpoint.

## Alternatives rejected

**`ShoalPool::start` calling `setup` and holding the guard.** This is the fix direction the item
itself wrote down, and it is rejected for the objection the same item raised against it: a library
that installs a **global** subscriber takes that decision away from every binary embedding it, and
there is exactly one global to take. The install belongs on the binary.

**A `--trace` flag on `shoal-bench run`, defaulting off.** This would have honored the item's other
constraint literally — *a benchmark capture must keep getting the subscriber it has always had* —
by making a traced capture something you have to ask for twice. Rejected because the switch would
then live in two places that can disagree, and because `--conf` travels with the capture where a
flag on one invocation does not. The protection is bought instead by `trace_level` on the artifact
and the guard in `compare`: the mistake is caught rather than prevented, which is the right trade
for a setting somebody may deliberately want to move.

**Leaving the `Warn` override in place and honoring only `remote`.** Half-honoring a config section
is worse than ignoring it: `level: Debug` would have appeared to work and done nothing, which is
the failure this item is about, relocated.

**Letting the console layer keep writing to stdout.** It cannot. See the invariant below.

## Invariants to uphold

- **The workload's console layer writes to stderr.** `run/exec.rs` harvests the hotpath profile
  from the *last line* the workload writes to stdout and inherits stderr unconditionally, so a log
  line on stdout lands inside `<label>.hotpath.json`. The corruption is silent and is found a
  capture later, because a truncated profile reads as code that was never called.
- **The guard outlives the run and is shut down before the last `println!`.** `TraceGuard`'s `Drop`
  is the only flush.
- **Nothing overrides the tracing section in `harness/conf.rs`.** Any field forced there makes the
  config a lie again, which is this item.
- **`trace_difference` returns `None` when either side recorded no facts.** Every capture taken
  before F34 is silent about tracing, and reading that silence as "untraced" would declare the whole
  committed corpus uncomparable with itself.
- **A subscriber is still global.** The install moved to the binary; it did not stop being global.
  Anything that wants a *scoped* subscriber — a test, most of all — needs a non-global path that
  does not exist yet.

## Still open

- **`shoalctl` installs nothing**, so an operator using it gets no structured output.
- **No test can observe any event the server emits.** `setup` installs a global subscriber, so the
  first test to call it would decide what every other test in that binary sees. The recovery
  summary has no automated coverage for this reason
  ([Test Coverage](../test-coverage.md)). Closing it needs a non-global path, which is not what this
  change built.
- **The library question is still unmade.** Whether `ShoalPool::start` should install anything is
  now moot for the benchmark and open for anything else embedding Shoal.
- **What tracing actually costs is still unmeasured.** F34's *Performance* section says so
  explicitly: the cost of a per-query span is quoted from [F5](../../features/flushed-sweep-gate.md)
  by analogy, not measured on this code.

These keep item 69 filed in [Known Issues](../known-issues.md), the way items 16, 20, 24 and 54 are.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_workload_logs_to_stderr` (`workload_main.rs`) | log lines return to stdout and corrupt the hotpath artifact |
| `a_workload_names_itself_on_every_span` (`workload_main.rs`) | a collector cannot separate one workload's spans from another's |
| `an_unlabelled_run_still_reports_itself` (`workload_main.rs`) | the hotpath phase, which passes no `--label`, loses its attributes |
| `the_harness_no_longer_forces_a_level` (`harness/conf.rs`) | the override returns and the config is inert again |
| `facts_record_the_trace_level_and_sink` (`harness/conf.rs`) | a traced capture becomes indistinguishable in the artifact |
| `a_traced_capture_does_not_compare_to_an_untraced_one` (`compare/macro_layer.rs`) | two captures of two different programs compare in silence |
| `matching_trace_facts_are_not_reported` (`compare/macro_layer.rs`) | the warning fires on every ordinary comparison and stops being read |
| `two_lifted_v1_captures_still_share_their_workload` (`compare/macro_layer.rs`) | the pre-F34 corpus is declared uncomparable with itself |
| `resource_attributes_reach_the_exporter` (`trace.rs`) | `TraceOptions::attributes` stops reaching the resource |
| `a_sample_ratio_selects_a_sampler` (`trace.rs`) | an absent ratio starts sampling, dropping spans for every existing config |

## Related

- [F34](../../features/benchmark-tracing.md) — the feature that closed this half
- [Observability](../../operations/observability.md) — what is instrumented, and how to read it
- [F16](../../features/client-builder.md) — the feature whose *Performance* section found this
- [F5](../../features/flushed-sweep-gate.md) — where the cost of a per-query span was last counted
- [Known Issues](../known-issues.md) — the open remainder
- [TODOs](../todos.md) — the metrics surface this does not build
