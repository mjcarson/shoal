# F34. A capture that honors its tracing config

## Context

`shoal-core/src/server/trace.rs` has built a complete OTLP over HTTP pipeline for a long time:
a `tracing_subscriber` registry, a batch span processor, an exporter pointed at whatever
`tracing.remote` names, and a `TraceGuard` whose `Drop` flushes it. `pub fn setup(conf: &Conf)` is
its entry point, and until this feature its **only caller in the workspace** was
`shoal/examples/tmdb.rs`.

So `shoal-workload` — the process that runs every one of the three hundred and seventy-four
workloads a capture measures — installed no subscriber at all. Three things followed, and they are
[item 69](../appendix/known-issues.md):

- The `tracing:` section of `shoal.yml` configured **nothing** for a capture. A file naming a
  collector behaved exactly like one naming neither a level nor a sink.
- Every `#[instrument]` and `event!` the engine and the client reach dispatched to `NoSubscriber`.
- What [F16](client-builder.md) measured about the cost of the client's spans was a measurement of
  *this* configuration, and this configuration was the only one that existed.

The immediate reason to close it: judging **Grafana as a benchmark viewer** against the explorer
([F29](benchmark-explorer.md)) needs the corpus in a collector, and watching a system on live data
needs spans to be reaching one at all. Neither is possible from a process that emits nothing.

## What it does

**The config is the switch.** `shoal-workload` reads the `tracing:` section of the file it was
pointed at with `--conf` and installs exactly what it asks for — a console layer always, an OTLP
layer when `remote` names a sink, and an OTLP **metrics** pipeline when a metrics sink is
configured or derivable. Nothing configured installs nothing, which is what a capture whose numbers
are going to be committed should be taking.

There is no `--trace` flag. The plan considered one and it is the wrong shape: a capture already
takes `--conf`, and a second switch that could disagree with the file is a second thing to get
wrong.

### Spans

Every span a workload emits carries who produced it, which is what makes one collector usable by
more than one kind of run:

| Attribute | Value |
| --- | --- |
| `service.name` | `shoal-workload` — deliberately **not** `Shoal`, so a capture's spans and a deployment's spans are two series |
| `shoal.workload` | the workload identifier, `macro/grid/unsorted/r50/1024` |
| `shoal.label` | the capture this run belongs to |
| `shoal.scale` | `full` or `smoke` |
| `shoal.seed`, `shoal.port` | what the run was given |

`trace::setup` grew an options form to carry them:

```rust
pub struct TraceOptions {
    pub service_name: String,
    pub attributes: Vec<(String, String)>,
    pub stderr: bool,
}

pub fn setup(conf: &Conf) -> TraceGuard;                              // unchanged, delegates
pub fn setup_with(conf: &Conf, options: &TraceOptions) -> TraceGuard; // new
```

Two settings arrived with it, both in `shoal-core/src/server/conf.rs`:

- **`sample_ratio`** on `OtlpTracing`, wired to `Sampler::TraceIdRatioBased`. Absent means every
  trace, which is what every config written before this behaved as.
- **`metrics`** on `Tracing`, an `OtlpMetrics` sink. Left unset, `Tracing::metrics_sink()` derives
  one from the trace endpoint by swapping `/v1/traces` for `/v1/metrics` — a collector serves both
  on one host and port, and requiring the endpoint twice is requiring two places to forget to
  change it. An endpoint whose path is not the one a rewrite recognizes derives nothing rather than
  guessing.

### Live run metrics

A capture writes its artifact at the end, and a full one is about two hours. `harness/metrics.rs`
ships one point per workload per run as each finishes, so a dashboard fills in over the two hours
rather than appearing at the end of them:

| Instrument | Kind | Unit |
| --- | --- | --- |
| `shoal_bench.rows_per_sec` | gauge | rows/s |
| `shoal_bench.ops_per_sec` | gauge | queries/s |
| `shoal_bench.wall_clock` | gauge | s |
| `shoal_bench.latency` | gauge, by `op` and `percentile` | ms |
| `shoal_bench.rows` | counter, by `counter` | rows |
| `shoal_bench.run.completed` | counter | 1 |

Every point carries `workload`, `label`, `seed`, `timing`, and — where the workload needed a server
— `shards` and `durability`.

`shoal_bench.run.completed` is the progress signal. Counted against the length of
`workload_ids::IDS`, it says how far through a capture a dashboard is looking, and it needed no
runner-side change to produce.

### The artifact says what was on

`ConfFacts` gained two fields, following `tls: bool` for why and the
[F20](configuration-sweeps.md) `Option` fields for how:

```rust
pub trace_level: Option<String>,   // "warn", "info", …
pub trace_remote: Option<bool>,    // whether spans were leaving the box
```

and `compare` reads them. Two captures that traced differently still produce their rows — a
deliberate before-and-after across that change is a real question — but the report says so:

```
  NOT COMPARABLE - these were traced differently on the two sides, so any movement
  above is at least partly the instrumentation:
    macro/insert_unsorted (level warn -> info)
```

Before this, `compare` read no field of `ConfFacts` at all.

## Design choices

**`tracing.level` is the cost knob; `sample_ratio` is the collector's.** This is the fact the whole
design is arranged around, and it is easy to get backwards. A sampler decides *after* `tracing` has
built the span, so it bounds what is serialized and POSTed and not what is spent building it.
`#[instrument]` defaults to `INFO`, so `level: Info` switches on a registry slab insert per query
on `Shard::handle_query`, `Shard::reply` and ~~`Coordinator::send_to_shard`~~ **`Coordinator::route`,
which replaced it** ([Resolved #89](../appendix/resolved/fragmented-query-traces.md)) — the same
class of cost [F5](flushed-sweep-gate.md) measured at **711,638 slab inserts per run** for a single
span it then removed. `Warn` is the capture-grade setting; ~~and is what the committed `shoal.yml`
names~~ — **the committed `shoal.yml` names `Info`**, and has throughout, with a remote sink
alongside it. That sentence was wrong when it was written. Nothing acts on it, so the correction is
the whole of the change: a capture taken against the committed config is a traced capture at
`Info`, and `compare` says so because `ConfFacts` records the level.

**Metrics are recorded per run, from the finished artifact, not per query.** The obvious design is
a histogram fed by `Measurement::record`, which would give the shape of latency *within* a run.
That call is on the measured path, and an instrument on it would make a traced capture slower than
an untraced one by an amount that is a property of the metrics module rather than of the database.
`Meters::record` takes the `MacroCaptureV2` the harness already built, after the server has stopped,
so nothing in it can appear in a number. The within-run shape is not lost — it is what the spans
are for, and a sampled trace carries it with the causal structure a histogram throws away.

**The console layer writes to stderr, in the workload only.** Not cosmetic. The runner harvests the
hotpath profile from the *last line* the workload writes to stdout (`Stdout::LastLine` in
`run/exec.rs`) while inheriting stderr unconditionally, so a log line on stdout lands inside
`<label>.hotpath.json` and is discovered a capture later. `TraceOptions::stderr` exists for this one
reason and `a_workload_logs_to_stderr` is the test that keeps it.

**The harness stopped forcing a level.** `harness/conf.rs` set `conf.tracing.level = Warn` on every
workload. That was harmless while nothing installed a subscriber and is a silent override now that
something does, so it is gone — and what it was protecting is done by recording the level on the
artifact instead, which protects the comparison rather than the number.

**`shoal-core` gained no metrics SDK.** The `OtlpMetrics` *type* lives there because the
configuration does; the pipeline lives in `shoal-bench`, because the instruments are benchmark
measurements. Both `opentelemetry-otlp` and `opentelemetry_sdk` already carry `metrics` in their
default feature sets, so this added instruments to build and **no packages to the lockfile**.

## Alternatives rejected

**A `--trace` flag, defaulting off.** The safest option and the one the plan opened with: captures
stay pristine unless somebody asks. Rejected because it puts the switch in two places that can
disagree, and because the config already travels with the capture — `--conf` is recorded, a flag on
one invocation is not. The safety it bought is bought instead by `trace_level` on the artifact and
the guard in `compare`, which catch the mistake after the fact rather than preventing a deliberate
choice.

**`ShoalPool::start` installing the subscriber.** The fix direction item 69 wrote down. Rejected
for the reason the same item raised against itself: a library that installs a **global** subscriber
takes that decision away from every binary embedding it. The install belongs on the binary, which
is where it now is. That leaves the decision unmade for `shoalctl` and the tests — see *Still open*
in [the resolved page](../appendix/resolved/benchmark-tracing.md).

**A per-query OTLP histogram.** See *Design choices*. Filed in
[the todos](../appendix/todos.md) rather than dropped, because the shape it would give is real —
it just cannot be paid for inside the measured window.

**An OTLP exporter in the runner, for capture progress.** The runner never links `shoal` and is
deliberately the cheapest build in the tree. `shoal_bench.run.completed` gives the same progress
series from the workload side with no new dependency on the runner at all.

**Deduplicating the metrics endpoint by requiring it.** Making `metrics.endpoint` mandatory would
have avoided the rewrite in `metrics_sink()`. Rejected because the common case is one collector,
and two copies of one URL is two places to forget.

## Limitations

- **A traced capture is not comparable to an untraced one**, and this feature does not make it so —
  it makes the difference visible. `compare` names it; nothing refuses.
- **`RUST_LOG` overrides `tracing.level` entirely, per target.** That is deliberate and predates
  this, but it now means an environment variable can change what a capture costs. A capture taken
  with `RUST_LOG` set records the *configured* level in `ConfFacts`, not the effective one.
- **A collector can accept an export and still drop every span.** `opentelemetry-otlp` 0.28 ignores
  `partial_success`, so from inside Shoal a total rejection looks like success —
  [item 87](../appendix/known-issues.md), unchanged by this.
- **The metrics are the benchmark's, not the engine's.** Resident bytes, LRU depth, compaction
  backlog and blocked-query count are still unobservable; a server running on its own still exports
  no metrics at all. That is the [todos'](../appendix/todos.md) *metrics surface* and is untouched.
- **Nothing here is measured.** No capture was taken for this change, so what tracing at each level
  actually costs is stated from F5's figure by analogy and not from a measurement of this code.
- **`shoalctl` and the tests still install nothing.** The remainder of item 69.

## Invariants to uphold

- **The workload's console layer goes to stderr.** stdout belongs to the artifact. Changing
  `trace_options` to `.stderr(false)` corrupts `<label>.hotpath.json` silently.
- **The trace guard outlives the run, and is shut down before the last `println!`.** `TraceGuard`'s
  `Drop` is the only flush; dropping it early stops the export, and flushing after the artifact
  line would put export chatter on stdout.
- **`Meters` never touches the measured path.** It takes a finished `MacroCaptureV2`. An instrument
  reached from `Measurement::record`, from a driver, or from anything inside `harness::run`'s timed
  block breaks the thing this feature is for.
- **`harness/conf.rs` does not override the tracing section.** Any field of it that gets forced
  there makes the config a lie again.
- **New `ConfFacts` fields stay `Option` with `skip_serializing_if`.** The committed corpus parses
  with `serde`, and an un-skipped field rewrites all of it.
- **A capture with no tracing facts on either side is not "untraced".** `trace_difference` returns
  `None` when either side is silent, which is what keeps the whole pre-F34 corpus comparable with
  itself.

## Performance

**Not measured, deliberately.** The change moves `shoal.yml` and files under
`shoal-bench/src/workloads/`, so every committed capture is correctly reported as no longer
describing this tree — but a capture taken to quantify tracing would be measuring the collector on
the other end of the network as much as the code, and the first capture taken with `remote:` live
is an exploratory number rather than a baseline.

What can be said without one:

- With `tracing.remote` unset and `level: Warn`, the shape is the same as before: a console layer
  with nothing to print, and a filter that rejects at the callsite.
- With `level: Info`, the registry allocates a span per query on at least three per-query
  callsites. F5 removed one such span and counted 711,638 slab inserts per run.
- `Meters` costs one OTLP POST per workload run, taken after the server has stopped.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_workload_logs_to_stderr` (`workload_main.rs`) | log lines return to stdout and corrupt the hotpath artifact |
| `a_workload_names_itself_on_every_span` (`workload_main.rs`) | a collector cannot tell one workload's spans from another's |
| `an_unlabelled_run_still_reports_itself` (`workload_main.rs`) | the hotpath phase, which passes no `--label`, loses its attributes |
| `resource_attributes_reach_the_exporter` (`trace.rs`) | `TraceOptions::attributes` stops reaching the resource |
| `a_sample_ratio_selects_a_sampler` (`trace.rs`) | an absent ratio starts sampling, silently dropping spans for every existing config |
| `a_sample_ratio_parses_and_defaults_to_none` (`conf.rs`) | `deny_unknown_fields` rejects the documented setting |
| `a_metrics_sink_parses`, `a_misspelled_metrics_key_is_rejected` (`conf.rs`) | the metrics block silently does nothing |
| `a_metrics_endpoint_defaults_from_the_trace_one` (`conf.rs`) | one collector needs its URL written twice |
| `an_unrecognized_endpoint_derives_no_metrics_sink` (`conf.rs`) | an unfamiliar URL is rewritten into a path the collector does not serve |
| `the_harness_no_longer_forces_a_level` (`harness/conf.rs`) | the override returns and the config is inert again |
| `facts_record_the_trace_level_and_sink` (`harness/conf.rs`) | a traced capture becomes indistinguishable in the artifact |
| `a_traced_capture_does_not_compare_to_an_untraced_one` (`compare/macro_layer.rs`) | the silent-comparison hole reopens |
| `matching_trace_facts_are_not_reported` (`compare/macro_layer.rs`) | the warning fires on every ordinary comparison and stops being read |
| `two_lifted_v1_captures_still_share_their_workload` (`compare/macro_layer.rs`) | the pre-F34 corpus is declared uncomparable with itself |
| `nothing_configured_installs_nothing` (`harness/metrics.rs`) | a capture naming no sink gets a pipeline anyway, and spends the run failing to reach it |
| `a_trace_sink_alone_installs_a_pipeline`, `a_metrics_sink_alone_installs_a_pipeline` (`harness/metrics.rs`) | the derivation stops being used, or metrics start requiring a trace backend |
| `committed_artifacts` (`tests/committed_artifacts.rs`) | the new fields were not `skip_serializing_if` and the corpus stops parsing |

## Related

- [Observability](../operations/observability.md) — what is instrumented, and the runbook for
  reading it in Grafana
- [Configuration](../getting-started/configuration.md) — the `tracing:` section
- [Benchmarking](../performance/benchmarking.md) — the capture runbook
- [Resolved item 69](../appendix/resolved/benchmark-tracing.md) — the defect this closes, and what
  it leaves open
- [F29](benchmark-explorer.md) — the custom explorer this is meant to be judged against
- [F5](flushed-sweep-gate.md) — where the cost of a per-query span was last counted
- [F20](configuration-sweeps.md) — the `ConfFacts` `Option` precedent
