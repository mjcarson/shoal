# Observability

Shoal has two instrumentation systems: `tracing` with an optional OpenTelemetry exporter, and
an optional `hotpath` profiler. It has no metrics endpoint, no health check, and no
introspection API.

## Tracing

Configured under `tracing:` in the config
([Configuration](../getting-started/configuration.md)):

```yaml
tracing:
  level: Info                                        # Trace|Debug|Info|Warn|Error|Off
  remote:
    Otlp:
      endpoint: "http://127.0.0.1:4318/v1/traces"    # the full URL, path included
      headers:                                       # optional
        X-Scope-OrgID: Shoal                         # the tenant, on a multi tenant collector
      timeout_secs: 10                               # optional, default 10
      batch_delay_ms: 1000                           # optional, default 1000
      max_queue_size: 8192                           # optional, default 8192
      sample_ratio: 0.001                            # optional, default every trace
  metrics:                                           # optional, derived from remote when absent
    endpoint: "http://127.0.0.1:4318/v1/metrics"
    headers: {}                                      # optional
    interval_secs: 10                                # optional, default 10
    timeout_secs: 10                                 # optional, default 10
```

**`level` is the cost knob; `sample_ratio` is the collector's.** This is easy to get backwards. A
sampler decides *after* `tracing` has built the span, so it bounds what is serialized and POSTed and
not what is spent building it. `#[instrument]` defaults to `INFO`, so `level: Info` switches on a
registry slab insert per query on `Shard::handle_query`, `Shard::reply` and ~~`Coordinator::send_to_shard`~~
**`Coordinator::route`** — the class of cost [F5](../features/flushed-sweep-gate.md) counted at
711,638 slab inserts per run for one span it then removed. Turn `sample_ratio` down to protect the
collector; turn `level` down to protect the measurement.

That third callsite **moved** rather than multiplied.
[Resolved #89](../appendix/resolved/fragmented-query-traces.md) took `send_to_shard`'s
function-level span, which was one per bundle, and reopened it inside the routing loop as
`Coordinator::route`, which is one per query. For a bundle of one — the common case — that is the
same cost; for a bundle of *n* it is *n* − 1 more. `Shoal::request` adds one more per bundle, and
that is the whole of the change: the response write is covered by entering the query's own span
rather than by a span of its own.

`metrics` may be left out. With `remote` set, [`Tracing::metrics_sink`] derives the metrics endpoint
by swapping `/v1/traces` for `/v1/metrics` on the same collector, carrying the tenant header across —
a collector serves both on one host and port, and requiring the URL twice is requiring two places to
forget to change it. An endpoint whose path is not the one a rewrite recognizes derives nothing
rather than guessing. Nothing in `shoal-core` builds a metrics pipeline; the type is there because
the configuration is, and `shoal-bench` owns the exporter.

[`Tracing::metrics_sink`]: ../features/benchmark-tracing.md

`RUST_LOG` overrides `level`, per target, and this is the setting that makes an export problem
diagnosable — the OTLP exporter reports every step of a POST on its own targets at `DEBUG`:

```bash
RUST_LOG=info,opentelemetry-otlp=debug,opentelemetry-sdk=debug,opentelemetry-http=debug
# HttpTracesClient.CallingExport / ReqwestBlockingClient.Send on success
# BatchSpanProcessor.ExportError                            on failure
```

Setup is in `shoal-core/src/server/trace.rs`. A console `fmt` layer is always installed; if a
remote is configured, an OTLP layer is added on top, and `setup` hands back a guard:

```rust
pub fn setup(conf: &Conf) -> TraceGuard;                              // the defaults
pub fn setup_with(conf: &Conf, options: &TraceOptions) -> TraceGuard; // F34
```

`TraceOptions` carries the three things a process that is not a server needs to choose — the
service name it reports as, the resource attributes every span carries, and whether the console
layer writes to **stderr** rather than stdout. That last one is not cosmetic for anything whose
stdout is an artifact; see [F34](../features/benchmark-tracing.md).

**Hold the guard for as long as spans are being emitted.** `SdkTracerProvider` has no `Drop` of
its own, so dropping the guard is the only thing that flushes what the batch processor is holding.
`shutdown(guard)` does the same at a point you name; it is idempotent with the drop that follows.

`setup` is **not called by `ShoalPool::start`** — the application must call it. The bundled example
does, and ~~the benchmark workloads and `shoalctl` still do not, so a `shoal-workload` run produces
no structured logs at all~~ — **`shoal-workload` does now**, since
[F34](../features/benchmark-tracing.md), which is what makes the section above configure anything
for a capture. `shoalctl` and the tests still install nothing, and a *library* installing a global
subscriber is still an open question; that remainder is
[item 69](../appendix/known-issues.md).

Three details worth knowing:

- ~~The `RemoteTracing::Grpc` variant is exported over **HTTP**, not gRPC~~ — **fixed.** The
  variant is now `RemoteTracing::Otlp`, which is what the exporter has always spoken. `Grpc:`
  still parses and means the same thing, so a config written against the old name keeps working;
  `the_deprecated_grpc_spelling_still_loads` pins that.
- ~~The remote layer is hardcoded to `LevelFilter::INFO`, independent of `tracing.level`~~ —
  **fixed.** Both layers are filtered at the configured level, so `level: Debug` now means more
  spans remotely as well as on stdout.
- ~~Batch queue size is 2048 × 100 spans, which is large~~ — **fixed.** The default is 8,192 and
  `max_queue_size` sets it. The scheduled delay is 1 s rather than the SDK's 5 s, so a run shorter
  than five seconds no longer exports only at shutdown.

A trace sink that is unreachable, or that rejects an export, is logged and never panics. It must
not be able to take the database down with it.

**A collector can accept an export and still drop the spans.** `opentelemetry-otlp` 0.28 checks
the HTTP status and ignores the `partial_success` field of the response body, so a collector that
answers `200` while rejecting every span looks exactly like success from inside Shoal. Confirming
delivery means asking the collector, not reading Shoal's logs — filed as
[item 87](../appendix/known-issues.md).

### Reading it in Grafana

A benchmark capture and a running server both report over OTLP to whatever collector the config
names, and they are told apart by `service.name`: a server is `Shoal`, a workload is
`shoal-workload`. Filter on that first, or a capture's three hundred and seventy-four runs and a
deployment's traffic are one series.

A workload's spans carry the run that produced them:

| Attribute | Example |
| --- | --- |
| `shoal.workload` | `macro/grid/unsorted/r50/1024` |
| `shoal.label` | the capture, `f28-rearchive` |
| `shoal.scale` | `full` or `smoke` |
| `shoal.seed`, `shoal.port` | what the run was given |

and the metrics it reports carry the same identity as labels, plus `op`, `percentile`, `counter`,
`shards` and `durability` where those apply:

| Instrument | Kind | Unit |
| --- | --- | --- |
| `shoal_bench.rows_per_sec` | gauge | rows/s |
| `shoal_bench.ops_per_sec` | gauge | queries/s |
| `shoal_bench.wall_clock` | gauge | s |
| `shoal_bench.latency` | gauge, by `op` and `percentile` | ms |
| `shoal_bench.rows` | counter, by `counter` | rows |
| `shoal_bench.run.completed` | counter | 1 |

`shoal_bench.run.completed` is capture progress: count it against the three hundred and seventy-four
identifiers in `workload_ids::IDS` and a panel says how far through a two hour capture the run is.

Three things to know before trusting a dashboard built on these:

- **The metrics are one point per workload run**, recorded from the finished artifact after the
  server has stopped. They are not a live series *within* a run — that is what the spans are for,
  and [F34](../features/benchmark-tracing.md) says why the histogram that would have given it is
  deliberately not there.
- **A traced capture is not comparable to an untraced one.** `shoal-bench compare` says so, reading
  `trace_level` and `trace_remote` off the artifact. A Grafana panel says nothing, so the label is
  what you have to check yourself.
- **A collector can accept an export and drop every span** — see below.

## What is instrumented

`#[instrument]` is applied fairly consistently across the server. The spans that matter for
following a query:

| Span | Location |
| --- | --- |
| `Shoal::request` | `shard.rs`, in `client_rx_relay` — one per frame, and the root of its trace |
| `Coordinator::handle_client` | `shard.rs` |
| `Coordinator::route` | `shard.rs`, in `send_to_shard` — **one per query**, not one per bundle |
| `Shard::handle_query` | `shard.rs` |
| `Shard::handle_released` | a query run again after the partition it parked on was read ([F26](../features/archive-routed-requests.md)) |
| `PersistentTable::handle` | `.../persistent/sorted.rs` |
| `PersistentTable::{insert,get,exists,delete,update}` | `.../persistent/sorted.rs`, one each |
| `Shard::handle_gathered` | `shard.rs`, one per share of a query that was split across shards |
| `Shard::reply` | `shard.rs` |
| `Fsloader::spawn_task` | `.../fs/loader.rs`, one per partition read requested |
| `FileSystemCompactor::*` | `.../fs/compactor.rs`, seven of them |
| `FileSystem::read_intents` | `.../fs.rs` |
| `loader::read_partition` | `.../fs/loader.rs`, one per partition read, whether it succeeds or fails |
| `PersistentTable::block_on_load` | `.../persistent/sorted.rs`, `.../persistent/unsorted.rs` — one per query parked on a read |
| `PersistentTable::fail_partition` | `.../persistent/sorted.rs`, `.../persistent/unsorted.rs` — one per read that gave up |

And the client's, which are in another process and are joined to the ones above by a trace context
on the wire ([F35](../features/wire-trace-context.md)):

| Span | Location |
| --- | --- |
| `Shoal::send` | `client.rs` — only on the bundle path; `send_one` reaches `send_stamped` directly |
| `Shoal::send_stamped` | `client.rs` — the span whose context goes on the wire, and the one a query's answers hang off |
| `ShoalQueryStream::send` | `client.rs`, one per bundle on a stream |
| `Shoal::response` | `client.rs`, in `TcpProxy::relay` — one per frame read back, parented off the `Waiter` |
| `ShoalResultStream::next` | `client.rs`, one per response handed to the caller |
| `ShoalUnorderedResultStream::next` | `client.rs`, the unordered stream's half of the same |
| `ShoalConnectionManager::connect_to` | `client.rs`, one per pooled connection opened |

The line numbers that used to be in this table were removed rather than corrected. Every one of
them had drifted, and a wrong line number reads exactly like a right one — the symbol name is what
to grep for.

**`Shard::handle_flushed` used to be on this list and deliberately is not any more.** It ran once
per message the shard handled and was a parent to nothing — `Shard::reply` attaches itself to the
query's own span rather than to the ambient one — so the span was 711,638 registry slab inserts per
run saying "I ran". [F5](../features/flushed-sweep-gate.md) removed it. Two more spans on per-query
paths are filed as [O25](../appendix/optimizations.md#o25-two-instrument-spans-remain-on-per-query-paths)
for the same reason, and are **not** removed: `reply`'s is real trace structure, and neither is on a
path that usually does nothing.

### How one query stays one trace

Spans are propagated **manually across channel hops**, which is the part worth understanding. An
asynchronous message queue breaks tracing's implicit parenting, so the parent travels in the
message.

The shape is: **one trace per request frame, one subtree per query in it.** A frame is a bundle —
`Queries<S>` carries one UUID and a `Vec` of queries — so the read, the validation and the framing
are shared and belong to the frame rather than to any one query. For a bundle of one, which is the
common case, that is one trace per query.

```
Shoal::send_stamped                      the caller's process: frames the bundle and writes it
├── Shoal::request                       the server: opened in client_rx_relay when the frame lands
│   ├── Coordinator::handle_client       validates the bundle and routes it
│   └── Coordinator::route               one per query in the bundle
│       ├── Shard::handle_query          on the shard that owns the partition
│       │   └── PersistentTable::{handle,get,block_on_load}
│       ├── Fsloader::spawn_task         only if the partition is not resident
│       │   └── loader::read_partition   in a task of its own, on the loader's queue
│       ├── Shard::handle_released       the query replayed once the read landed
│       └── Shard::reply
├── Shoal::response                      the caller again: one per frame the reader task routes
└── ShoalResultStream::next              one per response handed back to the caller
```

**The two processes are one trace only when the client was built with `otel`** and is itself in a
trace. Without it the client sets no flag bit, `Shoal::request` is the root it has always been, and
everything under it is exactly as it was — which is the arm every deployment that has never
configured a collector is in.

`Coordinator::route` is what `QueryMetadata.span` holds, and it is opened per query rather than per
bundle. It used to be `Coordinator::send_to_shard`'s function-level span, which is one per bundle —
so a batch of a hundred queries produced one flat list of a hundred siblings
([Resolved #89](../appendix/resolved/fragmented-query-traces.md)).

**Neither of those two is opened by an `#[instrument]`, and that decides how each is timed.** A
span wrapping a function is entered and exited by the attribute; a span held across a channel hop
is not entered by anything unless something is made to enter it — and
`tracing-opentelemetry` timestamps a span when it is **exited**, resolving a missing end as
`end_time.unwrap_or(start_time)`. So a span that is only ever passed around as a parent exports
with **zero duration**: the trace is joined correctly and the root draws as a tick with its
children extending past it, which reads as a broken trace rather than as a timing bug.

Both are therefore entered where the interval they name actually is. `Shoal::request` is
`.instrument()`ed over the body read, so it covers the frame arriving — instrumented rather than
entered around, because a guard held across an `await` would leave it current while another
connection's task ran on the same executor. `Coordinator::route` is entered in `client_tx_relay`
around the framing and the socket write, so it covers **routing to response written**, which is the
server-side latency of that one query. There is no separate span for the write: the query's own
span already covers it, and `StageStamps` measures it far more precisely than a span would.

Every hop re-parents off it explicitly:

```rust
#[instrument(name = "Shard::handle_query", parent = &meta.span, ...)]
```

The same span travels through `PendingResponse` and back out in `reply`, so a trace covers the
write, the wait for durability, and the response — even though they happen in different iterations
of the shard loop. It travels through the tables' `blocked` map too, so a get that parked on a disk
read is answered inside the trace it arrived in.

**A load is linked rather than parented, for every query but one.** A partition read serves every
query parked on that partition, so it cannot be the child of more than one of them. It is the child
of the query `block_on_load` requested it for — the first to park — and `link_released`
(`tables/storage.rs`) gives the rest a `follows_from`, which `tracing-opentelemetry` exports as a
span link.

### The trap under all of this

**An empty parent is not an orphan. It is a new trace.**

```rust
let new_span = match parent.into() {
    Some(parent) => Attributes::child_of(parent, meta, values),
    None => Attributes::new_root(meta, values),
};
```

`tracing-0.1.41/src/span.rs:497-508`. So `#[instrument(parent = &meta.span, ...)]` where
`meta.span` is `Span::none()`, or is a span the layer's filter rejected, silently starts a fresh
trace id — no warning, no orphan marker, and console output that looks entirely correct.
`tracing-opentelemetry` has the same fall-through one level down, in `parent_context`, for a parent
its own per-layer filter cannot see.

Two rules follow, and both are load-bearing:

- **Every span on the query path sits at one level** — `INFO`, which is `#[instrument]`'s
  default. A parent a filter can drop independently of its children re-roots all of them. A leaf
  could safely sit lower, since nothing hangs off one, but there is no leaf here that is worth a
  span at all.
- **Every layer of the subscriber is filtered from one source.** `filter_directives`
  (`trace.rs`) decides the directives once and both layers build an `EnvFilter` from that string.
  They used to read different sources, which meant setting `RUST_LOG` — the thing a person does
  when their traces look wrong — could fragment every exported trace
  ([Resolved #90](../appendix/resolved/divergent-layer-filters.md)).

**A span crossing a spawn has to be passed, never inherited.** `glommio::spawn_local` and
`spawn_local_into` carry no ambient context, so every future spawned on the request path takes its
parent as a value.

**A span held across a hop has to be entered somewhere, or it has no duration.** This is the
zero-duration trap above, and `tracing_topology.rs` asserts against it by name for both spans that
are opened by hand rather than by an attribute.

### Events

Only a handful of `event!` calls exist, all at `INFO` except the two recovery summaries:

| Event | Level | Fields | Location |
| --- | --- | --- | --- |
| Eviction | INFO | `pre`, `post`, `removed`, `reclaimed`, `drift`, `partitions`, `evictable` | `.../persistent/sorted.rs` |
| Mark evictable | INFO | `marked` | `.../persistent/sorted.rs` |
| Compaction totals | INFO | `post_compaction`, `precompaction` | `.../fs/compactor.rs` |
| Archive removal | INFO | `msg`, `path` | `.../fs/compactor.rs` |
| Recovery progress | INFO | `msg`, `path` / `gen` | `.../fs.rs`, in `read_intents` |
| Skipped archive | INFO | `archive`, `skip` | `.../fs/compactor.rs` |
| **Recovery summary** | INFO / **WARN** | `msg`, `shard`, `orphaned_updates`, `unreplayable_entries`, `truncated_logs`, `updates_after_delete` | `Shard::report_recovery` (`shard.rs`) |
| **Compaction discarded intents** | **WARN** | `msg`, `orphaned_updates`, `unreplayable_entries`, `truncated_logs`, `updates_after_delete` | `FileSystemCompactor::apply_intents` |
| **Discarded intent log** | **WARN** | `msg`, `path` | `FileSystemCompactor::compact_intent` |

The eviction event is worth reading closely, because it is the only window onto shard memory.
`removed` is summed from the partitions the pass actually dropped and `reclaimed` is how far the
shard counter moved; `drift` is the gap. A persistently non-zero `drift` means the size accounting
is undercounting somewhere ([Known Issues #22](../appendix/known-issues.md#22-size-accounting-inconsistencies)),
not that eviction failed. It is an `INFO` field rather than a warning for exactly that reason
([Resolved #13](../appendix/resolved/eviction-log-underflow.md)).

There are still no counters and no gauges in the sense of something scrapeable. Throughput,
latency, queue depth, resident bytes, cache hit rate, and the number of blocked queries are all
unobservable except by inference from spans.

The one exception is recovery, which does keep counts —
[`RecoveryStats`](../storage/recovery.md#what-recovery-discards), reachable in-process through
`ShoalDatabase::recovery_stats`. It is emitted as an event rather than exposed as a metric, but
it is the shape the rest of this page is missing, and the hook a real metrics surface would read
first.

### Corruption is no longer silent

The intent log reader emits `tracing::warn!` on truncation and checksum failure
(`.../fs/reader.rs`), and replay warns on a skipped entry (`.../fs.rs`). ~~Nothing counts these,
so silently discarding the tail of a log produces one `WARN` line and no other signal.~~

Each of those is now counted, and a shard that discarded anything during recovery says so once,
at `WARN`, before it accepts a connection:

```
WARN Shard::init: msg="Recovery discarded data" shard="Shard-0" orphaned_updates=0
     unreplayable_entries=0 truncated_logs=1 updates_after_delete=0
```

Fixed by [item 9](../appendix/resolved/orphaned-update-intents.md). Two things about it are worth
knowing before relying on it:

- **`updates_after_delete` is not loss.** An update replayed onto a row a delete had already
  taken was meant to be dropped. It is reported alongside the others so the counts that *do* mean
  loss can be trusted, and it never on its own raises the event to `WARN`.
- **The summary is per shard, and there is no pool-wide total.** `ShoalPool::start` spawns its
  shard threads and returns without joining them, so there is no moment at which every shard has
  finished starting. Expect one line per shard and aggregate them yourself.

- **A torn tail on the active log raises this to `WARN` too**, and that is the common case rather
  than a rare one — it is what every unclean shutdown leaves behind, and nothing acknowledged was
  lost in it. Treat a `truncated_logs=1` on a shard that was killed as expected until
  [item 47](../appendix/known-issues.md#47-a-torn-tail-on-the-active-log-is-counted-as-data-loss)
  separates it from real corruption.

Compaction reports separately, and keeps reporting for the life of the shard rather than only at
startup — a startup compaction is dispatched to the compactor task, not awaited. It emits two
distinct events: `apply_intents` counts what it could not apply, and `compact_intent` says when a
log it is about to delete was one it could not read to the end. That second event covers both a
log nothing could be read from and one that gave up part way through after merging what it had —
the second used to be silent, which meant the larger loss was the quieter one
([item 44](../appendix/resolved/compaction-tail-loss.md)).

What is *not* fixed is the discarding itself: a flipped bit mid-log still costs every intent
after it ([Recovery](../storage/recovery.md#truncation-and-corruption)), on the compaction path
as much as the recovery one.

### Debug output that is not tracing

~~Three places print directly to stdout, bypassing the level filter entirely: `Networking::to_addr`,
`compact_if_needed`, and six lines in `PersistentSortedTable::exists`.~~ None is left. The last of
them, the `listening on` line — printed once per shard, and before the bind it announced — went
with [Resolved #38, 58, 88](../appendix/resolved/pool-readiness.md), once `ShoalPool::ready`
reported the address every shard actually bound. The whole item is
[Resolved #17](../appendix/resolved/leftover-printlns.md); nothing in `shoal-core/src` prints to
stdout now.

## hotpath

A profiler enabled by a feature flag. See
[Benchmarking](../performance/benchmarking.md#profile) for how to run one and
[F3](../features/performance-harness.md) for why it is kept apart from the other measurements.

```bash
cargo build --release --bin shoal-workload --features hotpath
```

> Until recently this command produced an **empty profile**. `shoal`'s `hotpath` feature
> enabled `hotpath/hotpath` but not `shoal-core/hotpath`, and every attribute lives in
> `shoal-core` — so the collector was installed with nothing to report, and the empty table
> read as "this code is cheap". The feature now forwards.

Attributes are scattered through the hot path and become no-ops without the feature:

```rust
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_query(...)
```

`shard.rs:612`

```rust
#[cfg_attr(feature = "hotpath", hotpath::measure_all)]
impl<D: ShoalDatabase> FileSystem<D> { ... }
```

`.../fs.rs:108`

`measure_all` covers every method in the impl block. `measure_block!` is used where a plain
attribute would not do: hotpath names a scope `module_path!() + fn_name`, so four different
`get` methods in `partitions.rs` would silently sum into one bucket. Those carry explicit
labels instead.

Instrumented today. A `tmdb` run reported **57** of these, because hotpath only emits a scope that
was actually entered and that workload never evicted a partition — `ValidatedArchive::new`,
`MaybeLoaded::get_archived` and the rest of the archived read path do not appear in a profile of it
at all. That is a fact about the workload rather than about the instrumentation, and it is why the
macro layer [could not adjudicate F4](../features/validated-archives.md#performance):

| Scope | Location |
| --- | --- |
| `ShoalPool::start`, `shard::start` | `server.rs:65`, `shard.rs:861` |
| `Shard::{handle_client, reply, handle_query, handle_released, handle_flushed, evict_data}` | `shard.rs:558`, `:581`, `:612`, `:720`, `:733` — `handle_released` is **new** with [F26](../features/archive-routed-requests.md), and is measured separately from `handle_query` for the reason the two are separate messages: it has no decode to pay |
| `Shard::shutdown_tasks` | `shard.rs:208` |
| `FileSystem` (all methods) | `.../fs.rs:108`, `:248` |
| `PersistentUnsortedTable` (all methods) | `.../persistent/unsorted.rs:123` |
| `PersistentSortedTable` (all methods) | `.../persistent/sorted.rs:175` |
| `SortedPartition::{insert, get, collect_rows}` | `.../partitions.rs`, labelled blocks |
| `MaybeLoaded::{get_archived, seek_archived, collect_archived}`, `SeekBytes::new` | `.../partitions.rs`, labelled blocks |
| `ValidatedArchive::new` | `.../persistent/sorted.rs`, `.../persistent/unsorted.rs`, labelled blocks — one per partition read off disk ([F4](../features/validated-archives.md)) |
| `StreamWriter::{write, prep, consume, flush_oldest_write}`, `write_helper`, `start_sync` | `.../fs/stream.rs` |
| `FileSystemCompactor` (all methods except `start`) | `.../fs/compactor.rs:140` |
| `loader::{read_partition, read_partition_helper}` | `.../fs/loader.rs:19`, `:32` |

Two things about reading the report:

**Long-lived loops report their lifetime, not their cost.** `FileSystemCompactor::start` runs
for as long as the shard does; measured, it reported ~24× the run length and swamped every real
entry. It carries `#[hotpath::skip]`. Instrument another such loop and it will need the same.

**Set `limit = 0`.** The default is 15, which truncates the report to the fifteen costliest
scopes *without saying so* — the first run of this instrumentation appeared to show that
`SortedPartition::insert` was never called.

Not instrumented, deliberately: `PendingGets::{resume, park}`. Both are a single map operation,
and the guard would cost more than the work it measured. What is wanted there is how long a get
sits parked, which is a span between events rather than a function duration.

The workspace also has `cargo-flamegraph` available, and a `profile.json.gz` and
`shoal_looper.sh` at the repo root suggest ad-hoc profiling workflows that are not documented.

## What is missing

For running Shoal anywhere real, the gaps are:

- **No metrics.** No Prometheus endpoint, no histograms, and no counters other than the recovery
  ones, which are emitted as an event rather than exposed. Filed in
  [TODOs](../appendix/todos.md#observability). The throughput figure the benchmark harness
  reports is computed by the *client*, not by the server, and is not available at runtime.
- **No health or readiness endpoint.** Liveness can only be inferred by connecting.
- **No introspection.** No way to ask a running server for its shard count, table list,
  resident bytes, LRU depth, or compaction backlog.
- **No slow-query log.**
- **No structured error reporting to clients** — server-side failures are panics
  ([Wire Protocol](../architecture/wire-protocol.md#limitations)).
- **`trace::setup` is opt-in**, so a binary that does not call it gets no logs. The example and
  `shoal-workload` call it ([F34](../features/benchmark-tracing.md)); `shoalctl` and the tests do
  not.

## Design notes

**Spans over metrics.** Shoal instruments causally — follow one query across shards, across the
flush boundary, across a disk read, and back — rather than aggregating. For a database being
actively developed, tracing a single slow query is more valuable than a request-rate graph, and the
manual span propagation through `QueryMetadata` is the deliberate investment that makes it work.

**The trace is the request, not the handler.** The root is opened when the last byte of a frame
comes off the socket and closes when the last response for that frame is written, so the ingress
queue and the framing are inside it. Rooting at the first instrumented *handler* is the easy thing
and it hides exactly the intervals a slow query is usually slow in.

**Zero-cost when off.** Both `tracing`'s level filter and `hotpath`'s feature gate compile the
instrumentation away, so the hot path pays nothing in a default release build.

## Limitations

- ~~The remote exporter ignores the configured level.~~ It is filtered at `tracing.level` now,
  like the stdout layer — and from the *same* `filter_directives` call, which is what stops the two
  disagreeing ([Resolved #90](../appendix/resolved/divergent-layer-filters.md)).
- ~~`RemoteTracing::Grpc` uses HTTP.~~ The variant is `RemoteTracing::Otlp`, which says so.
  `Grpc:` still parses and still means OTLP over HTTP.
- **A collector that answers `200` can still have dropped every span.** `opentelemetry-otlp` 0.28
  ignores `partial_success` in the response, so from inside Shoal a total rejection is
  indistinguishable from success. Filed as [item 87](../appendix/known-issues.md).
- ~~`trace::setup` is never called by the library **or by anything else** — not the example, not
  `shoal-workload`, not `shoalctl`~~ ~~— **partly.** The bundled example calls it […]
  `shoal-workload`, `shoalctl` and the tests still install no subscriber~~ — **the example and
  `shoal-workload` both call it**, so the `tracing` section of a config now configures a benchmark
  capture as well as the example ([F34](../features/benchmark-tracing.md)). `shoalctl` and the
  tests still install nothing, and the decision the remainder waits on — whether a *library* should
  install a global subscriber at all — is still unmade. Filed as
  [item 69](../appendix/known-issues.md).
- ~~`PersistentSortedTable` is not `hotpath`-instrumented.~~ It is now, along with the partition
  layer, the stream writer, the compactor and the loader — see the table above.
- ~~`partitions.rs` and `client.rs` still have **no `tracing` spans at all**~~ — `client.rs` has
  spans and `hotpath` scopes since [F16](../features/client-builder.md), on `Shoal::send`,
  `Shoal::send_stamped`, `ShoalQueryStream::send` and `ShoalConnectionManager::connect_to`. ~~and
  on `track_response`, `TcpProxy::read_frame` and both `next()`s~~ — **those three were never
  there**, and this list named them for two features. There are four client spans, not seven.
  ~~There are four client spans, not seven.~~ There are **seven** now, and three of them are the
  ones this list twice claimed and never had: `Shoal::response` and both `next()`s arrived with
  [F35](../features/wire-trace-context.md), which needed the return half of a query to be in the
  trace the send opened. `partitions.rs` still has none, so the hottest CPU code is invisible to a
  trace even though `hotpath` covers it.
- ~~**The client's spans and the server's are still two traces.** Nothing on the wire carries a
  trace context: the request preamble is eight bytes with no query id and `Queries<S>` is rkyv, so
  joining them is a protocol change.~~ **They are one trace now**, by
  [F35](../features/wire-trace-context.md): the request preamble grew an optional 26 byte W3C trace
  context behind `Flags::TRACE_CONTEXT`, and `PROTOCOL_VERSION` went to 3 with it. Two things a
  reader of this page has to know about the result. The client's half is behind the **`otel`
  feature**, off by default, so a client built without it is joined to nothing and says so by
  setting no flag bit. And **the sampling decision moved to the client**: the context is adopted as
  a *remote* parent, which a parent based sampler defers to, so `sample_ratio` on a server now
  governs only traces that arrived without one.
- **A flush is its own trace, and that is deliberate.** `StreamWriter`'s `fdatasync` tasks,
  `ArchiveMap`'s writers and `FileSystemCompactor` each cover every write they happened to catch,
  so they belong to no single query. A write's *response* rejoins its query's trace when the
  watermark moves, because `Shard::reply` is parented off the query's span — but what made it
  durable is not in that trace.
- ~~Corruption and truncation are warnings with no counters.~~ Counted and summarized per shard
  now, but only as an event — nothing scrapes it, and nothing aggregates across shards.
- Because `trace::setup` installs a **global** subscriber, none of these events reach a test — a
  test that installed one would decide what every other test in the binary sees. The recovery
  summary has no automated coverage for that reason
  ([Test Coverage](../appendix/test-coverage.md)). [F34](../features/benchmark-tracing.md) did not
  change this: it moved the install onto a binary, which is the right place for a *global* one and
  is no help to a test. What that needs is a non-global path out of `trace.rs`, and there is not
  one.
- **A traced benchmark capture measures a different program.** The level is what costs, and it is
  recorded on the artifact so a comparison across it is named rather than silent — but the cost
  itself has never been measured, only inferred from [F5](../features/flushed-sweep-gate.md).
- Three `println!` sites bypass the log level.
- No health checks or runtime introspection of any kind, and no metrics beyond recovery.
