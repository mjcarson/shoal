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
    Grpc: "http://127.0.0.1:4318/v1/traces"
```

Setup is in `shoal-core/src/server/trace.rs`. A stdout `fmt` layer is always installed; if a
remote is configured, an OTLP layer is added on top:

```rust
pub fn setup(conf: &Conf) -> Option<SdkTracerProvider> {
    let local = setup_local(&conf.tracing);
    let registry = tracing_subscriber::registry().with(local);
    match &conf.tracing.remote {
        Some(RemoteTracing::Grpc(endpoint)) => setup_remote("Shoal", endpoint, registry),
        None => { registry.try_init().unwrap(); None }
    }
}
```

`shoal-core/src/server/trace.rs:74-90`

`setup` is **not called by `ShoalPool::start`** — the application must call it. Neither the
bundled example nor the benchmark workloads do, so `cargo run --example tmdb` and a
`shoal-workload` run both produce no structured logs at all. Nothing warns about this.

Two details worth knowing:

- The `RemoteTracing::Grpc` variant is exported over **HTTP**, not gRPC:
  `SpanExporter::builder().with_http()` (`trace.rs:36-40`). The default port in the checked-in
  config, 4318, is the OTLP/HTTP port, so the config is right and the variant name is wrong.
- The remote layer is hardcoded to `LevelFilter::INFO`
  (`trace.rs:54-56`), independent of `tracing.level`. Setting `level: Debug` gives you more on
  stdout and exactly the same spans remotely.
- Batch queue size is 2048 × 100 spans (`trace.rs:41-44`), which is large. Under load,
  dropped spans are more likely than backpressure.

`shutdown(provider)` (`trace.rs:100-110`) must be called to flush the batch processor; skipping
it loses whatever is queued.

## What is instrumented

`#[instrument]` is applied fairly consistently across the server. The spans that matter for
following a query:

| Span | Location |
| --- | --- |
| `Coordinator::handle_client` | `shard.rs:452-457` |
| `Coordinator::send_to_shard` | `shard.rs:413` |
| `Shard::handle_query` | `shard.rs:505-511` |
| `PersistentTable::handle` | `.../persistent/sorted.rs:301` |
| `PersistentTable::{insert,get,exists,delete,update}` | `.../persistent/sorted.rs:328`, `:386`, `:533`, `:666`, `:825` |
| `Shard::reply` | `shard.rs:479` |
| `FileSystemCompactor::*` | `.../fs/compactor.rs:98`, `:129`, `:153`, `:193`, `:209`, `:278`, `:314` |
| `FileSystem::read_intents` | `.../fs.rs:428-431` |
| `loader::read_partition` | `.../fs/loader.rs`, one per partition read, whether it succeeds or fails |
| `PersistentTable::block_on_load` | `.../persistent/sorted.rs`, `.../persistent/unsorted.rs` — one per query parked on a read |
| `PersistentTable::fail_partition` | `.../persistent/sorted.rs`, `.../persistent/unsorted.rs` — one per read that gave up |

**`Shard::handle_flushed` used to be on this list and deliberately is not any more.** It ran once
per message the shard handled and was a parent to nothing — `Shard::reply` attaches itself to the
query's own span rather than to the ambient one — so the span was 711,638 registry slab inserts per
run saying "I ran". [F5](../features/flushed-sweep-gate.md) removed it. Two more spans on per-query
paths are filed as [O25](../appendix/optimizations.md#o25-two-instrument-spans-remain-on-per-query-paths)
for the same reason, and are **not** removed: `reply`'s is real trace structure, and neither is on a
path that usually does nothing.

Spans are propagated **manually across channel hops**, which is the part worth understanding.
An asynchronous message queue breaks tracing's implicit parenting, so `QueryMetadata` carries
a `Span`:

```rust
pub struct QueryMetadata {
    pub client: Uuid,
    pub id: Uuid,
    pub index: usize,
    pub end: bool,
    pub span: Span,
}
```

`shoal-core/src/server/messages.rs:14-26`

captured at fan-out with `Span::current()` (`messages.rs:37-45`) and re-entered on the far
side:

```rust
#[instrument(name = "Shard::handle_query", parent = &meta.span, ...)]
```

`shard.rs:505-506`

The same span travels through `PendingResponse` and back out in `reply`
(`shard.rs:479`), so a trace covers the write, the wait for durability, and the response — even
though they happen in different iterations of the shard loop. That is genuinely useful, and it
is the main reason the flush pipeline is debuggable at all.

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

Three places print directly to stdout, bypassing the level filter entirely:

- `Networking::to_addr` — "listening on ..." on every call
  (`shoal-core/src/server/conf.rs:111`).
- `compact_if_needed` — a "Compacting ->" line on every rotation (`.../fs.rs:366-370`).
- `PersistentSortedTable::exists` — six `println!`s including a `{:#?}` of the whole partition
  (`.../persistent/sorted.rs:546`, `:553`, `:593`, `:594`, `:613`, `:639`).

See [Known Issues](../appendix/known-issues.md#17-leftover-debug-printlns).

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
| `Shard::{handle_client, reply, handle_query, handle_flushed, evict_data}` | `shard.rs:558`, `:581`, `:612`, `:720`, `:733` |
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
- **`trace::setup` is opt-in and undocumented**, so the default experience is no logs.

## Design notes

**Spans over metrics.** Shoal instruments causally — follow one query across shards, across
the flush boundary, and back — rather than aggregating. For a database being actively
developed, tracing a single slow query is more valuable than a request-rate graph, and the
manual span propagation through `QueryMetadata` is the deliberate investment that makes it
work.

**Zero-cost when off.** Both `tracing`'s level filter and `hotpath`'s feature gate compile the
instrumentation away, so the hot path pays nothing in a default release build.

## Limitations

- The remote exporter ignores the configured level.
- `RemoteTracing::Grpc` uses HTTP.
- `trace::setup` is never called by the library **or by anything else** — not the example, not
  `shoal-workload`, not `shoalctl`. So no subscriber is ever installed and **every span and event
  on this page dispatches to nobody**, which makes the `tracing` section of `shoal.yml` inert.
  Filed as [item 69](../appendix/known-issues.md).
- ~~`PersistentSortedTable` is not `hotpath`-instrumented.~~ It is now, along with the partition
  layer, the stream writer, the compactor and the loader — see the table above.
- ~~`partitions.rs` and `client.rs` still have **no `tracing` spans at all**~~ — `client.rs` has
  spans and `hotpath` scopes since [F16](../features/client-builder.md), on `Shoal::send`,
  `ShoalQueryStream::send`, `ShoalConnectionManager::connect_to`, `track_response`,
  `TcpProxy::read_frame` and both `next()`s. `partitions.rs` still has none, so the hottest CPU code
  is invisible to a trace even though `hotpath` covers it.
- ~~Corruption and truncation are warnings with no counters.~~ Counted and summarized per shard
  now, but only as an event — nothing scrapes it, and nothing aggregates across shards.
- Because `trace::setup` is never called by the library, none of these events reach a test. The
  recovery summary has no automated coverage for that reason
  ([Test Coverage](../appendix/test-coverage.md)).
- Three `println!` sites bypass the log level.
- No health checks or runtime introspection of any kind, and no metrics beyond recovery.
