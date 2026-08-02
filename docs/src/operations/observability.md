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

`setup` is **not called by `ShoalPool::start`** — the application must call it. The bundled
example does not, so `cargo run --example tmdb` produces no structured logs at all. Nothing
warns about this.

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
| `Shard::handle_flushed` | `shard.rs:539` |
| `Shard::reply` | `shard.rs:479` |
| `FileSystemCompactor::*` | `.../fs/compactor.rs:98`, `:129`, `:153`, `:193`, `:209`, `:278`, `:314` |
| `FileSystem::read_intents` | `.../fs.rs:428-431` |
| `loader::read_partition` | `.../fs/loader.rs:31` |

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
| **Compaction discarded intents** | **WARN** | `msg`, `orphaned_updates`, `updates_after_delete` | `FileSystemCompactor::apply_intents` |

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

Compaction reports separately, and keeps reporting for the life of the shard rather than only at
startup — a startup compaction is dispatched to the compactor task, not awaited.

What is *not* fixed is the discarding itself: a flipped bit mid-log still costs every intent
after it ([Recovery](../storage/recovery.md#truncation-and-corruption)).

### Debug output that is not tracing

Three places print directly to stdout, bypassing the level filter entirely:

- `Networking::to_addr` — "listening on ..." on every call
  (`shoal-core/src/server/conf.rs:111`).
- `compact_if_needed` — a "Compacting ->" line on every rotation (`.../fs.rs:366-370`).
- `PersistentSortedTable::exists` — six `println!`s including a `{:#?}` of the whole partition
  (`.../persistent/sorted.rs:546`, `:553`, `:593`, `:594`, `:613`, `:639`).

See [Known Issues](../appendix/known-issues.md#17-leftover-debug-printlns).

## hotpath

A sampling profiler enabled by a feature flag:

```bash
cargo build --features hotpath
```

Attributes are already scattered through the hot path and become no-ops without the feature:

```rust
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_query(...)
```

`shard.rs:511`

```rust
#[cfg_attr(feature = "hotpath", hotpath::measure_all)]
impl<D: ShoalDatabase> FileSystem<D> { ... }
```

`.../fs.rs:72`

`measure_all` covers every method in the impl block. Instrumented today:

| Scope | Location |
| --- | --- |
| `ShoalPool::start`, `shard::start` | `server.rs:61`, `shard.rs:676` |
| `Shard::{handle_client, reply, handle_query, handle_flushed, evict_data}` | `shard.rs:457`, `:480`, `:511`, `:540`, `:553` |
| `Shard::shutdown_tasks` | `shard.rs:206` |
| `FileSystem` (all methods) | `.../fs.rs:72`, `:212` |
| `PersistentUnsortedTable` (all methods) | `.../persistent/unsorted.rs:110` |

Note `PersistentSortedTable` is **not** instrumented, so a `hotpath` profile of a sorted
workload misses the table layer entirely.

The workspace also has `cargo-flamegraph` available, and a `profile.json.gz` and
`shoal_looper.sh` at the repo root suggest ad-hoc profiling workflows that are not documented.

## What is missing

For running Shoal anywhere real, the gaps are:

- **No metrics.** No Prometheus endpoint, no histograms, and no counters other than the recovery
  ones, which are emitted as an event rather than exposed. Filed in
  [TODOs](../appendix/todos.md#observability).
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
- `trace::setup` is never called by the library.
- `PersistentSortedTable` is not `hotpath`-instrumented.
- ~~Corruption and truncation are warnings with no counters.~~ Counted and summarized per shard
  now, but only as an event — nothing scrapes it, and nothing aggregates across shards.
- Because `trace::setup` is never called by the library, none of these events reach a test. The
  recovery summary has no automated coverage for that reason
  ([Test Coverage](../appendix/test-coverage.md)).
- Three `println!` sites bypass the log level.
- No health checks or runtime introspection of any kind, and no metrics beyond recovery.
