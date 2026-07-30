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

Only a handful of `event!` calls exist, all at `INFO`:

| Event | Fields | Location |
| --- | --- | --- |
| Eviction | `pre`, `post`, `diff`, `partitions`, `evictable` | `.../persistent/sorted.rs:1006-1013` |
| Mark evictable | `marked` | `.../persistent/sorted.rs:985` |
| Compaction totals | `post_compaction`, `precompaction` | `.../fs/compactor.rs:455` |
| Archive removal | `msg`, `path` | `.../fs/compactor.rs:477-482` |
| Recovery progress | `msg`, `gen` | `.../fs.rs:447`, `:453` |
| Skipped archive | `archive`, `skip` | `.../fs/compactor.rs:338` |

There are no counters and no gauges. Throughput, latency, queue depth, resident bytes, cache
hit rate, and the number of blocked queries are all unobservable except by inference from
spans.

### Corruption is silent

The intent log reader emits `tracing::warn!` on truncation and checksum failure
(`.../fs/reader.rs:53`, `:71`, `:80`, `:88`), and replay warns on a skipped entry
(`.../fs.rs:173`). Nothing counts these, so silently discarding the tail of a log
([Recovery](../storage/recovery.md#truncation-and-corruption)) produces one `WARN` line and no
other signal.

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

- **No metrics.** No Prometheus endpoint, no counters, no histograms.
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
- Corruption and truncation are warnings with no counters.
- Three `println!` sites bypass the log level.
- No metrics, health checks, or runtime introspection of any kind.
