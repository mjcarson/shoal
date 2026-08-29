# 89. One query produced several disjoint traces

The page describing this system's observability
([Observability](../../operations/observability.md)) said that *"spans are propagated manually
across channel hops"* and that *"a trace covers the write, the wait for durability, and the
response"*. Half of that was true. The half that was not is the half a reader of a collector
notices first: a single get of a partition that is on disk arrived in **three separate traces**,
none of which named the others.

## Symptom

A query that misses in memory walks nine instrumented functions across three tasks and two OS
threads. In a collector they arrived as three unrelated traces:

- the query — `Coordinator::handle_client`, `Coordinator::send_to_shard`, `Shard::handle_query`,
  the table's `get` and `block_on_load`, then `Shard::handle_released` and `Shard::reply` — with
  an unexplained gap in the middle where the disk read happened;
- `Fsloader::spawn_task`, on its own;
- `loader::read_partition` and `read_partition_helper`, on their own.

Nothing joined them. The disk read is usually the *whole* latency of a cold get, and it was in a
trace that named neither the query, the client, nor the partition's table.

Two more things were missing rather than fragmented. The trace began at
`Coordinator::handle_client`, which runs when the **coordinator dequeues the bundle** — so the
socket read, and the time the bundle spent queued behind other work, were outside every trace. And
it ended at `Shard::reply`, which hands bytes to a channel; the framing and the `write_vectored`
that actually answer the client were in no span at all.

## Cause

Four separate gaps, which is why no single reading of the code found it.

**`Shard::start` is not instrumented, and neither is `client_rx_relay`.** The shard's message loop
(`shoal-core/src/server/shard.rs`) matches on `ServerMsg` and calls a handler, with no ambient span
of its own. So `Coordinator::handle_client` was an *explicit root*: one new trace per bundle. The
relay that reads the frame off the socket had no span either, and `ServerMsg::Client` carried no
field one could have travelled in.

**The loader hop dropped the context entirely.** `LoaderMsg::Request` carried `table_name` and
`partition_id` and nothing else (`shoal-core/src/server/tables/storage.rs`). `FsLoader::start` runs
as its own detached glommio task, so `Fsloader::spawn_task` opened at the root of an empty stack —
and it then spawned `read_partition` into *another* task with no `.instrument()`, so that was a
third root rather than a child of the second. A spawn carries no ambient span; nothing had made
that explicit.

**`meta.span` was one span per bundle, not per query.** `QueryMetadata::new` called
`Span::current()`, which resolved to `Coordinator::send_to_shard`'s function-level span. Every
query in a batch, and every shard-share of a query split across shards, took that one span as its
parent — so a bundle of a hundred queries produced one flat list of a hundred siblings and the
per-query identity lived only in `index` and `id` fields.

**The egress entered a span rather than opening one.** `client_tx_relay` did `span.enter()` around
the framing and the write, which puts those instants *under* the query's span for anything reading
`Span::current()`, and produces no span for anything reading the trace.

## Evidence

**Established by reproduction.** `shoal/tests/tracing_topology.rs` was written first, run against
the unfixed tree, and reported:

```
---- one_query_produces_one_trace stdout ----
thread 'one_query_produces_one_trace' panicked at shoal/tests/tracing_topology.rs:264:5:
assertion `left == right` failed: one query's spans resolved to 3 separate traces:
  root 204914057923264515 (Coordinator::handle_client): Coordinator::handle_client, Shard::handle_query, PersistentTable::block_on_load, Shard::handle_released, Shard::reply
  root 189151459227467777 (loader::read_partition): loader::read_partition
  root 186899659413782529 (Fsloader::spawn_task): Fsloader::spawn_task

  left: 3
 right: 1
```

The test reads `tracing`'s own `Attributes` rather than an exporter's output, so it is asserting
about the span tree the registry builds rather than about one collector's rendering of it. The
query is a get after a restart, so the partition really is only on disk and the read really is on
the query's latency.

The mechanism by which a lost parent becomes a *new trace* rather than an orphan was checked in the
dependencies rather than assumed. `tracing`'s `Span::child_of_with`
(`tracing-0.1.41/src/span.rs:497-508`) turns a `None` parent into `Attributes::new_root`, and
`tracing-opentelemetry`'s `parent_context` (`tracing-opentelemetry-0.29.0/src/layer.rs:773-810`)
returns `OtelContext::new()` for an explicit-parent span whose parent it cannot see. Both are new
trace ids, and neither warns.

## The fix

**A root at the socket read.** `client_rx_relay` opens `Shoal::request` with `parent: None` the
moment the frame header decodes — before the body read, and after the wait on the preamble, which
is idle time between requests rather than time this request spent anywhere — and hands it to the
shard on `ServerMsg::Client`.

It is `.instrument()`ed over the body read rather than entered around it. Entered would hold a
guard across an `await`, which on a glommio executor serving many connections leaves this span
current while another connection's task runs. `Instrumented` enters on each poll and exits on each
return, which is also what gives the span an end — see the trap below.

**A span per query at fan-out.** `Coordinator::send_to_shard`'s function-level `#[instrument]` was
**removed** and `Coordinator::route` is opened once per query inside its loop, under the request
span. That span is what `QueryMetadata::new` now takes as an argument rather than reading from
`Span::current()`, so a batch is one trace with one subtree per query in it. Everything downstream
already re-parented off `meta.span` and needed no change at all: `Shard::handle_query`,
`handle_released`, `handle_gathered`, `reply`, the `PendingResponse` durability park and the
released-query replay all landed in the right subtree the moment `meta.span` was the right span.

**The loader hop carries the span.** `LoaderMsg::Request` gained one — which cost it `Copy`, since
a `Span` is an `Arc` bump rather than a scalar — and `StorageSupport::load_partition` takes a
`&Span` that `block_on_load` fills from `meta.span`. `Fsloader::spawn_task` opens under it, and
passes its *own* span into the spawned future so `loader::read_partition` is a child of the spawn
rather than a root beside it.

**The fan-in is a link, not a parent.** A load releases *every* query parked on its partition, so
it cannot be the child of more than one of them. `link_released`
(`shoal-core/src/server/tables/storage.rs`) gives every released query but the first a
`follows_from` to the read — an OpenTelemetry span link. The first is the one `block_on_load`
requested the read for, and it is already the read's parent.

**An end on the socket.** `client_tx_relay` keeps entering the query's own span around the framing
and the write, which is what makes `Coordinator::route` cover **routing to response written** —
the server-side latency of that one query. A separate `Shoal::write_response` child was built and
then removed: the query's span already covers the same instants, `StageStamps` measures them far
more precisely, and it was a span per response on the path
[O44](../optimizations.md#o44-one-trace-per-request-costs-a-span-per-query-and-one-per-frame)
prices.

## Alternatives rejected

**One trace per query, rooted at routing.** This is what the symptom literally asks for, and it
cannot include the socket read, the frame validation or the framing, because a frame is a *bundle*:
`Queries<S>` carries one UUID and a `Vec` of queries, and those costs are shared by all of them.
Rooting per query would have left the ingress in a bundle trace joined by links — more traces, not
fewer. A bundle root with a subtree per query is one trace per query in the common case, because
the common case is a bundle of one.

**Leaving the loader alone and linking the read from the query.** Symmetric, and it keeps a cold
get visibly split: the trace would show a gap where the read was and a link out to it. The read
exists because one query asked for it, and being that query's child is the true statement.

**A `traceparent` on the wire, joining the client's spans to the server's.** Out of scope on
purpose. The request preamble is eight bytes with no query id
(`shoal-proto/src/shared/protocol.rs`) and `Queries<S>` is rkyv, so either shape is a protocol
change — filed in [Todos](../todos.md) with both. **Since built**, as
[F35](../../features/wire-trace-context.md), which took the first of the two.

**Linking the flush and compaction traces to the writes they made durable.** A flush covers every
write it happened to catch; it belongs to no query. Left as its own trace, and filed.

**A `Shoal::write_response` span around the socket write.** Built, and removed before this landed.
The query's own span, entered there, covers the same instants; `StageStamps` already measures the
write to a precision a span cannot reach; and it was one more span per response on the path
[O44](../optimizations.md#o44-one-trace-per-request-costs-a-span-per-query-and-one-per-frame)
prices. It would have been the one span here that could safely sit at `DEBUG`, being a leaf — which
is a reason it was cheap, not a reason it was worth having.

**A new span per query *in addition to* the per-bundle one.** Rejected on cost: `#[instrument]`
defaults to `INFO` and the committed `shoal.yml` runs at `Info`, so a span on a per-query path is a
registry slab insert per query on all three hundred and seventy-four workloads. Moving
`send_to_shard`'s span rather than adding one keeps the net cost to a single insert.

## Invariants to uphold

**`QueryMetadata.span` must never be empty.** `#[instrument(parent = &meta.span, …)]` with an empty
parent does not produce an orphan that something would notice — `tracing` turns a `None` parent into
`Attributes::new_root`, so every span downstream silently starts a **new trace**. Anything building
a `QueryMetadata` outside `send_to_shard` has to supply a real span, which is why `new` and
`untimed` take one rather than reading `Span::current()`.

**Every span on the query path stays at one level.** `Shoal::request`, `Coordinator::route`,
`Shard::handle_query`, `Shard::reply` and the loader's two are all `INFO`, which is
`#[instrument]`'s default. A parent that a filter can drop independently of its children re-roots
all of them — the same failure as an empty parent, reached from the other side, and the whole of
[item 90](divergent-layer-filters.md). A leaf could safely sit lower, since nothing hangs off one,
but there is no leaf here worth a span at all.

**A span held across a hop has to be entered somewhere, or it exports with no duration.**
`tracing-opentelemetry` sets a span's end time in `on_exit`, not `on_close`
(`tracing-opentelemetry-0.29.0/src/layer.rs`), and the SDK resolves a missing end as
`end_time.unwrap_or(start_time)` (`opentelemetry_sdk-0.28.0/src/trace/tracer.rs:112`). A span that
is only ever passed around as a parent is never entered by anything, so it exports as a **zero
width tick with its children extending past it** — which reads as a broken trace and is not one.
This was hit while building this fix, and both spans opened here are entered where the interval
they name is: `Shoal::request` over the body read, `Coordinator::route` over the response write.
`tracing_topology.rs` asserts it by name for both.

**A span crossing a spawn has to be passed, never inherited.** `glommio::spawn_local` and
`spawn_local_into` carry no ambient context. Every future spawned on the request path takes its
parent as a value — `read_partition` does, and anything added beside it must.

**The request span's lifetime is what makes its extent right.** It is never entered and is kept
alive only by the clones travelling in `QueryMetadata`. A path that drops the metadata before the
response is written would shorten the root to something that is not the request.

**`link_released` skips the first entry because the first entry asked for the read.**
`block_on_load` requests the load and *then* parks, so the requester is `blocked[0]`. A change to
that order silently turns a link into a cycle from a parent to its own child.

## Still open

- ~~Client and server are still two traces. `Shoal::send` is in another process and nothing on the
  wire carries a trace context.~~ **Closed** by [F35](../../features/wire-trace-context.md), which
  put an optional W3C trace context on the request preamble behind a `Flags` bit and took
  `PROTOCOL_VERSION` to 3. `Shoal::request` keeps the `parent: None` this fix gave it — the parent
  F35 sets is an OpenTelemetry one, resolved by the layer rather than by the registry, and the two
  are independent.
- The writer's `fdatasync` tasks, `ArchiveMap`'s writers and `FileSystemCompactor` are still their
  own traces. A write's response rejoins its query's trace when the watermark moves, because
  `Shard::reply` is parented off `meta.span`, but what made it durable is not in that trace.
- `Shard::handle_gathered` is in the right subtree, but the *shares* a split query fans out to
  other shards are siblings under one `Coordinator::route` span rather than a span each.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `tracing_topology::one_query_produces_one_trace` | Every span a cold get opens resolving to one root, and that root being the socket read rather than `Coordinator::handle_client`. The same test asserts that both hand-opened spans are **entered**, which is a different failure with the same appearance — verified by replacing the relay's `span.enter()` with an empty span, which fails it with `Coordinator::route was opened and never entered` |
| `server::tables::storage::tests::a_read_links_every_query_but_the_one_that_asked` | The two queries a load released that did not ask for it getting a `follows_from` to the read, and the one that did not getting one to its own child |
| `disk_lookups::a_partition_that_was_never_on_disk_is_looked_up_once` | The `block_on_load` span, which this counts, still being opened once per lookup after the span plumbing changed underneath it |

## Related

- [Observability](../../operations/observability.md) — what is instrumented and how it is joined
- [Item 90](divergent-layer-filters.md) — the other way a parent goes missing
- [F34](../../features/benchmark-tracing.md) — what installs a subscriber, and what a level costs
- [F5](../../features/flushed-sweep-gate.md) — the count that says what a per-query span is worth
