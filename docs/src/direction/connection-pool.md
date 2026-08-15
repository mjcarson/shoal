# D6. A production connection pool

## Context

The client's pooling design is good. Splitting the socket so that write halves go in the pool and
read halves go to a proxy, and demultiplexing responses by query id rather than by connection, is
what lets a slow query occupy neither a connection nor anyone else's queue
([The Client](../api/client.md#split-connections)). Nothing on this page proposes changing that.

What is missing is everything around it. There is no deadline anywhere, no health check that works,
no way to configure anything, no failover, no retry, and no cleanup on an abandoned stream. Each of
those is small on its own. Together they are the difference between a client that works and a
client that survives a bad afternoon — **and they are the largest stability return in this chapter
for the least design risk**, because none of them requires deciding anything the way
[D4](encryption.md) and [D7](shard-aware-routing.md) do.

## What exists today

An inventory, because a proposal is only legible against what it replaces.

**No deadlines.** The only timeout in the client is bb8's `connection_timeout` on establishing a
TCP connection:

```rust
.min_idle(10)
.max_size(50)
.connection_timeout(std::time::Duration::from_secs(5))
.idle_timeout(Some(std::time::Duration::from_secs(300)))
.max_lifetime(Some(std::time::Duration::from_secs(1800)))
```

`shoal-core/src/client.rs:141-145`, `Shoal::new`

There is no per-query deadline and no read deadline. `TcpProxy::start`'s `read_exact` calls
(`client.rs:506`, `:527`) and every `recv().await` on a response channel park indefinitely
([TODOs](../appendix/todos.md#timeouts)).

**Health checking is nominal.**

```rust
async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
    // TODO implement a ping/pong type request?
    conn.peer_addr()?;
    Ok(())
}
```

`shoal-core/src/client.rs:81-85`, `ShoalConnectionManager::is_valid`

`peer_addr()` reads local socket state. A server that has gone away without closing the socket is
not detected ([item 23](../appendix/known-issues.md#23-client-stream-and-pool-rough-edges)).

**One endpoint.** `Shoal::new` resolves the address and takes the first result
(`client.rs:130`), so there is no failover, no endpoint list, and no round-robin across nodes.

**Nothing is configurable.** Every number above is a literal. There is no builder and no config
struct — a caller who wants 200 connections, or a 50 ms deadline, has nowhere to put them.
Credentials ([D3](authentication.md)) turned out to be the exception rather than the example: rather
than wait for the builder, [F12](../features/authentication.md) added a second constructor,
`Shoal::with_credentials`. That is fine for one option and is exactly the pressure this page
describes — a **third** constructor is the point at which it stops being fine.

**A dead socket strands its in-flight queries.** bb8 reconnects transparently on the next call, and
the new read half is pushed to the proxy. But a query whose response was in flight on the dead
socket never completes: its `channel_map` entry is never removed and its caller's `next()` waits
forever.

**An abandoned stream leaks.** The release runs only in `next`'s `if end` arm
(`client.rs:1032-1039`, and `:1231-1238` for the unordered stream), and neither stream type
implements `Drop`
([item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client)).

**Nothing is bounded and nothing is measured.** Response channels are `kanal::unbounded_async`
([item 15](../appendix/known-issues.md#15-no-backpressure-anywhere)), and `client.rs` has no
`tracing` spans and no `hotpath` scopes at all
([O28](../appendix/optimizations.md#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends)).

## The design

Eight pieces, most of them independent.

### A builder

```rust
Shoal::<TmdbClient>::builder()
    .endpoints(["10.0.0.1:12000", "10.0.0.2:12000"])
    .pool(PoolConfig { min_idle: 10, max_size: 50, .. })
    .deadlines(Deadlines { connect, request, idle, .. })
    .tls(..)            // D4
    .credentials(..)    // D3
    .retry(RetryPolicy::ReadsOnly)
    .build().await?
```

Table stakes, and it is what makes [D3](authentication.md) and [D4](encryption.md) expressible at
all — adding credentials to a three-argument constructor is what makes the builder overdue rather
than optional. `Shoal::new(addr)` stays as the shorthand.

### Health checks that work

The [D2](framing.md) `Ping` / `Pong`, used by `is_valid` and by an idle keepalive.
[TODOs](../appendix/todos.md#client-and-ui) records the `client.rs:82` TODO as needing "a
message-type field the wire format does not have" — D2 is that field, and this is the entry it
closes.

### Deadlines

A per-query deadline around the `recv` in `ShoalResultStream::next`, and a read-idle deadline on
the proxy's socket loop. The first is a few lines; **the second half of it is not.** A client that
gives up on a query has to tell the server, or the server keeps working on it and eventually writes
a response into a channel with no reader — the same failure as item 60, arrived at from the other
direction. That is [D2](framing.md)'s `Cancel`. A deadline without `Cancel` converts a slow query
into a leak.

### Connection death

**This is the honest cost of the split-socket design and it deserves stating plainly.** To fail
exactly the streams affected by a dead socket, the client would have to know which connection each
bundle went out on — which it deliberately does not track, because not tracking it is what lets the
pool hand out connections freely. Three ways out:

| Approach | Cost |
| --- | --- |
| Track bundle → connection | An entry per in-flight bundle, and the pool's freedom is now conditional. Precise |
| Fail every outstanding stream when any connection dies | Over-broad — unrelated queries on healthy connections are failed too. Trivial |
| `GoAway` before a clean close, so the server drains first | Handles the common case exactly and the abrupt case not at all |

**Recommend `GoAway` plus over-broad failure.** A clean server shutdown or drain is the frequent
case and `GoAway` makes it correct; an abrupt socket death is rare, and failing extra queries with
a retryable error on a rare event is a much better trade than paying per-bundle bookkeeping on
every query forever. The precise option is the kind of correctness that costs more than the
incorrectness it removes.

### Retries

Only safe for idempotent queries, and the split falls exactly along the `ResponseAction` variants
(`shoal-core/src/shared/responses.rs:26-40`): `Get` and `Exists` are idempotent, `Insert`,
`Update`, and `Delete` are not. Recommend retrying reads by policy, never retrying writes unless
the caller explicitly opts in, and defaulting to `ReadsOnly`.

Retrying writes safely needs server-side deduplication — a per-query idempotency key the server
remembers long enough to recognise a repeat. That is a server feature, not a client one, and it
belongs in [TODOs](../appendix/todos.md) rather than being sketched here. Note what
[FoundationDB](prior-art.md#foundationdb) does instead: it retries the *transaction*, not the
request, which is why it can retry anything. Shoal has no transaction to retry, so it has to pick
its queries.

### `Drop` on both stream types

[Item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client)
already names the fix and the obstacle: returning the channel pair to `channel_queue` is an async
send, which `Drop` cannot await, so either the queue gains a synchronous path or `Drop` removes the
map entry and lets the pair go. Removing the entry is the half that matters.

What this page adds is that **`Drop` alone fixes the client and leaves the server wrong.** The
entry disappears, so the responses the server is still producing arrive at a proxy that cannot find
a channel for them — which today returns `Errors::ProtocolError` and kills the read task for that
connection (`client.rs:536-543`). `Cancel` is what makes the pair correct: `Drop` releases the
local state, `Cancel` tells the server to stop. Neither is complete without the other, and that is
a reason to do them together rather than to do the easy half now.

### Bounded channels

[Item 15](../appendix/known-issues.md#15-no-backpressure-anywhere). Bounding requires deciding what
happens when the bound is hit, and every answer — shed, block, reject — has to be expressible to
the client, ~~which is [D2](framing.md)'s error channel. Sequenced after it for that reason.~~
**That prerequisite is met**: [F11](../features/error-channel.md) landed the error channel and
reserved `ErrorCode::Shedding` for exactly this. What is left here is the bound and the policy.

### Instrumentation

Spans and `hotpath` scopes in `client.rs`, and ~~the `transport/*` workloads~~ — those are **built**
([F13](../features/transport-workloads.md)). This is step 0 of the whole chapter and it is listed
last here only because it is not a pool feature — it is the precondition for knowing whether
anything above cost anything. **Half of step 0 is now done**: a change to the pool can be measured
end to end at four transport modes and two row widths. The spans are what remain, and they are what
would say which part of a moved number was the pool rather than the wire.

## Recommendation

**Take the whole page, ~~after [D2](framing.md)~~, in the order the pieces are listed.** D2 has
landed in both halves ([F10](../features/framing-and-protocol-evolution.md),
[F11](../features/error-channel.md)), so every piece here is now a call site.

| | |
| --- | --- |
| **Rank** | **A3** — the largest stability return in the chapter for the least design risk |
| **Impact** | Argued for the performance of it; the value is correctness under failure, which no benchmark reports |
| **Difficulty** | L — contained to `client.rs` and the new builder, except for the pieces that need D2's message types |
| **Depends on** | ~~[D2](framing.md) for `Ping`, `Cancel`, `GoAway`~~ — **satisfied**, all three are defined and unwired since [F10](../features/framing-and-protocol-evolution.md), so each is a call site rather than a flag day; ~~still [D2](framing.md#the-error-channel) for the error channel~~ — also satisfied, by [F11](../features/error-channel.md). **Nothing on this page is blocked on the wire format any more** |
| **Blocks** | ~~[D3](authentication.md) and [D4](encryption.md) need the builder to put credentials and TLS into.~~ **Both shipped without it**, and the seam they left is `ClientOptions` + `Shoal::with_options` ([F14](../features/encryption-in-transit.md)) — a struct holding credentials and TLS and nothing else. This builder should **absorb** that rather than sit beside it: deadlines, pool sizing and health checks belong on the same object, and a second options type would be the third way to configure a client. [D7](shard-aware-routing.md) needs this pool before it can reshard it |
| **Tradeoff** | Contained — a deadline turns an indefinite wait into an error, which is a behaviour change callers must handle |
| **Benchmark** | ~~`transport/*`, unbuilt~~ — **built** ([F13](../features/transport-workloads.md)). **A deadline check on the hot path is the one piece here that could cost something measurable**, and `macro/transport/send_one/small` is where it would show: the narrow arm is the one where a fixed per-query cost is not buried under the bytes |

The builder, `Drop`, and the endpoint list can all be done today, without D2. Everything else waits
on a message type.

## What it costs

- **A deadline check per response**, which is the only per-query cost on this page and the only
  thing here a benchmark would see.
- **Bookkeeping for the endpoint list** — resolving several addresses and choosing among them,
  paid at connect time rather than per query.
- **A larger public API.** A builder with six sections is a bigger surface to keep compatible than
  a constructor with one argument.

## What it breaks

- **A query that used to hang now returns an error.** That is the point, and it is still a
  behaviour change: code written against the current client cannot observe a timeout and may
  treat any `Err` as fatal.
- **The pool stops being uniform once [D7](shard-aware-routing.md) lands.** `min_idle` and
  `max_size` are global numbers today; per-shard sub-pools make them per-shard, or make them global
  numbers that have to be divided. Building the builder with that in mind — a pool section that can
  later grow a per-shard variant — is what "putting the seam in place" means here.
- **`Drop` changes when the server sees a query end**, so a server-side change (`Cancel` handling)
  has to land with it rather than after it.

## Prerequisites

[D2](framing.md), for four of the eight pieces. Nothing else — and notably not
[D5](runtimes.md), though doing the split first means this work is written once in its final home.

## How it would be measured

Two different questions, and only one of them is a benchmark.

**Does it cost anything?** `transport/{send_one,send_batched,stream,stream_unordered}` — **built**
([F13](../features/transport-workloads.md)) — plus spans in `client.rs`
([O28](../appendix/optimizations.md#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends)),
which are not. The deadline check is the piece to watch, and the `small` arm of each mode is where
to watch it: a fixed per-query cost is visible at 256 bytes and vanishes at a MiB.

**Does it work?** Not a benchmark — a test, and the client has none for this. The streaming APIs
have [no test at all](../appendix/test-coverage.md#the-streaming-client-apis), which is why item 60
went unnoticed. What this page needs is a harness that can kill a connection underneath an in-flight
query and assert what the caller sees. That is closer to
[FoundationDB's simulation testing](prior-art.md#foundationdb) than to anything currently in the
repository, and it is the single most valuable test infrastructure this client could grow.

## Related

- [The Client](../api/client.md) — the pool and streams as they stand
- [D2. Framing and protocol evolution](framing.md) — `Ping`, `Cancel`, `GoAway`, and the error
  channel this page spends
- [D3](authentication.md), [D4](encryption.md) — what goes into the builder
- [D7. Shard-aware routing](shard-aware-routing.md) — what reshapes this pool later
- [item 15](../appendix/known-issues.md#15-no-backpressure-anywhere),
  [item 23](../appendix/known-issues.md#23-client-stream-and-pool-rough-edges),
  [item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client)
  — the open items this page closes
