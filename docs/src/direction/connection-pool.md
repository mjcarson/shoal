# D6. A production connection pool

> **The builder half landed as [F16](../features/client-builder.md)**: the builder itself, the
> endpoint list, the pool and deadline configuration, and the instrumentation this page lists last.
> **Three things on this page were already false when it was written down**, because
> [F11](../features/error-channel.md) and [F15](../features/client-server-split.md) shipped after
> it, and each is marked inline below — *Connection death* is **built**, and built the precise way
> this page argues against; `Drop` does **not** need `Cancel`; and the release arm is no longer
> `if end`. Every `shoal-core/src/client.rs` citation on this page is stale by a crate: the client
> is `shoal-client/src/client.rs` since F15.
>
> What is left is [F17](../features/client-builder.md) — `Drop`, deadlines and retries — and F18,
> the `Ping`/`Pong` health check and the `GoAway` drain. `Cancel` has been **dropped from scope**,
> for reasons given under *Deadlines* and *Drop on both stream types* below.

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

`shoal-client/src/client.rs`, `Shoal::connect` — **now `PoolConfig`** ([F16](../features/client-builder.md))

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

`shoal-client/src/client.rs`, `ShoalConnectionManager::is_valid` — the snippet above is **out of date**: [F11](../features/error-channel.md) added a `dead_conns` check ahead of the `peer_addr` call, which refuses a connection whose read half has stopped. The `TODO` survives, and is F18

`peer_addr()` reads local socket state. A server that has gone away without closing the socket is
not detected ([item 23](../appendix/known-issues.md#23-client-stream-and-pool-rough-edges)).

**One endpoint.** `Shoal::new` resolves the address and takes the first result, so there is no
failover, no endpoint list, and no round-robin across nodes. **Fixed by
[F16](../features/client-builder.md)**, and note what "takes the first result" was hiding: the
`.next()` was on `lookup_host`'s iterator, so a *name* was narrowed to one record too.

**Nothing is configurable.** Every number above is a literal. There is no builder and no config
struct — a caller who wants 200 connections, or a 50 ms deadline, has nowhere to put them.
Credentials ([D3](authentication.md)) turned out to be the exception rather than the example: rather
than wait for the builder, [F12](../features/authentication.md) added a second constructor,
`Shoal::with_credentials`. That is fine for one option and is exactly the pressure this page
describes — a **third** constructor is the point at which it stops being fine.

~~**A dead socket strands its in-flight queries.** bb8 reconnects transparently on the next call, and
the new read half is pushed to the proxy. But a query whose response was in flight on the dead
socket never completes: its `channel_map` entry is never removed and its caller's `next()` waits
forever.~~ **Already fixed when this was written**, by
[F11](../features/error-channel.md): `Waiter { conn }` records which connection a bundle went out
on, `dead_conns` records which read loops have stopped, and `TcpProxy::fail_waiting` fails exactly
the queries the dead socket owed. See *Connection death* below, which recommends against the option
that shipped.

**An abandoned stream leaks.** ~~The release runs only in `next`'s `if end` arm
(`client.rs:1032-1039`, and `:1231-1238` for the unordered stream)~~ — it runs on
`matches!(outcome, Err(_) | Ok((true, _)))` since [F11](../features/error-channel.md), so a stream
that *failed* no longer leaks. What still does is a stream the caller stops polling, and neither
stream type implements `Drop`
([item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client)).

**Nothing is bounded and nothing is measured.** Response channels are `kanal::unbounded_async`
([item 15](../appendix/known-issues.md#15-no-backpressure-anywhere)), ~~and `client.rs` has no
`tracing` spans and no `hotpath` scopes at all~~ — it has both since
[F16](../features/client-builder.md), which is what makes
[O28](../appendix/optimizations.md#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends)
and O30 adjudicable. The channels are still unbounded, and see *Bounded channels* below for why
that is no longer filed here.

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

> **Built as [F16](../features/client-builder.md)**, close to as sketched. Two departures worth
> naming. `Deadlines` holds `handshake` and not `connect`, because `PoolConfig` already carries
> `bb8`'s `connection_timeout` and calling both of them "connect" is how a reader ends up believing
> one bounds the other — it does not, which is the entire reason the handshake deadline exists.
> And the endpoint list turned out to have a **fix** hiding inside it that this page does not
> mention: `Shoal::connect` called `lookup_host(..).next()`, so DNS-based failover could not have
> worked *even with* an endpoint list, because every entry in the list would have been narrowed to
> one record the same way.

### Health checks that work

The [D2](framing.md) `Ping` / `Pong`, used by `is_valid` and by an idle keepalive.
[TODOs](../appendix/todos.md#client-and-ui) records the `client.rs:82` TODO as needing "a
message-type field the wire format does not have" — D2 is that field, and this is the entry it
closes.

### Deadlines

A per-query deadline around the `recv` in `ShoalResultStream::next`, and a read-idle deadline on
the proxy's socket loop. The first is a few lines; ~~**the second half of it is not.** A client that
gives up on a query has to tell the server, or the server keeps working on it and eventually writes
a response into a channel with no reader — the same failure as item 60, arrived at from the other
direction. That is [D2](framing.md)'s `Cancel`. A deadline without `Cancel` converts a slow query
into a leak.~~

**A deadline without `Cancel` does not convert a slow query into a leak**, because the leak it
names was fixed from the other end: the response arrives at a proxy that logs it and continues, and
`Drop` removes the map entry synchronously. See *Drop on both stream types* below. `Cancel` is out
of scope; the deadlines are F17.

The open question the deadlines carry is not `Cancel` but the **default**. `None` breaks nothing on
landing and gives nobody the stability this page is for; a number changes the behaviour of every
existing caller, including the benchmarks. F17 ships them off and files the choice, because picking
it wants the transport workloads run with deadlines on first.

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

~~**Recommend `GoAway` plus over-broad failure.** A clean server shutdown or drain is the frequent
case and `GoAway` makes it correct; an abrupt socket death is rare, and failing extra queries with
a retryable error on a rare event is a much better trade than paying per-bundle bookkeeping on
every query forever. The precise option is the kind of correctness that costs more than the
incorrectness it removes.~~

**This section describes work that was already done, and recommends against the way it was done.**
[F11](../features/error-channel.md) took the first row of that table — the precise one — and the
argument for it is in that page's *Design choices*: the map is shared by the whole pool, so a
blanket sweep would fail up to forty-nine other connections' healthy queries **and would fire on
every ordinary `bb8` idle reap**. That last clause is what this page missed. "An abrupt socket
death is rare" is true of a *server* dying and false of a socket closing, because the pool closes
its own sockets on `idle_timeout` and `max_lifetime` as a matter of routine. Over-broad failure
would not have been a rare over-reaction; it would have been a scheduled one every five minutes.

The bookkeeping it costs is one `Option<u64>` per in-flight bundle, in a map the client was already
keeping. `GoAway` is still worth having, for the reason given — it makes a clean drain correct
rather than merely survivable — and it is F18.

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

~~What this page adds is that **`Drop` alone fixes the client and leaves the server wrong.** The
entry disappears, so the responses the server is still producing arrive at a proxy that cannot find
a channel for them — which today returns `Errors::ProtocolError` and kills the read task for that
connection (`client.rs:536-543`). `Cancel` is what makes the pair correct: `Drop` releases the
local state, `Cancel` tells the server to stop. Neither is complete without the other, and that is
a reason to do them together rather than to do the easy half now.~~

**That was true and is not.** [F11](../features/error-channel.md) made an orphaned response a
`WARN` and a `continue` — pinned by
`an_error_frame_for_an_unknown_query_does_not_end_the_read_loop` — precisely because killing a
connection over one caller's leak takes every other query on it down too. So `Drop` on its own is
now complete on the client and harmless on the server, and **`Cancel` has been dropped from D6's
scope**. What is left without it is wasted server work and response bytes written to a socket whose
reader discards them: a performance claim, and
[Optimizations](../appendix/optimizations.md#how-these-are-ranked)' own rule forbids acting on one
before a benchmark exists that would show it. No workload abandons a stream.

**The sharper reason is that the cheap `Cancel` does not buy what the expensive one is for.** The
case that motivates cancellation is a timeout storm — a short deadline against a slow server, at
concurrency, with the server grinding on work nobody will read. Honouring `Cancel` at the
connection relay stops the *write* and not the work, so it does nothing about that. Stopping the
work means a `ServerMsg::Cancel` broadcast to every shard plus an expiring cancelled-set consulted
on the query path, which is a lookup on the arm `macro/transport/send_one/small` measures. And the
wire query id is a **bundle** id (`shoal-proto/src/shared/queries.rs`), so `Cancel` cancels a whole
bundle and is a near-synonym for `ShoalQueryStream::close` on the streaming path. Both depths are
costed in [TODOs](../appendix/todos.md).

### Bounded channels

[Item 15](../appendix/known-issues.md#15-no-backpressure-anywhere). Bounding requires deciding what
happens when the bound is hit, and every answer — shed, block, reject — has to be expressible to
the client, ~~which is [D2](framing.md)'s error channel. Sequenced after it for that reason.~~
**That prerequisite is met**: [F11](../features/error-channel.md) landed the error channel and
reserved `ErrorCode::Shedding` for exactly this. What is left here is the bound and the policy.

**Dropped from D6's scope, and it should not have been on this page.** Item 15 names five unbounded
channels — the shard mesh, the per-client response channel, compaction jobs, and two loaders — and
exactly one of them belongs to the pool. Bounding the client's half fixes none of the failure the
item describes, which is a *shard* falling behind and growing its queue until the process is
killed. This is a server backpressure feature wearing a pool feature's clothes. Item 15 stays open
and `ErrorCode::Shedding` stays reserved.

### Instrumentation

~~Spans and `hotpath` scopes in `client.rs`, and~~ the `transport/*` workloads — both are **built**
([F13](../features/transport-workloads.md), and the spans as part of
[F16](../features/client-builder.md)). This is step 0 of the whole chapter and it was listed last
here only because it is not a pool feature — it is the precondition for knowing whether anything
above cost anything. **Step 0 is done.**

**What it cost was nothing measurable, and what that means is narrower than it sounds.** 144
metrics over sixteen transport workloads, before and after, and not one disjoint pair —
`macro/transport/send_one/small`, the arm this page names below, moved −4.98% and overlapped.
`hotpath::measure` compiles away without its feature, so a default build carries none of it. The
spans *are* in the build, and cost nothing **in the configuration this was measured under, which
installs no subscriber** — `trace::setup` had no callers anywhere in the workspace when this figure
was taken. ~~The example calls it now, so a subscriber can be switched on, but `shoal-workload`
still installs none~~ — `shoal-workload` installs one now, and a capture honors its `tracing:`
section ([F34](../features/benchmark-tracing.md)). **Every number on this page was still measured
without one**, so the measurement remains a statement about spans nobody was listening to; what has
changed is that the other configuration now exists and can be measured, not that this figure covers
it ([item 69](../appendix/known-issues.md)).

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
