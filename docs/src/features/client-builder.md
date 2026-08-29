# F16. The client builder

## Context

[D6](../direction/connection-pool.md#a-builder) predicted the moment this became overdue, and named
it precisely:

> Credentials ([D3](../direction/authentication.md)) turned out to be the exception rather than the
> example: rather than wait for the builder, [F12](authentication.md) added a second constructor,
> `Shoal::with_credentials`. That is fine for one option and is exactly the pressure this page
> describes — a **third** constructor is the point at which it stops being fine.

[F14](encryption-in-transit.md) added the third. It did not add a third *constructor* — it added
`ClientOptions` and `Shoal::with_options`, a struct holding credentials and TLS and a constructor
taking it — and that was the right call for two settings. But `ClientOptions`' own doc comment
said what it was:

> It is the seam that builder should absorb rather than sit beside — deadlines, pool sizing and
> health checks all belong on the same object and none of them are here.

`shoal-client/src/client.rs`, before this change

Everything else about a client was a literal. Ten idle connections, fifty at most, five seconds to
check one out, three hundred to reap an idle one, eighteen hundred to retire a live one, and ten
for a handshake — six numbers, none of them reachable from anywhere. And a client knew one server:
`Shoal::connect` called `lookup_host` and then `.next()`, so a name with three records behind it
produced a client that had heard of one of them and had nowhere to go when it stopped answering.

## What it does

**A client can be described instead of named.**

```rust
let client = Shoal::<TmdbClient>::builder()
    .endpoints(["10.0.0.1:12000", "10.0.0.2:12000"])
    .pool(PoolConfig { max_size: 200, ..PoolConfig::default() })
    .deadlines(Deadlines { handshake: Duration::from_secs(2) })
    .credentials(Credentials::scram("reader", "hunter2"))
    .tls(TlsClientOptions::new("/etc/shoal/ca.pem"))
    .build()
    .await?;
```

| | Before | Now |
| --- | --- | --- |
| Pool sizing and ageing | five literals in `Shoal::connect` | `PoolConfig`, defaulted to those five |
| Handshake deadline | a `const HANDSHAKE_TIMEOUT` | `Deadlines::handshake` |
| Servers | the first address the first name resolved to | every address every endpoint resolves to |
| A server being down | the client is down | the next endpoint is tried, in the same attempt |
| Credentials and TLS | `ClientOptions` + `with_options` | the same, and also builder sections |
| Attribution | none — no span, no scope | spans on six call paths, and `hotpath` scopes |

**A client tries every endpoint before it says it could not connect.** `ManageConnection::connect`
takes a turn from a shared counter, then walks the whole list from there. The counter alone would
spread connections across endpoints and would eventually find a live one across enough of `bb8`'s
retries — but those retries are bounded by `PoolConfig::connection_timeout`, so a client whose
first four endpoints are down would spend that entire budget backing off rather than reaching the
fifth, which was answering the whole time.

**The client is instrumented, which is step 0 of the whole
[Direction](../direction/overview.md#the-recommended-order) chapter.** `Shoal::send`,
`ShoalQueryStream::send` and `ShoalConnectionManager::connect_to` carry `#[instrument]`; those and
`track_response`, `TcpProxy::read_frame` and both `next()` implementations carry `hotpath` scopes.
`shoal-bench` was already ready for them —
`shoal-bench/src/render/chart/hotpath_scopes.rs` has listed `shoal_client::` as a known prefix, and
tested that it shortens correctly, since [F15](client-server-split.md). Nothing in the reporting
half changed.

## Design choices

**The defaults are the literals, exactly.** `PoolConfig::default()` is 10 / 50 / 5 s / 300 s /
1800 s and `Deadlines::default()` is 10 s, because those are the numbers every client was already
built with. `the_pool_defaults_are_the_numbers_that_were_hardcoded` asserts all six. This is the
whole safety argument for the change: making something configurable must not quietly reconfigure
every caller that never asked for anything.

**`Deadlines` holds one field, and is a struct anyway.** A single `Duration` argument would have
done for today. It is a struct because [F17 is going to add `request` and `idle`](../direction/connection-pool.md#deadlines),
and those change what a caller must *handle* rather than only what it may configure — a query that
used to hang starts returning an error. Growing a struct additively is a change nobody has to read;
turning an argument into a struct is a change every call site has to.

**`Deadlines` deliberately does not hold the pool's `connection_timeout`.** That is a `bb8` setting
bounding a checkout and its retry loop, it lives on `PoolConfig` beside the other four, and it is
named after `bb8` rather than after this crate. Putting both on one struct and calling them
`connect` and `connection_timeout` is how a reader ends up believing one bounds the other. It does
not: the handshake deadline exists precisely *because* `bb8`'s timeout does not reach inside
`connect`.

**`ClientOptions` is absorbed, not replaced.** It stays public, stays constructible, and
`with_options` still takes it. `ShoalBuilder::options` takes a whole one, so a caller already
holding one moves to the builder without taking its two fields apart. There are four live call
sites outside this crate — `shoal/tests/tls.rs`, and two in `shoal-bench` — and a builder whose
first act is to break them would have been a worse trade than a type that has two ways in.

**An endpoint resolves to every address it names, not the first.** This is the part that is a fix
rather than a feature. `lookup_host(...).next()` silently discarded records, so DNS-based failover
— the ordinary way a datacenter names a service — never worked, and would not have worked even
after an endpoint *list* was added, because each entry in the list would have been narrowed the
same way.

**Duplicates are dropped and order is kept.** The round robin steps through the resolved list, so
two names resolving to one address would send two out of every three connections to the same
server. `an_address_given_twice_is_only_kept_once` pins it.

**Nothing is handed to the proxy until every handshake has succeeded.** This was already true and
is now load-bearing in a way it was not: a failed attempt has to leave no read half registered
anywhere, or trying the next endpoint would be a way to accumulate half-open connections. It is
written down in `connect_to`'s invariants because the failover loop is what makes breaking it
expensive.

**`hotpath` no longer implies `server`.** The facade's feature was `hotpath = ["server",
"shoal-core/hotpath"]`. It is now `["shoal-client/hotpath", "shoal-core?/hotpath"]`. The client is
the layer this exists to attribute, and requiring an engine in the graph to profile it would mean
the one build where the client is the whole of the code —
[the client-only build F15 made possible](client-server-split.md) — is the one build that cannot be
measured.

## Alternatives rejected

**Delete `ClientOptions` and make the builder the only way.** Cleaner as a surface, and it is what
"absorb" could have meant. It breaks four call sites for no behaviour, and it makes this change a
migration rather than an addition. The cost of keeping it is one type that is also reachable
another way; the cost of removing it is that every caller has to move on this feature's schedule.

**A typestate builder that will not compile without an endpoint.** `ShoalBuilder<NoEndpoint>` →
`ShoalBuilder<HasEndpoint>` turns `a_client_with_no_endpoint_is_refused` from a test into a compile
error, which is strictly stronger. It also puts a type parameter on every signature that names a
builder, in a crate whose constructors already carry a ten-line `where` clause each. The error this
buys is one a caller hits once, on the first run, with a message that says exactly what is missing.

**Change the three constructors to take a list.** `Shoal::new(["a", "b"])` would give failover to
callers who never learn the builder exists. It also changes the signature of the most-used function
in the crate, and `A: ToSocketAddrs` is what makes `Shoal::new(&addr)` work for every shape of
address the standard library accepts. The constructors keep taking one address and now expand it to
every record behind it, which is the part of the win that can be had without a break.

**Leave failover to `bb8`'s retry loop and keep only the counter.** Half a page shorter, and it
does work — a fact established by writing the integration test first and watching it pass against a
build with the loop stubbed out. That is why the property is pinned by unit tests over
`endpoint_order` rather than only by the integration test: the end-to-end behaviour does not
distinguish the two designs, and the connection timeout budget is what does.

**Install a `tracing` subscriber so the spans go somewhere.** Out of scope, and it would have made
the performance question below unanswerable by changing two things at once. See *Limitations*.

## Limitations

**A multi-endpoint TLS client must name its server.** `tls::connect` falls back to the address it
dialled as the SNI name, so several endpoints with no explicit `TlsClientOptions::server_name` means
each server is asked for a different name, and every one whose certificate does not carry its own IP
fails its check. `build()` logs a `WARN` when it sees that combination. It is a warning rather than
an error because one certificate per host is a legitimate deployment.

**Every endpoint shares one frame bound.** `peer_max_frame_bytes` is a single `AtomicU32` learned
from whichever connection most recently shook hands, which was already
[F10's limitation](framing-and-protocol-evolution.md) and gets sharper with an endpoint list: two
servers configured with different `max_frame_bytes` now sit behind one client, and it will use
whichever it heard last for both.

**Endpoints are tried in order, not by health.** A dead endpoint is tried and refused on every
connection whose turn starts at it, rather than being marked down and skipped. Refusal on loopback
is immediate and on a partitioned network it is a full TCP timeout, so the cost is real and is not
paid until something is actually broken. Health-based preference wants the `Ping` from
[F18](../direction/connection-pool.md#health-checks-that-work) and is filed in
[TODOs](../appendix/todos.md).

**No query deadline yet.** `Deadlines` has one field, and it bounds opening a connection. A server
that accepts a query and never answers still parks its caller forever
([item 62](../appendix/known-issues.md)). That is F17.

**The spans reach nobody.** `shoal-core/src/server/trace.rs` builds a subscriber and ~~**nothing in
the workspace calls it** — not `ShoalPool::start`, not the workload binary, not the tests. So the
`tracing` section of `shoal.yml` configures nothing~~ — **the workload binary calls it now**, so
the `tracing` section of `shoal.yml` configures a capture and both halves' spans can be switched on
([F34](../features/benchmark-tracing.md)). `ShoalPool::start` and the tests still do not. **The
measurement below is unchanged by that** and says exactly what it always said: what these spans cost
when no subscriber is installed, which was the only configuration that existed when it was taken.
The remainder is [item 69](../appendix/known-issues.md).

## Invariants to uphold

**`PoolConfig::default()` and `Deadlines::default()` must stay equal to the values they replaced.**
Not "sensible", not "tuned" — equal. The moment they drift, every caller that never configured
anything is running a client somebody else reconfigured, and no benchmark in this repository
attributes a change to the pool. `the_pool_defaults_are_the_numbers_that_were_hardcoded` is the
test.

**`connect_to` hands nothing to the proxy until all three handshakes have succeeded.** The TLS
handshake, the Shoal handshake and the authentication exchange all complete before `into_split`,
and the read half goes to the proxy after that. Moving the split earlier would both feed the
handshake's own ack into the proxy as a response to a query nobody sent (F10's invariant) and leave
a registered read half behind on every endpoint the failover loop tries past.

**One attempt tries every endpoint exactly once.** Not "eventually reaches a live one" — that is
what the counter does on its own, and it is not enough, because `bb8`'s retries are bounded by
`connection_timeout`. `endpoint_order` is a free function so this can be asserted directly rather
than inferred from an end-to-end run that passes either way.

**The endpoint turn is taken modulo, and the counter is never reset.** `next_endpoint` is a
`fetch_add` shared by every clone of the manager, so it runs past the endpoint count immediately
and eventually wraps. `a_turn_past_the_endpoint_count_still_names_an_endpoint` covers both.

**`hotpath` on the facade must not imply `server`.** A client-only build is the one that most needs
attributing, and the feature that measures it must not drag an engine into the graph to do it.

**The two `read_exact` calls in `TcpProxy::read_frame` stay two calls.** Untouched here, and the
`hotpath` scope now wrapping that function must not become a reason to restructure it: merging them
would land the payload at offset 24 and end the zero-copy read silently
([F10](framing-and-protocol-evolution.md)).

## Performance

**Measured, and there is nothing to report — which is the result.**
`d6-f16-before` and `d6-f16-after` are both in `docs/perf/runs/`, five runs each over all sixteen
`transport` workloads, taken either side of this change on the same machine at the same governor.

**Not one of the 144 metrics is a result.** Every interval overlaps its baseline. The arm
[D6 named as the place a fixed per-query cost would show](../direction/connection-pool.md#benchmark)
— `macro/transport/send_one/small`, 50,000 rows at 256 bytes, where a per-query cost is not buried
under the bytes — moved −4.98% on wall clock and −4.94% on mean get latency, both inside the noise
and both in the *faster* direction, which is what noise looks like.

| `macro/transport/send_one/small` | before | after | |
| --- | --- | --- | --- |
| wall clock | 202.92 [174.31 - 213.38] ms | 192.82 [186.24 - 195.06] ms | intervals overlap |
| get p50 | 60.76 [52.90 - 64.99] µs | 58.00 [56.44 - 59.09] µs | intervals overlap |
| get p99 | 131.54 [107.58 - 135.18] µs | 121.31 [118.62 - 126.21] µs | intervals overlap |

**What that does and does not establish.** `hotpath::measure` compiles to nothing without the
feature, so a default build contains none of it and the scopes are genuinely free here. The
`tracing` spans are in the build — and **no subscriber is installed**, so each one is a check
against a global dispatcher that finds nothing and returns. That is the honest scope of this
measurement: it says the spans cost nothing in the configuration this repository actually runs, and
it says nothing about what they would cost under a subscriber. Since `trace::setup` had no callers
at all when this was measured, that configuration was also the only one that existed. The example
calls it now, and so does `shoal-workload` since [F34](../features/benchmark-tracing.md), which
makes the other configuration reachable and measurable — and leaves this measurement saying exactly
what it always said, about the one without a subscriber. **Nobody has taken the other measurement.**

**What it unblocks is worth more than what it cost.**
[O28](../appendix/optimizations.md) and
[O30](../appendix/optimizations.md) both carry a Benchmark row reading *none, and none can exist
yet — `client.rs` has no `tracing` spans and no `hotpath` scopes at all*. That sentence is now
false, and both entries are adjudicable for the first time. **O28's own fix was deliberately not
taken here**: it is one guard instead of two, and taking it in this change would have put the
instrumentation's cost and the fix's benefit into the same capture with no way to separate them.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `the_pool_defaults_are_the_numbers_that_were_hardcoded` | Making the pool configurable silently reconfigures every caller that never asked for anything |
| `a_pool_that_cannot_be_satisfied_is_refused`, `a_pool_of_no_connections_is_refused` | A pool `bb8` can never satisfy is built anyway, and fails somewhere further in |
| `the_default_pool_is_valid` | The validation rejects the configuration every existing caller uses |
| `a_client_with_no_endpoint_is_refused` (unit) | A builder with nowhere to go asks a resolver about nothing and reports a DNS failure |
| `endpoints_are_resolved_in_the_order_they_were_given` | The endpoint a caller wrote first stops being the one a healthy client mostly talks to |
| `an_address_given_twice_is_only_kept_once` | Two names for one server send two out of every three connections to it |
| `one_attempt_tries_every_endpoint_exactly_once` | Failover falls back to `bb8`'s retry budget, and a client with four dead endpoints spends its connection timeout on backoff |
| `each_connection_starts_where_its_turn_says` | A pool of ten opens ten connections to one endpoint |
| `a_turn_past_the_endpoint_count_still_names_an_endpoint` | The never-reset counter indexes past the endpoint list |
| `a_single_endpoint_is_tried_once`, `no_endpoints_yields_nothing` | The order arithmetic loops or divides by zero at the edges |
| `a_client_the_builder_built_answers_a_query` | The route every constructor now takes through the builder loses something on the way |
| `an_endpoint_that_is_down_is_tried_past` | A client with a live endpoint behind a dead one cannot reach it |
| `a_client_with_no_live_endpoint_fails` | Trying past a dead endpoint becomes trying past a dead client |
| `a_pool_sized_by_the_caller_still_answers` | The pool numbers are taken and dropped rather than reaching `bb8` |
| `a_pool_that_cannot_be_satisfied_never_opens_a_socket` (integration) | A configuration error is reported as a connection failure |
| `cargo check -p shoal-client-check --no-default-features` | The builder drags a server path into the client half of `#[shoal::db]` |
| `cargo check -p shoal --no-default-features --features hotpath` | Profiling the client starts requiring an engine in the graph |

## Related

- [D6. A production connection pool](../direction/connection-pool.md) — where this was designed,
  and the three things that page says which the code had already overtaken
- [F14. Encryption in transit](encryption-in-transit.md) — the `ClientOptions` seam this absorbs
- [F12. Authentication](authentication.md) — the second constructor that predicted the third
- [F15. The client is a crate that cannot start a database](client-server-split.md) — the
  client-only build that `hotpath` must not exclude
- [The Client](../api/client.md) — the pool and the streams as they stand
- [O28](../appendix/optimizations.md), [O30](../appendix/optimizations.md) — the two entries this
  makes adjudicable, and the one it deliberately did not take
