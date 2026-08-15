# D3. Authentication

> **Half of this has been built**, as [F12](../features/authentication.md). The SCRAM-SHA-256 half
> landed with a mechanism-negotiation step in front of it; the mTLS half is still open and is still
> waiting on [D4](encryption.md). This page is kept as the design record, struck through where the
> build answered it — the [chapter overview](overview.md#the-d-number) says why a `D` page outlives
> the `F` page that supersedes it. **The build followed this page closely and got two things wrong
> about it**, both marked below: the ordering argument in *Recommendation*, and one of the three
> mitigations in *What it costs*.

## Context

Anything that can reach the port can read and write any table. That is stated in the
[Wire Protocol](../architecture/wire-protocol.md#limitations) page and it is exactly true — there
is no credential, no identity, and no seam where one would go.

The usual objection is that a datacenter store behind a firewall does not need authentication.
That objection is weaker than it looks for two reasons that have nothing to do with attackers.
**Identity is what makes an audit log meaningful**, and **identity is what makes multi-tenancy
possible at all** — a store with no notion of who is connected cannot later grow per-table
authorization, per-client quotas, or a way to answer "which service filled this table with
garbage". Those are the things authentication is a prerequisite for, and they are why this is worth
designing before it is needed rather than after.

## What exists today

> **As of [F12](../features/authentication.md) this section describes the past.** There is now a
> `shared::auth` module holding SCRAM-SHA-256 and a credential store, an `auth` section on the
> config, and a `Shoal::with_credentials` on the client. What follows is what was here when this
> page was written.

~~Nothing.~~ A search for authentication-related terms across `shoal-core`, `shoalctl`, and `shoal`
returned no hits outside the SHQL tokenizer and two comments using the phrase "token ring". The
only `tls` in the workspace is still `tonic`'s `tls-roots` feature, pulled in by the OTLP trace
exporter (`shoal-core/src/server/trace.rs`) and unrelated to client connections.

The network configuration had two fields:

```rust
pub struct Networking {
    /// The interface to bind to
    pub interface: String,
    /// The port to bind to
    pub port: u16,
}
```

`shoal-core/src/server/conf.rs:132-138`, `Networking`

~~There is also no seam.~~ Authentication is a per-connection fact established before the first
query, which means a handshake, which means a message type — and the protocol had neither
([D2](framing.md)). It has both since [F10](../features/framing-and-protocol-evolution.md), which
is what unblocked this.

## The options

| Option | Identity is | Needs | Fits |
| --- | --- | --- | --- |
| **Shared bearer token in `Hello`** | a string every client holds | D2 only | A single trusted operator, and nothing else. One credential for everyone means no audit trail and no revocation short of a restart |
| **SCRAM-SHA-256** | a username, proved without sending the password | D2, a credential store | Deployments with no PKI, and human operators at `shoalctl` |
| **mTLS client certificates** | the certificate's subject | [D4](encryption.md), a CA | Service-to-service inside a datacenter — the default case |
| **SPIFFE / SPIRE identities** | a workload identity document | D4, an identity plane | Deployments that already run one. Otherwise it is mTLS with more moving parts |
| **External token (OIDC / JWT)** | a claim signed by an issuer | D2, a JWKS fetch, clock sync | Humans and CI, layered on top of one of the above |

Two of these are worth expanding.

**SCRAM-SHA-256** is the boring correct answer for password authentication and is what Postgres,
MongoDB, and Cassandra all landed on. The server stores a salted, iterated derivation rather than
the password; the client proves knowledge without transmitting it; and the exchange authenticates
the *server* to the client as well, which a bearer token does not. It is a multi-round exchange, so
it needs `Auth` and `AuthResponse` to be repeatable message types rather than a single field in
`Hello` — a detail worth fixing in [D2](framing.md) rather than discovering later.

**mTLS** is different in kind from the rest: it is not a credential the application checks, it is a
property of the connection the TLS layer establishes. If [D4](encryption.md) is taken, the peer
certificate is already there and authentication is a matter of reading the subject and mapping it
to a principal. There is no secret to distribute, no credential store to keep, and rotation is
certificate rotation, which a datacenter already has machinery for.

## Recommendation

**mTLS as the primary identity; SCRAM-SHA-256 as the fallback for deployments with no PKI.**

| | |
| --- | --- |
| **Rank** | **B** — after D4, which decides most of it |
| **Impact** | Argued — this is a capability, not a cost |
| **Difficulty** | ~~L with mTLS (read a certificate subject), XL with SCRAM~~ — **SCRAM measured L, not XL.** The credential store is one map and a decoy derivation, the multi-round exchange is two state machines with one `step` method each, and the operator surface is a config section and a 60-line example. The estimate was made against building a credential *service*; what it needed was a credential *file* |
| **Depends on** | ~~[D2](framing.md) for the handshake~~ — **satisfied**, the handshake and the `Auth`/`AuthResponse` discriminants landed with [F10](../features/framing-and-protocol-evolution.md); [D4](encryption.md) if mTLS is the mechanism |
| **Blocks** | ~~per-table authorization, quotas, and any audit log worth keeping~~ — **unblocked** by [F12](../features/authentication.md), which produces a `Principal`. None of the three are built |
| **Tradeoff** | Contained — a connection either authenticates or is refused, and the failure is at connect time. **Held**: the whole of F12 is off unless a config asks for it |
| **Benchmark** | ~~`transport/*`, unbuilt.~~ Built ([F13](../features/transport-workloads.md)) — **and it does not help here**, which is the point this row was always making. The cost is per connection, not per query, so it is the one item in this chapter a query benchmark cannot see however many of them exist. **Still true after the build, and still true after F13** — see [O30](../appendix/optimizations.md) |

~~The ordering matters and is the reason this page is ranked behind [D4](encryption.md): the
encryption decision makes the authentication decision.~~

**This was the one thing this page got wrong, and it is worth naming.** The ordering argument
treats the two mechanisms as alternatives, so that choosing between them requires knowing whether
there is TLS. They are not alternatives — they are two mechanisms behind one negotiation step, and
a server can accept both. Once that is seen, the encryption decision does not decide anything about
SCRAM at all: it decides whether there is a *second* mechanism to prefer over it. The half that
needs no PKI was buildable the whole time, and D4 became an addition rather than a prerequisite.

What survives of the argument is smaller and still true: **SCRAM over a plaintext link is the
weaker deployment**, because an observer sees the username and the whole exchange, and a bearer
token there would be a password on the wire. That is an argument for doing D4, not for having done
it first.

Copy Cassandra's shape for the SCRAM path. Its native protocol answers a `STARTUP` with
`AUTHENTICATE` naming the mechanism, then exchanges `AUTH_RESPONSE` / `AUTH_CHALLENGE` until
`AUTH_SUCCESS` ([D9](prior-art.md#cassandra)). It is a well-worn SASL framing and there is nothing
to invent.

## What it costs

**Fifty handshakes, not one.** This is the point most likely to be missed, and it falls directly
out of the client's design. The pool opens up to 50 connections:

```rust
.min_idle(10)
.max_size(50)
```

`shoal-core/src/client.rs:141-142`

and authentication is per connection. A TLS handshake plus a multi-round SCRAM exchange on each of
ten eagerly-idle connections is a startup cost the client does not have today, and it lands
precisely when an application is starting and is least tolerant of latency. Three mitigations, in
order of value:

- ~~**TLS session resumption**, so only the first connection pays a full handshake.~~
  **Unavailable in the form encryption actually shipped.**
  [F14](../features/encryption-in-transit.md) took kTLS, and a TLS 1.3 server sends
  `NewSessionTicket` *after* the handshake — on a socket the kernel has taken over that is a non
  application record, and a plain `read` fails it with `EIO`. So `send_tls13_tickets = 0`, and the
  first and most valuable of these three mitigations is gone. Neither page saw this coming: it is
  the second time an interaction between these two features was missed, the first being the
  ordering argument struck through above.
- **A re-auth ticket** issued on first authentication and accepted in `Hello` on subsequent
  connections of the same client, collapsing SCRAM's rounds to one.
- **Lower `min_idle`**, which [D6](connection-pool.md) makes configurable anyway.

**None of the three were built**, and the second is worse than this page makes it sound. A ticket
in `Hello` is a bearer token with a shorter lifetime — it is replayable by anything that sees it,
which on a plaintext link is anything on the path, and it re-introduces exactly the property
[the options table](#the-options) rejects a bearer token for. It is worth having *behind TLS* and
is not worth having instead of it. All three are filed in [TODOs](../appendix/todos.md).

There is a compensating gain worth stating. The handshake belongs inside
`ShoalConnectionManager::connect` (`shoal-core/src/client.rs:63-74`), which is where bb8 already
re-establishes a connection after one dies. **A reconnect re-authenticates for free**, with no
code anywhere else, because the pool already treats connection creation as the place where a
connection becomes usable. **This held exactly**: the credentials sit on the manager and no
reconnect path was touched.

## What it breaks

- **A server with authentication enabled refuses every existing client**, which is correct and is
  the point, but means the setting has to be configurable per listener and default to off until a
  deployment opts in. The same requirement [D4](encryption.md) has, for the same reason: the
  benchmark harness and the integration tests must be able to keep connecting without credentials
  or the [frozen baseline](../operations/performance-baseline.md) becomes incomparable. **Built as
  described**, though the granularity is per server rather than per listener, because there is one
  listener.
- **`shoalctl` grows a credential surface** — somewhere to type a password or point at a
  certificate, and somewhere to store it. Today it takes an address and nothing else. **This did
  not happen, because `shoalctl`'s own binary is a placeholder**: it requires a database type at
  compile time, so the surface belongs to whatever binary a deployment writes around
  `shoalctl::run`, and that binary now calls `Shoal::with_credentials`. What was actually needed
  was somewhere to *generate* a credential, which is `shoal/examples/scram_credential.rs`.
- **`Shoal::new` grows parameters**, which is [D6](connection-pool.md)'s builder. Adding
  credentials to the current three-argument constructor is what makes the builder overdue rather
  than optional. **Deferred rather than done**: `with_credentials` is a second constructor, so
  fifteen call sites did not have to change, and D6's builder is still the place this argument
  belongs. A third constructor would be the signal that it is overdue.

## Prerequisites

~~[D2](framing.md), for `Hello`, `Auth`, and `AuthResponse`.~~ Satisfied by
[F10](../features/framing-and-protocol-evolution.md).

**D4 has since been built** ([F14](../features/encryption-in-transit.md)), so the mTLS half is
unblocked and still unbuilt — `AuthMechanism::MutualTls` is still defined and still refused, and
turning it on is now genuinely the new arm in two matches this page predicted.

~~[D4](encryption.md), if the recommendation is taken as written~~ — and note this is the chapter's
only *soft* edge: SCRAM over plaintext is a coherent deployment and was designed for exactly that,
so D3 can ship without D4 if the mTLS half is deferred. **That is what happened.** D4 is still what
the mTLS half waits on, and is now the only thing left on this page.

## Out of scope, deliberately

**Authorization.** Who may read which table is a server-side catalog problem — it needs a place to
store grants, a check on the query path, and a way to express them in SHQL — and none of that is
client design. It is the thing authentication exists to enable, and it is filed in
[TODOs](../appendix/todos.md) rather than sketched here, because a design for it written before
there is any notion of a principal would be a design for nothing.

**Encryption at rest.** A storage question, unrelated to this part.

## How it would be measured

Connection establishment cost, which no existing benchmark reports. The macro layer measures
queries against an already-warm pool, so a handshake that costs milliseconds would be invisible to
every number in [Benchmark Results](../operations/benchmark-results.md). If this is built, the
thing to add is not a query workload but a *connect* workload — time to first successful query from
a cold client — and it is the one measurement in this chapter that the ~~planned~~ `transport/*`
workloads would still not provide. **They are now built and this prediction held**:
[F13](../features/transport-workloads.md) opens its pool before it samples anything, exactly as
every other macro workload does, so a handshake cost is still outside every number it reports.

**It was built and this is still unmeasured.** The connect workload does not exist, so the cost
[F12](../features/authentication.md#performance) added is bounded by arithmetic and not by a
capture. It is filed as [O30](../appendix/optimizations.md), which is the honest place for it:
this chapter inherits the rule that nothing is acted on until a benchmark would show the
difference, and that rule does not stop applying once something ships.

## Related

- [F12. Authentication](../features/authentication.md) — the half of this that was built
- [D2. Framing and protocol evolution](framing.md) — the handshake this needs
- [D4. Encryption in transit](encryption.md) — which decides whether this is mTLS or SCRAM
- [D6. A production connection pool](connection-pool.md) — the builder credentials go into, and the
  `min_idle` that decides how many handshakes happen at once
- [D9. Lessons from other databases](prior-art.md#cassandra) — Cassandra's SASL flow, and
  Postgres and MongoDB on SCRAM
- [Wire Protocol](../architecture/wire-protocol.md#limitations) — where the current absence is
  recorded
