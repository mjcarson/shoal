# D3. Authentication

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

Nothing. A search for authentication-related terms across `shoal-core`, `shoalctl`, and `shoal`
returns no hits outside the SHQL tokenizer and two comments using the phrase "token ring". The only
`tls` in the workspace is `tonic`'s `tls-roots` feature, pulled in by the OTLP trace exporter
(`shoal-core/src/server/trace.rs`) and unrelated to client connections.

The network configuration has two fields:

```rust
pub struct Networking {
    /// The interface to bind to
    pub interface: String,
    /// The port to bind to
    pub port: u16,
}
```

`shoal-core/src/server/conf.rs:132-138`, `Networking`

There is also no seam. Authentication is a per-connection fact established before the first query,
which means a handshake, which means a message type — and the protocol has neither
([D2](framing.md)).

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
| **Difficulty** | L with mTLS (read a certificate subject), XL with SCRAM (a credential store, a multi-round exchange, and an operator surface) |
| **Depends on** | [D2](framing.md) for the handshake; [D4](encryption.md) if mTLS is the mechanism |
| **Blocks** | per-table authorization, quotas, and any audit log worth keeping |
| **Tradeoff** | Contained — a connection either authenticates or is refused, and the failure is at connect time |
| **Benchmark** | `transport/*`, unbuilt. The cost is per connection, not per query, so this is the one item here a query benchmark would not see |

The ordering matters and is the reason this page is ranked behind [D4](encryption.md): **the
encryption decision makes the authentication decision.** Take TLS and mTLS is nearly free. Decline
TLS and SCRAM becomes mandatory rather than a fallback, because a bearer token on a plaintext link
is a password on the wire.

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

- **TLS session resumption**, so only the first connection pays a full handshake.
- **A re-auth ticket** issued on first authentication and accepted in `Hello` on subsequent
  connections of the same client, collapsing SCRAM's rounds to one.
- **Lower `min_idle`**, which [D6](connection-pool.md) makes configurable anyway.

There is a compensating gain worth stating. The handshake belongs inside
`ShoalConnectionManager::connect` (`shoal-core/src/client.rs:63-74`), which is where bb8 already
re-establishes a connection after one dies. **A reconnect re-authenticates for free**, with no
code anywhere else, because the pool already treats connection creation as the place where a
connection becomes usable.

## What it breaks

- **A server with authentication enabled refuses every existing client**, which is correct and is
  the point, but means the setting has to be configurable per listener and default to off until a
  deployment opts in. The same requirement [D4](encryption.md) has, for the same reason: the
  benchmark harness and the integration tests must be able to keep connecting without credentials
  or the [frozen baseline](../operations/performance-baseline.md) becomes incomparable.
- **`shoalctl` grows a credential surface** — somewhere to type a password or point at a
  certificate, and somewhere to store it. Today it takes an address and nothing else.
- **`Shoal::new` grows parameters**, which is [D6](connection-pool.md)'s builder. Adding
  credentials to the current three-argument constructor is what makes the builder overdue rather
  than optional.

## Prerequisites

[D2](framing.md), for `Hello`, `Auth`, and `AuthResponse`. [D4](encryption.md), if the
recommendation is taken as written — and note this is the chapter's only *soft* edge: SCRAM over
plaintext is a coherent deployment and was designed for exactly that, so D3 can ship without D4 if
the mTLS half is deferred.

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
a cold client — and it is the one measurement in this chapter that the planned `transport/*`
workloads would still not provide.

## Related

- [D2. Framing and protocol evolution](framing.md) — the handshake this needs
- [D4. Encryption in transit](encryption.md) — which decides whether this is mTLS or SCRAM
- [D6. A production connection pool](connection-pool.md) — the builder credentials go into, and the
  `min_idle` that decides how many handshakes happen at once
- [D9. Lessons from other databases](prior-art.md#cassandra) — Cassandra's SASL flow, and
  Postgres and MongoDB on SCRAM
- [Wire Protocol](../architecture/wire-protocol.md#limitations) — where the current absence is
  recorded
