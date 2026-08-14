# Direction

**Nothing in this part is built.** Every page describes work that could be done to the client, not
work that has been. A `D` page is the design that would be written down *before* a feature page, in
the same way [TODOs](../appendix/todos.md) records that something is unbuilt — the difference is
that this part says how it would be built, what it would cost, and what it would break.

Six things are wanted from the client eventually: authentication, encryption, portability across
async runtimes, a pool that survives a production failure, shard-aware routing over the tablet map,
and a stronger compile-time link between a query and its response. They are usually discussed as
six independent features. **They are not.** Five of the six land on a wire protocol that has no
message-type field, no version, and no error channel
([Wire Protocol](../architecture/wire-protocol.md#design-notes)), so the first question each of
them asks is the same question, and answering it once is most of the work.

That is why this is a chapter rather than six entries in `todos.md`.

## The constraint every page inherits

Shoal is a datacenter store. The link between a client and a shard is fast, reliable, and inside a
trust boundary that is administratively defined rather than physically real. Two consequences run
through every page here:

- **Loss recovery and path mobility are worth little**, which is most of what modern transports
  sell. [D1](transport.md) is where that is argued in full.
- **A microsecond is a large number.** The response path is zero-copy — the socket is read directly
  into an `AlignedVec<16>` and reading a row is a pointer cast
  (`shoal-core/src/client.rs:524-527`, `TcpProxy::start`). Encryption, a transport with mandatory
  AEAD, and a routing layer that reshapes the pool each threaten that property in a different way,
  and each page has to say whether its recommendation preserves it.

The second one is the sharper constraint, and the honest reason this chapter exists on a branch
named `ZeroCopyResponses`: several of the obvious answers to these six asks would quietly undo the
thing the branch was for.

## The `D` number

`D` numbers are **never reused**, the same rule [Known Issues](../appendix/known-issues.md),
[Optimizations](../appendix/optimizations.md), and [Delivered
Features](../features/delivered-features.md) follow. An entry appears in exactly one place. If a
`D` item is built, its page stays here as the design record and a new `F` page describes what was
actually shipped — the two are not the same document, and a design that survived contact with the
code unchanged is rare enough to be worth showing.

## How a `D` page is written

Every page uses the same sections, in this order:

**Context** — what makes this worth having. **What exists today**, cited, because a proposal is
only legible against what it replaces. **The options**, all of them, including the ones not taken.
**Recommendation**, with a scorecard. **What it costs** and **What it breaks** — the second being
the forward-looking twin of a feature page's *Limitations*, and the section that most often turns
out to be the reason something was not built. **Prerequisites**, naming other `D` items by number.
**How it would be measured**. **Related**.

Two of those carry the weight.

**What it breaks.** Each of these six changes invalidates something that currently holds — a pool
of interchangeable connections, a read that lands in aligned memory, a single binary that is both
client and server. Naming it is the point of the page.

**How it would be measured.** [Optimizations](../appendix/optimizations.md) forbids acting on an
entry until a benchmark exists that would show the difference. This part inherits that rule, and
most pages have to answer *nothing can adjudicate this yet* — because `client.rs` carries no
`tracing` spans and no `hotpath` scopes at all
([O28](../appendix/optimizations.md#o28-the-client-takes-two-guards-on-its-response-map-for-every-query-it-sends)),
and the `transport/*`, `wire_codec`, and `routing` workloads that would give the client a number
are unbuilt ([TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have)). Saying so
on every page is repetitive on purpose: it is the same missing thing each time, which is what makes
it step 0 below.

The scorecard uses the axes and grades defined in
[Optimizations](../appendix/optimizations.md#how-these-are-ranked) — `Impact` graded by what backs
the claim, `Difficulty` S through XL, `Tradeoff` None / Contained / Major — so the two pages can be
read against each other. Note that every entry here is at least `L` by that scale's own definition,
since `XL` *means* "reaches the wire format, the on-disk format, or the client".

## The chapter

| # | Page | In one line |
| --- | --- | --- |
| D1 | [The transport](transport.md) | Stay on TCP; QUIC's headline feature is already implemented in userspace and its crypto would end zero-copy |
| D2 | [Framing and protocol evolution](framing.md) | An 8-byte header with a version, a type, and a bounded length — the keystone, and the flag day worth spending now |
| D3 | [Authentication](authentication.md) | mTLS identity as the primary, SCRAM-SHA-256 for deployments with no PKI |
| D4 | [Encryption in transit](encryption.md) | rustls decrypting in place into the response buffer, so TLS does not cost the zero-copy path |
| D5 | [Runtime portability](runtimes.md) | Split the crate first — the client is not tokio-portable, it is *glommio-infected*, and that is the real defect |
| D6 | [A production connection pool](connection-pool.md) | Deadlines, real health checks, a builder, and a `Drop` — the most stability per unit of design risk |
| D7 | [Shard-aware routing](shard-aware-routing.md) | Build it last, and measure the intra-node hop first, because it may be worth a microsecond |
| D8 | [Compile-time guarantees](typed-queries.md) | A `Query::Response` associated type, and a sealed trait that deletes eighty lines of copy-pasted bounds |
| D9 | [Lessons from other databases](prior-art.md) | Scylla, Cassandra, FoundationDB, Aerospike, Redis, TiKV, Dragonfly, Kafka — what to copy and what not to |

## The dependency graph

The book has no mermaid preprocessor, so this is a table
([Optimizations](../appendix/optimizations.md) does the same for the same reason). Hard edges only.

| Edge | Why |
| --- | --- |
| D2 → D3, D4, D6, D7 | A handshake, a `Ping`, a `Topology` push, and a `GoAway` are message types, and there is no message-type field to carry one |
| D4 → D3 | mTLS makes authentication a byproduct of encryption. Choosing SCRAM instead is only *forced* if D4 is declined, so D4 decides D3 rather than the reverse |
| D5 → D2, D7 | The crate split has to happen before anything outside `shoal-core` can consume a protocol module or a topology map |
| D6 → D7 | Shard-awareness turns one flat pool into per-shard sub-pools. The pool has to be rebuildable before it can be resharded |
| D2 ↔ D8 | The strongest compile-time guarantee available — that the peer was built from the same schema — lives in a handshake field, not in the type system |
| D1 → everything | Only in the sense that declining QUIC is what makes D2 and D4 real work rather than free |

The one edge that is **not** there is worth naming: **D3 does not block D4.** Encryption without
authentication is a coherent deployment (a trusted network that still wants confidentiality), and
so is authentication without encryption (SCRAM over a plaintext link, which is what SCRAM was
designed for). They are ordered by convenience here, not by necessity.

## The recommended order

Step 0 is not a `D` item, and it is the most important line on this page.

**0. Instrument the client.** `tracing` spans and `hotpath` scopes in `client.rs`, and the
`transport/{send_one,send_batched,stream,stream_unordered}` workloads
([TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have)). Today every macro
number in [Benchmark Results](../operations/benchmark-results.md) includes the client and **none of
them can attribute anything to it** — the total this chapter proposes to change has never been
bounded. It is also the cheapest thing on the list. Nothing below can be judged until it is done.

1. **[D5](runtimes.md)'s crate split.** Independent of everything else, closes [item
   54](../appendix/known-issues.md#54-shoaldb-needs-three-crates-the-caller-has-never-heard-of),
   and changes no behaviour. The only item here with no design risk at all.
2. **[D2](framing.md).** The keystone, and a flag day. Every later item becomes additive once it
   lands, and it is cheapest now, while the only deployments are tests, benchmarks, and `shoalctl`.
3. **[D6](connection-pool.md).** Deadlines, health, configuration, `Drop`. The largest stability
   return for the least design risk, and it is where the seam for D7 gets put in.
4. **[D4](encryption.md), then [D3](authentication.md).** In that order, because the encryption
   decision is what makes the authentication decision.
5. **[D8](typed-queries.md).** Entirely additive and parallel to all of the above. Its cheapest
   piece — the sealed bounds trait — could land any time.
6. **[D7](shard-aware-routing.md).** Last, and only after `routing` says what the hop it removes is
   worth.

**D7 being last is a claim, not an ordering convenience.** It is the item that looks most like a
database feature and it is the one whose value is least established: the hop it eliminates is a
`kanal` send between two cores on the same machine. See
[D7](shard-aware-routing.md#how-it-would-be-measured).

## Related

- [The Client](../api/client.md) — what the client does now, which every page here starts from
- [Wire Protocol](../architecture/wire-protocol.md) — the framing all of this lands on
- [TODOs and Unbuilt Work](../appendix/todos.md) — the entries this part supersedes, kept in place
- [Optimizations](../appendix/optimizations.md) — the ranking vocabulary, and the rule about
  benchmarks that this part inherits
- [Known Issues](../appendix/known-issues.md) — items 15, 23, 32, 33, 34, 54, 56, and 60 are all
  closed by something proposed here
