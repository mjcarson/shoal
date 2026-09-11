# Direction

**Nothing in this part is built.** Every page describes work that could be done to the client, not
work that has been. A `D` page is the design that would be written down *before* a feature page, in
the same way [TODOs](../appendix/todos.md) records that something is unbuilt — the difference is
that this part says how it would be built, what it would cost, and what it would break.

Six things are wanted from the client eventually: authentication, encryption, portability across
async runtimes, a pool that survives a production failure, shard-aware routing over the tablet map,
and a stronger compile-time link between a query and its response. They are usually discussed as
six independent features. **They are not.** Five of the six land on a wire protocol that had no
message-type field, no version, and no error channel
([Wire Protocol](../architecture/wire-protocol.md)), so the first question each of them asks is the
same question, and answering it once is most of the work.

That is why this is a chapter rather than six entries in `todos.md`.

**D2 has since landed**, in two parts. [F10](../features/framing-and-protocol-evolution.md) is the
answer to that shared question: a version, a message type, two flag bytes, a bounded length, and a
handshake carrying a schema fingerprint. [F11](../features/error-channel.md) took the error channel
F10 had left out of scope, in both the `ResponseAction::Error` half and the frame-level `Error`
half. The discriminants for `Ping`, `Pong`, `Topology`, `GoAway` and `Cancel` are all still defined
and unwired, so each of the pages below now needs a call site rather than a flag day, and **no page
in this chapter is waiting on a wire format any more.**

**D3 has since landed by half**, as [F12](../features/authentication.md): SCRAM-SHA-256 spent the
`Auth` and `AuthResponse` discriminants and two of the handshake's reserved bytes, and produced the
`Principal` that per-table authorization was filed as blocked on. The mTLS half is still open and
still waits on D4. The most useful thing that build says about this chapter is in its own
[Recommendation](authentication.md#recommendation): the D4 → D3 edge below was **wrong**, and it
was wrong in a way worth watching for on the other pages — it treated two mechanisms as
alternatives when they are two entries behind one negotiation step.

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
~~and `client.rs` carries no `tracing` spans and no `hotpath` scopes at all~~ — it carries both
since [F16](../features/client-builder.md) — and ~~the `transport/*`, `wire_codec`, and `routing`
workloads that would give the client a number are unbuilt~~ — `transport/*` is **built** ([F13](../features/transport-workloads.md)), `wire_codec`
and `routing` are not ([TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have)).
Saying so on every page is repetitive on purpose: it is the same missing thing each time, which is
what makes it step 0 below. **What F13 changed is which half is missing.** The client now has a
bounded *total* at four transport modes and two row widths, so a change to it can be shown to have
moved something. What it still has no way to do is *attribute* — a transport sample includes the
server, and only the spans can separate them.

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
| D3 | [Authentication](authentication.md) | mTLS identity as the primary, SCRAM-SHA-256 for deployments with no PKI. **Half built** — [F12](../features/authentication.md) is the SCRAM half; mTLS waits on D4 |
| D4 | [Encryption in transit](encryption.md) | ~~rustls decrypting in place into the response buffer~~ — **no released rustls does that**; kTLS, with rustls doing only the handshake, so TLS does not cost the zero-copy path. **Built** as [F14](../features/encryption-in-transit.md) |
| D5 | [Runtime portability](runtimes.md) | Split the crate first — the client is not tokio-portable, it is *glommio-infected*, and that is the real defect |
| D6 | [A production connection pool](connection-pool.md) | Deadlines, real health checks, a builder, and a `Drop` — the most stability per unit of design risk. **Builder half built** as [F16](../features/client-builder.md), which also closed step 0 |
| D7 | [Shard-aware routing](shard-aware-routing.md) | Build it last, and measure the intra-node hop first, because it may be worth a microsecond |
| D8 | [Compile-time guarantees](typed-queries.md) | A `Query::Response` associated type, and a sealed trait that deletes eighty lines of copy-pasted bounds |
| D9 | [Lessons from other databases](prior-art.md) | Scylla, Cassandra, FoundationDB, Aerospike, Redis, TiKV, Dragonfly, Kafka — what to copy and what not to |

## The dependency graph

The book has no mermaid preprocessor, so this is a table
([Optimizations](../appendix/optimizations.md) does the same for the same reason). Hard edges only.

| Edge | Why |
| --- | --- |
| ~~D2~~ → D3, D4, D6, D7 | A handshake, a `Ping`, a `Topology` push, and a `GoAway` are message types, and there was no message-type field to carry one. **Satisfied** by [F10](../features/framing-and-protocol-evolution.md); all four discriminants exist |
| ~~D4 → D3~~ | mTLS makes authentication a byproduct of encryption. Choosing SCRAM instead is only *forced* if D4 is declined, so D4 decides D3 rather than the reverse. **This edge was not real.** The two are mechanisms behind one negotiation step rather than alternatives, so D4 adds a mechanism to D3 instead of deciding it — see [F12](../features/authentication.md). What is left is a soft edge in the other direction: D3's *mTLS half* waits on D4 |
| D5 → ~~D2~~, D7 | The crate split has to happen before anything outside `shoal-core` can consume a topology map. It turned out **not** to gate the protocol module: that module depends on `core` and `uuid` and nothing else, so it was written inside `shoal-core` and moves to `shoal-proto` unchanged |
| D6 → D7 | Shard-awareness turns one flat pool into per-shard sub-pools. The pool has to be rebuildable before it can be resharded |
| D2 ↔ D8 | The strongest compile-time guarantee available — that the peer was built from the same schema — lives in a handshake field, not in the type system |
| D1 → everything | Only in the sense that declining QUIC is what makes D2 and D4 real work rather than free |

The one edge that is **not** there is worth naming: **D3 does not block D4.** Encryption without
authentication is a coherent deployment (a trusted network that still wants confidentiality), and
so is authentication without encryption (SCRAM over a plaintext link, which is what SCRAM was
designed for). They are ordered by convenience here, not by necessity.

## The recommended order

Step 0 is not a `D` item, and it is the most important line on this page.

**0. Instrument the client. Done**, as part of [F16](../features/client-builder.md) — `tracing`
spans and `hotpath` scopes in `client.rs`, and
~~the `transport/{send_one,send_batched,stream,stream_unordered}` workloads
([TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have))~~ — **the workloads are
done** ([F13](../features/transport-workloads.md)), at eight rather than four, because a row-width
axis turned out to decide whether they could answer [D4](encryption.md) at all. Today every macro
number in [Benchmark Results](../performance/overview.md) includes the client and **none of
them can attribute anything to it** — the total this chapter proposes to change ~~has never been~~
**is now** bounded, and ~~the attribution is not. The spans are what remain, they are the cheapest
thing on the list, and nothing below can be *attributed* until they are done.~~ so is the
attribution. What F16 also found is that ~~**the spans reach nobody**: `trace::setup` has no callers
anywhere in this workspace~~ ~~— the bundled example calls it now […] `shoal-workload` and
`shoalctl` still install no subscriber, so a benchmark capture's `tracing` section still configures
nothing~~ — **`shoal-workload` installs one too**, since
[F34](../features/benchmark-tracing.md), so a capture honors its `tracing:` section and the spans
reach a collector on every run of one. `shoalctl` and the tests still install nothing
([item 69](../appendix/known-issues.md)). F16's *measurement* is unaffected either way: it was
taken under no subscriber and says what these spans cost in that configuration. The scopes work regardless — `hotpath` does not go through
`tracing` — so the attribution this step was for is available, and making the *spans* reach a
collector is a separate piece of work that step 0 turned up rather than one it was.

1. **[D5](runtimes.md)'s crate split.** Independent of everything else, closes [item
   54](../appendix/known-issues.md#54-shoaldb-needs-three-crates-the-caller-has-never-heard-of),
   and changes no behaviour. The only item here with no design risk at all.
2. **[D2](framing.md).** The keystone, and a flag day. Every later item becomes additive once it
   lands, and it is cheapest now, while the only deployments are tests, benchmarks, and `shoalctl`.
3. **[D6](connection-pool.md).** Deadlines, health, configuration, `Drop`. The largest stability
   return for the least design risk, and it is where the seam for D7 gets put in. **The
   configuration third is built** ([F16](../features/client-builder.md)) — the builder, the
   endpoint list and the instrumentation — and the seam for D7 is the `PoolConfig` it introduced.
   Deadlines and `Drop` are next; the health check waits on nothing but the work. `Cancel` was
   **dropped from scope** on inspection, because the two things D6 said needed it had already been
   fixed from the other end ([TODOs](../appendix/todos.md)).
4. ~~**[D4](encryption.md), then [D3](authentication.md).** In that order, because the encryption
   decision is what makes the authentication decision.~~ **D3's SCRAM half was done first**, out of
   this order and without D4, because the ordering rested on the edge struck through above. What is
   left here is **[D4](encryption.md)**, which now also carries D3's remaining half. **D4 has since
   been built too**, as [F14](../features/encryption-in-transit.md) — so nothing remains at this
   step. D3's mTLS half is unblocked and unbuilt, and is filed in
   [TODOs](../appendix/todos.md) rather than left here, since it is now a missing feature rather
   than a blocked design.

   **D4's recommendation has since been corrected**, and the correction is worth reading before
   starting any page in this chapter, because it is a failure of method rather than of judgement.
   The page recommended rustls' unbuffered API on the strength of that API's documentation, which
   describes it as letting the caller supply the buffers. It does — for ciphertext. The plaintext
   still comes out of a `Vec` rustls owns, which the crate's own source says plainly and its
   documentation does not. **A design page that argues from a dependency's prose rather than its
   source is a page that can be confidently wrong**, and this chapter argues from dependencies on
   almost every page. The corrected recommendation is kTLS.
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
- [Distributed Shoal](../distributed/overview.md) — the multi-node half, which this chapter never
  claimed and which now has a part of its own; its [C4](../distributed/tablet-map.md) builds D7's
  step 1, the `Topology` frame, because the cluster needs it before the client does
