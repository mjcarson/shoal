# D2. Framing and protocol evolution

## Context

This is the keystone of the chapter, and it is the only item here that is a prerequisite for four
others. Every one of the following is a message the protocol has no way to express:

| Wanted | Needs a frame that says | For |
| --- | --- | --- |
| A pool health check that detects a dead peer | `Ping` / `Pong` | [D6](connection-pool.md), and the `client.rs:82` TODO |
| A client proving who it is | `Auth` / `AuthResponse` | [D3](authentication.md) |
| A client learning which shard owns which tablet | `Topology` | [D7](shard-aware-routing.md) |
| A server saying a read failed rather than returning nothing | `Error` | [items 51, 55, 56](../appendix/known-issues.md#56-a-response-cannot-say-that-a-read-failed) |
| A server draining a connection before it closes | `GoAway` | [item 32](../appendix/known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere) |
| A client abandoning a query it will never read | `Cancel` | [item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client), and every deadline in D6 |

There is no message-type field, so every row in that table is blocked on the same eight bytes.
`todos.md` already records one of them as blocked for exactly this reason — the ping "needs a
message-type field the wire format does not have"
([TODOs](../appendix/todos.md#client-and-ui)).

**Do this one first, and do it while it is cheap.** It is a flag day: every client and server binary
has to change together. Today the deployments are the integration tests, `shoal-bench`, and
`shoalctl`, all of which are compiled from this tree. That is the cheapest this will ever be.

## What exists today

Two frames, four hardcoded call sites, no shared constant, and no protocol module.

```
 client → server
 ┌────────────────────┬─────────────────────────────────────┐
 │ length (8 B, LE)   │ rkyv-archived Queries<S>            │
 └────────────────────┴─────────────────────────────────────┘

 server → client
 ┌──────────────────┬────────────────────┬────────────────────────────┐
 │ query id (16 B)  │ length (8 B, LE)   │ rkyv-archived ResponseKinds│
 └──────────────────┴────────────────────┴────────────────────────────┘
```

Written from an `IoSlice` set on both sides (`client.rs:220` in `Shoal::send`, `shard.rs:101-107`
in `client_tx_relay`), read as a fixed preamble then an exact-size body (`shard.rs:56-71` in
`client_rx_relay`, `client.rs:504-527` in `TcpProxy::start`). One frame per response, not per
bundle.

What the format does not carry:

- **No version.** Client and server must be compiled from the same schema *and* the same rkyv
  layout. A mismatch is undefined behaviour caught, at best, by `bytecheck`
  ([Wire Protocol](../architecture/wire-protocol.md#limitations)).
- **No message type.** The payload type is inferred from the direction of travel.
- **No maximum size.** The length is used as an allocation size before a byte of the body is read,
  on both sides — `BytesMut::zeroed(len)` at `shard.rs:69`, `AlignedVec::with_capacity(len)` at
  `client.rs:524` ([item 34](../appendix/known-issues.md#34-the-request-length-prefix-is-unvalidated)).
- **No error path.** `ResponseAction` has five variants and none of them carries an error
  (`shoal-core/src/shared/responses.rs:26-40`), which is why the server expresses failure as a
  panic.

## The options

### The header itself

**An 8-byte fixed header on both directions**, ahead of everything currently written:

```
 ┌─────────┬─────────┬──────────┬────────────────────┐
 │ version │  type   │  flags   │   length (u32 LE)  │
 │  (1 B)  │  (1 B)  │  (2 B)   │       (4 B)        │
 └─────────┴─────────┴──────────┴────────────────────┘
```

Three sizing decisions, each of which could have gone the other way:

**`u32` length, not `u64`.** Four gibibytes is already an absurd frame, and the field is paid on
every one of N frames per bundle rather than once per bundle — a 100-query bundle writes 100
response frames (`architecture/wire-protocol.md`). Combined with a configured `max_frame_bytes`
checked before allocating, this closes item 34 on both sides at once, and the check is the point:
a bound the format *has* is enforceable, where a bound it lacks is not.

**A version byte, not a negotiated feature set.** One byte compared against a known-supported range
in `Hello`, and a connection that cannot agree is refused with a legible message rather than
mis-parsed. Cassandra put the version in the first byte of its native protocol and evolved for
fifteen years on it ([D9](prior-art.md#cassandra)).

**Two flag bytes, spent slowly.** The known callers are: this response is an error; the client's
topology is stale ([D7](shard-aware-routing.md)); this frame's payload is the last for its query.
Reserving sixteen bits now costs two bytes and avoids a second flag day for the first three
things that turn out to need one bit.

The response's 16-byte query id stays where it is, after the header — it is a routing field, not a
framing field, and only response frames have one.

### What the header must not break

**The client's split read.** The client reads its preamble into a stack array and its body into a
freshly allocated `AlignedVec<16>`:

```rust
let mut preamble: [u8; 24] = [0; 24];
...
let mut aligned_buff = AlignedVec::<16>::with_capacity(len);
```

`shoal-core/src/client.rs:504-527`, `TcpProxy::start`

That two-read structure is what makes the payload land at the start of an aligned allocation
regardless of how long the preamble is, which is what makes the response path zero-copy. A header
of any length is fine; **a single read of header-plus-body into one buffer is not**, and the
temptation to "optimize" the two `read_exact` calls into one is the thing that would silently undo
it. That belongs in the invariants of whatever page describes the built version.

### Message types

| Type | Direction | Unblocks |
| --- | --- | --- |
| `Hello`, `HelloAck` | both | version and schema agreement, [D3](authentication.md), [D4](encryption.md) |
| `Auth`, `AuthResponse` | both | [D3](authentication.md) — a multi-round SASL exchange needs both to repeat |
| `Queries` | client → server | what the current request frame becomes |
| `Response` | server → client | what the current response frame becomes |
| `Ping`, `Pong` | both | [D6](connection-pool.md)'s health check |
| `Topology` | server → client | [D7](shard-aware-routing.md), pushed rather than polled |
| `Error` | server → client | a failure that is not attached to a query |
| `GoAway` | server → client | a drain before close — [item 32](../appendix/known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere) |
| `Cancel` | client → server | a query nobody will read — [item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client) |

### The schema fingerprint

**The single largest safety win in this chapter, and the macro delivers it for free.**

The protocol's worst failure mode is not a corrupt frame, which `bytecheck` catches. It is a client
and a server compiled from *different but structurally similar* schemas — a field reordered, a
variant inserted, a type widened. rkyv's validation checks that bytes are well-formed for the type
it is told to read, not that the peer meant that type. `#[shoal::db]` already generates
`QueryKinds` and `ResponseKinds` from the schema
(`shoal-derive/src/structs/query_kinds.rs`), so it can also emit

```rust
pub const SCHEMA_FINGERPRINT: u64 = /* over table names, field names, types, and order */;
```

exchanged in `Hello` and compared before the first query. A mismatch becomes a refused connection
naming both fingerprints, instead of undefined behaviour.

Note where this guarantee lives. [D8](typed-queries.md) makes the client's *own* view of its
responses total, and that is worth doing — but no amount of client-side typing helps when the peer
was built from a different schema, because both sides are individually consistent and only their
agreement is wrong. **The strongest compile-time guarantee available to this system is a runtime
handshake field.**

### The error channel

A `ResponseAction::Error` variant, plus the frame-level `Error` type for failures with no query to
attach to. Three open items want the same variant and none of them can be closed without it:

- [item 51](../appendix/known-issues.md#51-a-partition-load-that-fails-inside-load_partition-still-never-releases-its-queries)
  — a partition load that fails has no way to tell the waiting queries so.
- [item 55](../appendix/known-issues.md#55-a-get-that-found-nothing-is-reported-as-a-query-that-failed)
  — a get that matched no rows is indistinguishable from a get that failed, so `send_one` reports
  an empty table as a broken server.
- [item 56](../appendix/known-issues.md#56-a-response-cannot-say-that-a-read-failed) — the general
  case.

It is also what makes two other things possible. Most of the server's hot-path panics
([item 16](../appendix/known-issues.md#16-panics-on-the-hot-path)) exist because there is nowhere
for an error to go — `client_rx_relay` panics on any non-EOF socket error (`shard.rs:63-64`) and
`client_tx_relay` panics on both a short write and a write error (`shard.rs:110-114`), killing a
shard and every other client it was serving. And bounding the channels
([item 15](../appendix/known-issues.md#15-no-backpressure-anywhere)) requires a way to say
*shedding*, which is an error the client has to be able to receive.

## Recommendation

**Take it, first, as one change.**

| | |
| --- | --- |
| **Rank** | **A1** — four other pages in this part are blocked on it, and it is cheapest now |
| **Impact** | Argued — the cost is 8 bytes per frame; the value is that five other items become possible |
| **Difficulty** | XL — reaches the wire format and both peers by definition |
| **Depends on** | [D5](runtimes.md)'s crate split, if the framing is to live somewhere a client-only crate can see it |
| **Blocks** | [D3](authentication.md), [D4](encryption.md), [D6](connection-pool.md), [D7](shard-aware-routing.md) |
| **Tradeoff** | Major — a compatibility break, taken once and deliberately |
| **Benchmark** | `wire_codec`, unbuilt ([TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have)) |

Do the whole header at once — version, type, flags, bounded length — rather than adding a type byte
now and a version byte later. The expensive part is the flag day, and it is paid per break, not per
field.

**Put it in a module.** The four call sites should become one encoder and one decoder with the
sizes as constants. A protocol with no shared definition of its own header is how the two
directions came to disagree about endianness — the query id is big-endian field order per RFC 4122,
the lengths are little-endian
([Wire Protocol](../architecture/wire-protocol.md#framing)) — which is harmless and entirely
accidental.

## Alternatives rejected

**Kafka's per-API-key versioning.** Every request carries an API key and a version for that key, so
a cluster can be upgraded while old clients keep working, indefinitely and granularly. It is the
most permissive evolution model in wide use, and it is far more machinery than a system whose
client and server are usually built from the same commit needs. Recorded because if Shoal ever
ships a client independently of a server, this is the model to revisit
([D9](prior-art.md#kafka)).

**A self-describing envelope — protobuf, CBOR, or MessagePack — around the rkyv payload.** Buys
introspectable frames and mature tooling. Costs a parse on every frame, on the path whose whole
value is that there is no parse. Declined on the same grounds as QUIC in [D1](transport.md): the
zero-copy read is the thing being protected.

**Negotiating an rkyv layout version rather than a protocol version.** Tempting because the real
hazard is a layout mismatch, but it couples the wire contract to a dependency's internal
versioning, and it does not catch the actual failure — two peers on identical rkyv with different
*schemas*. The fingerprint catches that; a layout version does not.

**A length-delimited stream framing crate (`tokio-util`'s `LengthDelimitedCodec`).** Would replace
the client's hand-rolled read loop with a maintained one, but it owns its buffers, which is the same
alignment problem QUIC has, and the server side is glommio and cannot use it anyway.

## What it costs

Eight bytes per frame, in both directions. On the response side that is 8 bytes on top of a
24-byte preamble per response, and a bundle of 100 queries produces 100 responses — so it is 800
bytes per 100-query bundle, against payloads that carry rows. On the request side it is 8 bytes per
bundle. Neither is measurable without `wire_codec`, and neither is plausibly material.

The real cost is the flag day.

## What it breaks

**Every existing client and server binary, once.** There is no version field today, so there is no
mechanism by which an old peer could recognise a new frame and decline it — an old server reading a
new frame will read the first eight bytes as a length and allocate whatever they happen to spell.
That is the strongest single argument for doing this before anything ships, and it should be said
plainly on the page rather than discovered.

It also breaks the assumption that the two directions are structurally different — today the client
knows it is reading responses because it is a client. With a type byte both peers run a dispatch,
and `TcpProxy::start` grows a match where it currently has a straight line.

## Prerequisites

[D5](runtimes.md)'s crate split, and only for placement: the framing wants to live in a crate that
does not drag in glommio, so that a client-only consumer can link it. The header can be built
without the split and moved later, but doing the split first avoids writing it in a module that has
to move.

## How it would be measured

The `wire_codec` workload — rkyv round trips over `Queries` and `ResponseKinds` plus the framing —
is the one that would adjudicate this, and it is unbuilt. It is the same benchmark
[O1](../appendix/optimizations.md#o1-queries-are-fully-deserialized-on-arrival) and
[O2](../appendix/optimizations.md#o2-every-returned-row-is-copied-at-least-twice) are blocked on,
which is an argument for building it once and getting three answers.

The honest position is that this change does not need a benchmark to justify it — 8 bytes against
a frame that carries rows is not a performance question — but the codec bench should exist before
the change lands, because it is the only thing that would catch a header that accidentally made the
payload unaligned.

## Related

- [Wire Protocol](../architecture/wire-protocol.md) — what is being replaced, in detail
- [D1. The transport](transport.md) — why this framing is being built rather than inherited
- [D3](authentication.md), [D4](encryption.md), [D6](connection-pool.md),
  [D7](shard-aware-routing.md) — the four pages that cannot start until this lands
- [D8. Compile-time guarantees](typed-queries.md) — where the fingerprint fits against the type
  system
- [D9. Lessons from other databases](prior-art.md#cassandra) — Cassandra's header, which is this
  proposal with fifteen years of evidence behind it
