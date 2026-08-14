# Wire Protocol

Shoal speaks a framed binary protocol over TCP. Every frame opens with the same eight bytes; a
connection opens with a handshake that agrees a protocol version and a schema. There is no
authentication and no encryption.

The format lives in one place — `shoal-core/src/shared/protocol.rs`, with `protocol/handshake.rs`
and `protocol/fingerprint.rs` beside it. It depends on `core` and `uuid` and nothing else, which
is deliberate: the server reads with glommio and the client with tokio, and the two share every
decision in the module and none of the I/O ([F10](../features/framing-and-protocol-evolution.md)).

## The header

```
 ┌─────────┬─────────┬──────────┬────────────────────┐
 │ version │  type   │  flags   │   length (u32 LE)  │
 │  (1 B)  │  (1 B)  │  (2 B)   │       (4 B)        │
 └─────────┴─────────┴──────────┴────────────────────┘
```

**These eight bytes mean the same thing in every protocol version.** The version byte is at offset
0 and the length is at offsets 4..8, and neither ever moves. That is what lets a peer read a frame
written in a version it does not speak, report which version it saw, and drain exactly the right
number of body bytes before it replies — which is what a version refusal is made of. A future
version may add meaning to the flag bits or change what follows the header; it may not move these
two fields.

**`length` counts every byte after the header**, including a response frame's query id. It is not
the payload length. That is what lets a peer skip a frame whose type it does not know, and it is
what makes `decode_response`'s "shorter than its own query id" check possible.

`version` is `PROTOCOL_VERSION`, currently 1. A frame naming any other version is refused.

`type` is one of twelve, with discriminants that are explicit, start at 1, and are never reused:

| Type | Byte | Direction | Wired |
| --- | --- | --- | --- |
| `Hello` | 1 | client → server | yes |
| `HelloAck` | 2 | server → client | yes |
| `Auth` | 3 | client → server | reserved — [D3](../direction/authentication.md) |
| `AuthResponse` | 4 | server → client | reserved — [D3](../direction/authentication.md) |
| `Queries` | 5 | client → server | yes |
| `Response` | 6 | server → client | yes |
| `Ping` | 7 | both | reserved — [D6](../direction/connection-pool.md) |
| `Pong` | 8 | both | reserved — [D6](../direction/connection-pool.md) |
| `Topology` | 9 | server → client | reserved — [D7](../direction/shard-aware-routing.md) |
| `Error` | 10 | server → client | reserved — the error channel |
| `GoAway` | 11 | server → client | reserved — [item 32](../appendix/known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere) |
| `Cancel` | 12 | client → server | reserved — [item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client) |

Starting at 1 rather than 0 is what stops a zeroed buffer decoding as a valid type. The eight
reserved entries exist so that the features that need them are a call site rather than a second
flag day.

`flags` is sixteen bits, four of which are claimed: `IS_ERROR` (1), `STALE_TOPOLOGY` (2), `LAST`
(4), `REFUSED` (8). Only `REFUSED` is set today, on a `HelloAck` that turns a client away.
**Unknown bits are preserved, never rejected** — that is the whole mechanism by which the other
twelve can be spent one at a time without a version bump.

## Framing

### Client → Server

```
 ┌─────────────────┬─────────────────────────────────────┐
 │ header (8 B)    │ rkyv-archived Queries<S>            │
 └─────────────────┴─────────────────────────────────────┘
  ◀── preamble ───▶◀────────── length bytes ───────────▶
```

Written vectored from two slices (`Shoal::send` and `ShoalQueryStream::send` in
`shoal-core/src/client.rs`), read as an 8-byte header then an exact-size body (`client_rx_relay`
in `shoal-core/src/server/shard.rs`).

### Server → Client

```
 ┌─────────────────┬──────────────────┬────────────────────────────┐
 │ header (8 B)    │ query id (16 B)  │ rkyv-archived ResponseKinds│
 └─────────────────┴──────────────────┴────────────────────────────┘
  ◀────── 24-byte preamble ─────────▶◀────── payload bytes ──────▶
  ◀──────────────── length bytes ───────────────────────────────▶
```

Written vectored from two slices (`client_tx_relay`), read as a 24-byte preamble then an
exact-size payload (`TcpProxy::read_frame` in `shoal-core/src/client.rs`).

The query id stays where it is, after the header: it is a routing field, not a framing field, and
only response frames have one.

**The header costs zero bytes.** The old request frame was an 8-byte `u64` length and the new one
is an 8-byte header; the old response preamble was 16 bytes of query id plus an 8-byte `u64`
length and the new one is 8 bytes of header plus 16 bytes of query id. Narrowing the length to a
`u32` paid for the version, the type and the two flag bytes exactly.
`the_preamble_sizes_are_unchanged` is that claim as an assertion.

The query id is a raw UUID in `Uuid::as_bytes` order — big-endian field order per RFC 4122, *not*
little-endian like the length and the flags. That asymmetry is accidental and harmless; it is
recorded here so that nobody has to rediscover it. Every multi-byte field the protocol itself owns
is little-endian, so it assumes a little-endian peer and does no conversion.

One frame per response, not one per bundle: a bundle of 100 queries produces 100 frames, each
carrying the same query id. That is the argument for the length being a `u32` — it is paid *N*
times per bundle rather than once.

### What the framing does when it goes wrong

Nothing in either relay panics. Every failure ends one connection and leaves the shard and every
other client it serves alone:

| Where | On a frame it cannot read | On a write that fails |
| --- | --- | --- |
| Server (`client_rx_relay`, `client_tx_relay`) | log at `ERROR`, `break` | log at `ERROR`, `break` |
| Client (`TcpProxy::read_frame`, `Shoal::send`) | `Errors::Protocol` | `Errors::IO` |

When the read relay ends, the task that owns it cancels the write relay. That matters more than it
looks: the two halves of a split stream keep the stream alive between them, so a read relay that
ended on its own used to leave the write relay parked on an empty channel holding a socket nobody
would ever read from again.

`shoal/tests/framing.rs` is four raw sockets doing four different wrong things beside a healthy
client, each asserting that the raw connection closed **and that the healthy client still answers**.
The second assertion is the one that matters; before [F10](../features/framing-and-protocol-evolution.md)
every one of those four was a way for one peer to kill the database for everybody
([Resolved #34](../appendix/resolved/unvalidated-length-prefix.md)).

**Every frame is bounded.** `Networking::max_frame_bytes` defaults to 64 MiB, is exchanged in the
handshake, and is checked in a pure decoder before any allocation happens. Each side checks its own
bound before it allocates and the peer's before it writes — the second is what turns "the
connection died" into an error naming both sizes.

**A truncated frame is still indistinguishable from a slow one.** `read_exact` on the body waits
for exactly as many bytes as the header claimed, with no deadline
([TODOs](../appendix/todos.md#timeouts)), so a peer that sends a header and then stops leaves the
relay parked. The handshake is the one exception: it runs under a ten second deadline on both
sides.

## The handshake

A connection opens with one 24-byte frame in each direction. **The client always speaks first** —
it writes and then reads; the server reads and then writes. If both waited to read, every
connection would deadlock and nothing in the frame layout would show it.

```
Hello    body, 16 B, client → server : fingerprint u64 LE | max_frame_bytes u32 LE | reserved [u8;4]
HelloAck body, 16 B, server → client : fingerprint u64 LE | max_frame_bytes u32 LE | reason u8 | reserved [u8;3]
```

The bodies are **fixed bytes, not rkyv archives**. The whole purpose of the exchange is to detect
that the peer's schema — and with it, potentially, its rkyv layout — does not match ours, and
decoding it with rkyv would make the detector depend on the thing it detects.

The version is not in the body. It is in the header of every frame, which is why the version byte
is per frame rather than per connection.

`reason` is 0 for accepted, 1 for an unsupported version, 2 for a schema mismatch. **A refusal is
still a `HelloAck`**, with `REFUSED` set in the header and the server's own version and fingerprint
in the body, written before the socket closes — so a client that was turned away learns why rather
than seeing a reset. The server drains the body it was told about before writing that reply, for a
TCP reason rather than a protocol one: closing a socket with unread bytes queued sends a reset,
which would discard the reply.

Both peers compare fingerprints and each names both numbers in its own error, so the refusal is
legible in the server's log *and* in `Shoal::new`'s return value:

```rust
Errors::Handshake(ConnectError::Protocol(ProtocolError::SchemaMismatch { ours, theirs }))
```

Where it runs: on the client, inside `ShoalConnectionManager::connect`, **before the stream is
split** — moving it after the split would send the `HelloAck` down the response path, where it
decodes as a response to a query nobody sent. On the server, inside the per-connection task, before
the split and before `NewClient` is broadcast. That task is why the accept loop never waits on a
peer; a handshake done inline would let one client that connects and says nothing park every
subsequent connection to a single-shard server.

The client's half runs under a `tokio::time::timeout`. This is not optional: `bb8`'s connection
timeout bounds its retry loop and `pool.get()`, not `connect` itself, so without it a server that
accepts and then stalls would park `Shoal::new` forever.

## The schema fingerprint

A `u64` computed at compile time by the derive macros, folding:

- the database's name and the protocol version;
- per table, in declaration order: the field name it is held under, the whole type it was declared
  as (which carries the table kind and the storage engine), and the table's own constant;
- per row, in declaration order: each field's name, the spelling of its type, the **size and
  alignment of its archived form**, its position, and the roles it plays in a query;
- per declared projection: its name and its own constant.

The mixing function is FNV-1a 64 as a chain of `const fn`s, with a `0xff` separator after every
value. The separator is load-bearing: without it `("ab", "c")` and `("a", "bc")` hash the same, and
renaming a pair of adjacent fields would pass.

The archived size and alignment matter for one case the spelling cannot catch. A type alias whose
definition changes from `u32` to `u64` leaves every declaration reading `Id` — a false *agreement*,
which is the one dangerous direction. Two spellings of the same type disagreeing is the safe
direction and is left as is.

The constant lives on `QuerySupport`, the only trait both peers see, and is required rather than
defaulted so that a hand-written implementation cannot silently opt out.

**This is a mistake detector, not authentication.** A hostile peer can send whatever fingerprint it
likes, and a 64-bit hash can collide. `bytecheck` remains the second line of defence on both paths.
Proving who a peer is needs [D3](../direction/authentication.md).

## Payloads

### `Queries<S>`

```rust
pub struct Queries<S: QuerySupport> {
    pub id: Uuid,
    pub queries: Vec<S::QueryKinds>,
    pub base_index: usize,
}
```

`shoal-core/src/shared/queries.rs`

`id` is the correlation key. The client generates it, ensures it is unique within its own channel
map, and the server echoes it in the preamble of every response.

`base_index` supports streaming. `ShoalQueryStream` sends several bundles under one id and advances
`base_index` by the number of queries already sent, so indices stay globally ordered across
bundles.

`QueryKinds` is generated per database — one variant per table, wrapping that table's
`SortedQuery<T>` or `UnsortedQuery<T>` (`shoal-derive/src/structs/query_kinds.rs`).

### `ResponseKinds`

Also generated. It has one variant per table, mirroring `QueryKinds`, **and one per projection** —
a get that named a projection answers as that type rather than as its table's row type, so the
client can `access::<MovieSummary>()` it ([F2](../features/projections.md#design-choices)). An
archived enum is the size of its largest variant and every `Response<T>` archives to the same size
— a relative pointer and a length — so the extra variants cost nothing on the wire. They do cost
a fingerprint change, because an archived enum's discriminants are wire state.

Every variant wraps:

```rust
pub struct Response<T> {
    pub id: Uuid,
    pub index: usize,
    pub data: ResponseAction<T>,
    pub end: bool,
}

pub enum ResponseAction<T> {
    Insert(bool),
    Get(Option<Vec<T>>),
    Delete(bool),
    Update(bool),
    Exists(bool),
}
```

`shoal-core/src/shared/responses.rs`

Mutations return only a boolean. **There is still no error channel in the protocol**: a failed
insert and a rejected insert are both `Insert(false)`, and a get that found nothing and a get
against a nonexistent partition are both `Get(None)`. The frame-level `Error` type and the flag bit
it would use both exist; nothing constructs either
([item 56](../appendix/known-issues.md#56-a-response-cannot-say-that-a-read-failed)).

## Ordering and completion

Responses arrive in whatever order shards finish. Two fields make reassembly possible:

- **`index`** — the query's position in the logical stream, assigned by the coordinator as
  `index + base_index`.
- **`end`** — set on the last query of a bundle.

`ShoalResultStream` keeps `next_index` and a `BTreeMap<usize, ClientMsg>` of early arrivals,
returning responses strictly in index order. `ShoalUnorderedResultStream` returns them as they
land, tracking a `BTreeSet` of seen indices purely to know when the stream is complete.

For streams the client sets `unbounded_queries: true` and ignores the server's `end` entirely,
terminating on a locally generated `ClientMsg::End(base_index)` sent by `ShoalQueryStream::close`.
This is why the `end` computation being wrong for streamed bundles never showed up in stream tests
— the flag is simply not consulted on that path. It is
[fixed](../appendix/resolved/sorted-limit.md) now, but this path still does not consult it.

## Alignment

rkyv requires archives to be aligned. The client reads its preamble into a stack array and its
payload into a freshly allocated `AlignedVec<16>`:

```rust
let mut preamble = [0u8; protocol::RESPONSE_PREAMBLE_LEN];
self.reader.read_exact(&mut preamble).await?;
let frame = protocol::decode_response(&preamble, self.max_frame_bytes)?;
let mut aligned_buff = AlignedVec::<16>::with_capacity(frame.payload_len);
aligned_buff.resize(frame.payload_len, 0);
self.reader.read_exact(&mut aligned_buff).await?;
```

`TcpProxy::read_frame`, `shoal-core/src/client.rs`

**That two-read structure is the zero-copy read path, and it must not be merged into one.** It puts
the archive at offset zero of a sixteen-byte-aligned allocation, which is what makes
`ShoalResponse` a pointer cast rather than a parse. A single read of preamble plus payload lands
the payload at offset 24, and offset 24 of a sixteen-byte-aligned allocation is never itself
sixteen-byte aligned — so the zero-copy read would end silently. Merging the two `read_exact` calls
looks like an obvious optimization, which is why
`the_response_payload_lands_on_a_sixteen_byte_boundary` parameterises over seven awkward payload
lengths rather than one.

The server does not do this. It reads requests into a `BytesMut`, which carries no alignment
guarantee, and then calls `Queries::access` — which succeeds because rkyv's `access` validates and
because in practice the allocator returns suitably aligned memory. The request path also fully
deserializes anyway, so it gains nothing from alignment today.

## Validation

| Direction | Validation |
| --- | --- |
| Header, both directions | Version, type, and length against the frame bound, in a pure decoder over a fixed-size array |
| Client → Server | `Queries::access` runs `bytecheck` validation before use (`shared/queries.rs`) |
| Server → Client | `ShoalResponse::new` runs `RkyvSupport::access`, also validated (`client.rs`) |
| Connection open | Protocol version and schema fingerprint, in the handshake |

There is one deliberately unchecked path: `ShoalDatabase::unarchive_queries` uses
`rkyv::access_unchecked` (`shared/traits.rs`). It is not called on the live request path, which
goes through the validated `Queries::access`.

## Design notes

**One module, one encoder, one decoder.** The four hardcoded call sites this format used to live at
are how the two directions came to disagree about endianness. The sizes are constants and the
codec is pure functions over fixed-size arrays, so a length can never reach an allocation without
passing its bound.

**The correlation id is per bundle, not per query.** Routing responses needs only the id; ordering
within the bundle is the `index` field's job. That keeps the preamble at 24 bytes and lets the
server stream responses as they complete.

**Vectored writes on both sides.** Header and payload are separate allocations that are never
concatenated, on either direction of the connection. The encoders return stack arrays rather than
`Vec`s precisely to keep that true.

## Limitations

- **No authentication, no TLS.** Anything that can reach the port can read and write any table, and
  the handshake proves nothing about who a peer is
  ([D3](../direction/authentication.md), [D4](../direction/encryption.md)).
- **No error responses.** `ResponseAction` has five variants and none carries an error
  ([item 56](../appendix/known-issues.md#56-a-response-cannot-say-that-a-read-failed)). This is what
  stops a read that failed being distinguishable from a read that found nothing, and it is why a
  response too large to frame closes a connection with nothing on the wire to say why
  ([item 61](../appendix/known-issues.md#61-a-response-too-large-to-frame-closes-a-connection-silently)).
- **Eight message types are defined and unwired.** `Ping`/`Pong`, `Cancel`, `GoAway`, `Topology`,
  `Auth`/`AuthResponse`, `Error`. Each is now a call site rather than a flag day.
- **Little-endian assumed** for every field the protocol owns.
- **No deadline on a frame** once the handshake is done. A peer that sends a header and then stops
  parks the reader indefinitely ([D6](../direction/connection-pool.md#deadlines)).
- **No keepalive or ping.** The pool's health check still calls `peer_addr()`, which does not detect
  a dead peer ([D6](../direction/connection-pool.md#health-checks-that-work)). The message type it
  needs exists.
- **A client learns one frame bound.** `peer_max_frame_bytes` is a single value shared across the
  pool. Every connection in a pool goes to one address today, so this cannot bite yet; it becomes
  real with [D7](../direction/shard-aware-routing.md)'s per-shard endpoints.

**What the transport itself is not.** TCP is a decision rather than an inheritance, and the case
for keeping it — including why QUIC's stream multiplexing is already implemented one layer up in
`channel_map`, and why its mandatory crypto would end the zero-copy read described under
[Alignment](#alignment) — is in [D1](../direction/transport.md).
