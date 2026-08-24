# Wire Protocol

Shoal speaks a framed binary protocol over TCP. Every frame opens with the same eight bytes; a
connection opens with a handshake that agrees a protocol version, a schema, and — since
[F12](../features/authentication.md) — optionally an authentication mechanism. Since
[F14](../features/encryption-in-transit.md) the whole of it can run inside TLS 1.3, which is
established before the first byte of this format crosses and which the format itself knows nothing
about: the kernel does the record layer, so every layout below describes plaintext either way.

The format lives in one place — `shoal-core/src/shared/protocol.rs`, with `protocol/handshake.rs`,
`protocol/fingerprint.rs`, `protocol/error.rs` and `protocol/auth.rs` beside it. It depends on
`core` and `uuid` and nothing else, which
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
| `Auth` | 3 | client → server | yes — [F12](../features/authentication.md) |
| `AuthResponse` | 4 | server → client | yes — [F12](../features/authentication.md) |
| `Queries` | 5 | client → server | yes |
| `Response` | 6 | server → client | yes |
| `Ping` | 7 | both | reserved — [D6](../direction/connection-pool.md) |
| `Pong` | 8 | both | reserved — [D6](../direction/connection-pool.md) |
| `Topology` | 9 | server → client | reserved — [D7](../direction/shard-aware-routing.md) |
| `Error` | 10 | server → client | yes — [F11](../features/error-channel.md) |
| `GoAway` | 11 | server → client | reserved — [item 32](../appendix/known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere) |
| `Cancel` | 12 | client → server | reserved — [item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client) |

Starting at 1 rather than 0 is what stops a zeroed buffer decoding as a valid type. The five
reserved entries exist so that the features that need them are a call site rather than a second
flag day — which is what `Auth`, `AuthResponse` and `Error` turned out to be.

`flags` is sixteen bits, four of which are claimed: `IS_ERROR` (1), `STALE_TOPOLOGY` (2), `LAST`
(4), `REFUSED` (8). Two are set today: `REFUSED`, on a `HelloAck` that turns a client away, and
`IS_ERROR`, on every `Error` frame. `IS_ERROR` is redundant against the type byte on that frame and
is set anyway, so that "is this a failure" stays one uniform bit test when a `Response` frame
carrying an error payload starts setting it too — **the type byte remains authoritative, and the
flag is never the sole test**. **Unknown bits are preserved, never rejected** — that is the whole
mechanism by which the other twelve can be spent one at a time without a version bump.

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
Hello    body, 16 B, client → server : fingerprint u64 LE | max_frame_bytes u32 LE | mechanisms u16 LE | reserved [u8;2]
HelloAck body, 16 B, server → client : fingerprint u64 LE | max_frame_bytes u32 LE | reason u8 | mechanism u8 | reserved [u8;2]
```

The `mechanisms` and `mechanism` fields were cut out of the reserved tails by
[F12](../features/authentication.md), not appended to the bodies — both are still 16 bytes and the
version byte did not move. That is what those bytes were reserved for. Zero means "none" in both,
which is what an older peer wrote and read.

The bodies are **fixed bytes, not rkyv archives**. The whole purpose of the exchange is to detect
that the peer's schema — and with it, potentially, its rkyv layout — does not match ours, and
decoding it with rkyv would make the detector depend on the thing it detects.

The version is not in the body. It is in the header of every frame, which is why the version byte
is per frame rather than per connection.

`reason` is 0 for accepted, 1 for an unsupported version, 2 for a schema mismatch, 3 for no
authentication mechanism in common. **A refusal is still a `HelloAck`**, with `REFUSED` set in the header and the server's own version and fingerprint
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

## The authentication exchange

Only when the `HelloAck` named a mechanism, which only happens when the server's config asked for
one. It runs between the ack and the split, on both sides, and under the same ten second deadline
the handshake has — a peer that stalls between its `Hello` and its proof holds exactly as much of
the server as one that stalls before either.

```
Auth         body, 4 B + payload, client → server : mechanism u8 | reserved [u8;3] | SASL payload
AuthResponse body, 4 B + payload, server → client : status u8   | reserved [u8;3] | SASL payload
```

`status` is 1 for a challenge, 2 for success, 3 for a refusal — and a refusal also sets `REFUSED`
in the header, so it can be told from a challenge without reading the body. Both start at 1, so a
zeroed buffer decodes as neither. The payload is opaque to the protocol module: it is RFC 5802's
message text, and what it means lives in `shared::auth`, which is a separate module so that
`protocol` keeps its `core`-and-`uuid`-only dependency list.

**These frames carry no query id**, unlike every other frame a server sends a client. They belong
to the pre-split part of a connection, alongside the handshake, where the reader is
`ShoalConnectionManager::connect` rather than the response proxy and reads one frame at a time
knowing which one it asked for.

**The payload bound is `MAX_AUTH_PAYLOAD_LEN`, 4 KiB — not the connection's `max_frame_bytes`.**
These are the only variable-length frames read from a peer that has proved nothing, and the frame
bound is 64 MiB by default.

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
Proving who a peer is is [F12](../features/authentication.md), which runs *after* this check and is
a separate exchange for exactly this reason — the fingerprint says two peers were built from the
same schema, and says nothing about whether either of them should be talking to the other.

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
    Error(ResponseError),
}
```

`shoal-core/src/shared/responses.rs`

Mutations return only a boolean, so a failed insert and a rejected insert are both `Insert(false)`.
~~**There is still no error channel in the protocol**~~ — [F11](../features/error-channel.md) built
it. A query that *could not run* is `Error(ResponseError { code, msg })`, where the code is a pinned
`u16` from `protocol::error::ErrorCode`, so a get that found nothing and a get whose partition could
not be read are no longer the same answer. The variant is appended, never inserted: rkyv derives the
wire representation from the declaration order.

### The `Error` frame

For a failure with no response to attach it to, the server sends a frame of its own:

```text
 ┌──────────────────┬──────────┬───────────┬─────────────────────┐
 │ query id (16 B)  │ code     │ reserved  │ message (UTF-8)     │
 │                  │ (u16 LE) │  (2 B)    │  len = rest of body │
 └──────────────────┴──────────┴───────────┴─────────────────────┘
```

The query id is at exactly the offset a response frame puts one, which is what lets a client read
one fixed 24-byte preamble for both and dispatch on the type afterwards. A nil id means the frame is
about the connection rather than about a query. The message is bounded at four kibibytes
independently of `max_frame_bytes`, so the error channel cannot become an allocation channel while
staying inside the frame bound, and it is decoded lossily — a garbled message must not be allowed to
swallow the code in front of it.

This is what `client_tx_relay` sends when a response is too large to frame: it holds an opaque
`AlignedVec` and cannot build a `ResponseKinds`, and closing the connection was the only other thing
it could do ([Resolved #56, 61](../appendix/resolved/response-error-channel.md)).

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
let frame = protocol::decode_server_frame(&preamble, self.max_frame_bytes)?;
let aligned_buff = read_payload(&mut self.reader, frame.rest_len).await?;
```

`TcpProxy::read_frame`, `shoal-client/src/client.rs`. The payload read is a helper rather than a
`read_exact` because the buffer is no longer zeroed before it
([F25](../features/read-buffers-are-filled-not-zeroed.md)); it fills the allocation through tokio's
`ReadBuf` and claims the length only once the reader has reported every byte written.

**That two-read structure is the zero-copy read path, and it must not be merged into one.** It puts
the archive at offset zero of a sixteen-byte-aligned allocation, which is what makes
`ShoalResponse` a pointer cast rather than a parse. A single read of preamble plus payload lands
the payload at offset 24, and offset 24 of a sixteen-byte-aligned allocation is never itself
sixteen-byte aligned — so the zero-copy read would end silently. Merging the two `read_exact` calls
looks like an obvious optimization, which is why
`the_response_payload_lands_on_a_sixteen_byte_boundary` parameterises over seven awkward payload
lengths rather than one.

The server does not do this. It reads requests into a `RequestBody` — a `BytesMut` behind a private
field, so that the read that fills it is the only way one can be built
([F25](../features/read-buffers-are-filled-not-zeroed.md)) — which carries no alignment guarantee,
and then calls `Queries::access` on it, which succeeds because rkyv's `access` validates and because
in practice the allocator returns suitably aligned memory. **That is why the removal of the zeroing
could not change the allocation path**: `with_capacity` and `zeroed` allocate the same way, and
anything that did not would end this paragraph's "in practice". The request path also fully
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

- ~~**No authentication, no TLS.**~~ Half built as [F12](../features/authentication.md): a server
  can require SCRAM-SHA-256 and refuse a client that cannot do it, and a connection that completes
  one carries a `Principal`. **What is left is the larger half.** ~~There is no TLS, so the username
  and the whole exchange are visible to anything on the path
  ([D4](../direction/encryption.md))~~ — there is TLS now
  ([F14](../features/encryption-in-transit.md)), and it is off unless a config asks, so on a default
  deployment the username and the whole exchange are still visible on the path; there is no
  authorization, so a principal that authenticated
  can still read and write *any* table; and a server with no `auth` section — which is the default
  and every deployment today — still lets anything that reaches the port do anything.
- ~~**No error responses.**~~ Built as [F11](../features/error-channel.md). What is left is that an
  `Error` **frame** names a bundle rather than one query in it, because a query id is a bundle id —
  so an oversize response fails a whole result stream. The two reserved bytes after the code are
  where an index would go.
- **Five message types are defined and unwired.** `Ping`/`Pong`, `Cancel`, `GoAway`, `Topology`.
  Each is now a call site rather than a flag day, which is what `Auth`/`AuthResponse` turned out
  to be.
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
