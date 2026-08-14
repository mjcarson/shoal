# Wire Protocol

Shoal speaks a minimal length-prefixed binary protocol over TCP. There is no handshake, no
version negotiation, no authentication, and no encryption.

## Framing

### Client → Server

```
 ┌────────────────────┬─────────────────────────────────────┐
 │ length (8 B, LE)   │ rkyv-archived Queries<S>            │
 └────────────────────┴─────────────────────────────────────┘
                       ◀────────── length bytes ───────────▶
```

Written vectored from two slices (`shoal-core/src/client.rs:221`), read as an 8-byte length
then an exact-size body (`shoal-core/src/server/shard.rs:54-71`).

### Server → Client

```
 ┌──────────────────┬────────────────────┬────────────────────────────┐
 │ query id (16 B)  │ length (8 B, LE)   │ rkyv-archived ResponseKinds│
 └──────────────────┴────────────────────┴────────────────────────────┘
  ◀───────── 24-byte preamble ─────────▶◀────── length bytes ───────▶
```

Written vectored from three slices (`shoal-core/src/server/shard.rs:103-107`), read as a
24-byte preamble then an exact-size body (`shoal-core/src/client.rs:504-527`).

The query id is a raw UUID in `Uuid::as_bytes` order — big-endian field order per RFC 4122,
*not* little-endian like the length. Lengths are little-endian throughout
(`u64::to_le_bytes` / `from_le_bytes`), so the protocol assumes a little-endian peer and does
no conversion.

One frame per response, not one per bundle: a bundle of 100 queries produces 100 frames, each
carrying the same query id.

### What the framing does when it goes wrong

There is no recovery anywhere in it. Both directions are written the same way — advance the
`IoSlice` set until it is empty — and both treat any deviation as fatal to the whole process
rather than to the connection:

| Where | On a short write | On an error |
| --- | --- | --- |
| Server → client (`shard.rs:109-116`) | `panic!("No bytes were written?")` | `panic!("Ahhh error?: …")` |
| Client → server (`client.rs:223-236`) | `Errors::IO(WriteZero)` | propagated as `Errors::IO` |

The asymmetry is the whole point. The client returns an error the caller can act on; the server
kills the shard, and with it every other client that shard was serving. The same is true one step
earlier on the read side: a non-EOF socket error in `client_rx_relay` panics (`shard.rs:64`), and a
body read that fails after a good length prefix panics (`:71`). So a client that is killed
mid-frame — or a length prefix that does not match what follows — takes a shard down rather than a
connection ([item 16](../appendix/known-issues.md#16-panics-on-the-hot-path),
[item 34](../appendix/known-issues.md#34-the-request-length-prefix-is-unvalidated)).

**A truncated frame is indistinguishable from a slow one.** `read_exact` on the body waits for
exactly as many bytes as the prefix claimed, with no deadline
([TODOs](../appendix/todos.md#timeouts)), so a peer that sends a length and then stops leaves the
relay parked forever rather than failing. Only a socket close ends it — and then it panics rather
than returning.

**Nothing bounds a frame.** The length is used as an allocation size before a byte of the body has
been read, on both sides. The protocol has no maximum message size to check it against, which is
why this is a property of the format rather than a missing bounds check
([Limitations](#limitations)).

## Payloads

### `Queries<S>`

```rust
pub struct Queries<S: QuerySupport> {
    pub id: Uuid,
    pub queries: Vec<S::QueryKinds>,
    pub base_index: usize,
}
```

`shoal-core/src/shared/queries.rs:19-30`

`id` is the correlation key. The client generates it, ensures it is unique within its own
channel map (`client.rs:192-203`), and the server echoes it in the preamble of every
response.

`base_index` supports streaming. `ShoalQueryStream` sends several bundles under one id and
advances `base_index` by the number of queries already sent (`client.rs:1302-1303`), so
indices stay globally ordered across bundles.

`QueryKinds` is generated per database — one variant per table, wrapping that table's
`SortedQuery<T>` or `UnsortedQuery<T>` (`shoal-derive/src/structs/query_kinds.rs`).

### `ResponseKinds`

Also generated. It has one variant per table, mirroring `QueryKinds`, **and one per projection** —
a get that named a projection answers as that type rather than as its table's row type, so the
client can `access::<MovieSummary>()` it ([F2](../features/projections.md#design-choices)). An
archived enum is the size of its largest variant and every `Response<T>` archives to the same size
— a relative pointer and a length — so the extra variants cost nothing on the wire.

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

`shoal-core/src/shared/responses.rs:26-51`

Mutations return only a boolean. There is no error channel in the protocol: a failed insert
and a rejected insert are both `Insert(false)`, and a get that found nothing and a get
against a nonexistent partition are both `Get(None)`. Anything the server cannot express as a
boolean it expresses as a panic.

## Ordering and completion

Responses arrive in whatever order shards finish. Two fields make reassembly possible:

- **`index`** — the query's position in the logical stream, assigned by the coordinator as
  `index + base_index` (`shard.rs:534-541`).
- **`end`** — set on the last query of a bundle.

`ShoalResultStream` keeps `next_index` and a `BTreeMap<usize, ClientMsg>` of early arrivals,
returning responses strictly in index order (`client.rs:880-978`).
`ShoalUnorderedResultStream` returns them as they land, tracking a `BTreeSet` of seen indices
purely to know when the stream is complete (`client.rs:1160-1210`).

For streams the client sets `unbounded_queries: true` and ignores the server's `end` entirely,
terminating on a locally generated `ClientMsg::End(base_index)` sent by
`ShoalQueryStream::close` (`client.rs:1346-1350`). This is why the `end` computation being wrong
for streamed bundles never showed up in stream tests — the flag is simply not consulted on that
path. It is [fixed](../appendix/resolved/sorted-limit.md) now, but this path still does not
consult it.

## Alignment

rkyv requires archives to be aligned. The client allocates its read buffer as
`AlignedVec<16>` and reads the socket payload directly into it:

```rust
let mut aligned_buff = AlignedVec::<16>::with_capacity(len);
aligned_buff.resize(len, 0);
self.reader.read_exact(&mut aligned_buff).await?;
```

`shoal-core/src/client.rs:524-527`

That is what makes the response path zero-copy: `ShoalResponse` keeps the buffer and a
pointer into it, so accessing a row is a pointer cast rather than a parse.

The server does not do this. It reads requests into a `BytesMut`
(`shard.rs:66`), which carries no alignment guarantee, and then calls `Queries::access` —
which succeeds because rkyv's `access` validates and because in practice the allocator
returns suitably aligned memory. The request path also fully deserializes anyway
(`shard.rs:635`), so it gains nothing from alignment today.

## Validation

| Direction | Validation |
| --- | --- |
| Client → Server | `Queries::access` runs `bytecheck` validation before use (`shared/queries.rs:160`). |
| Server → Client | `ShoalResponse::new` runs `RkyvSupport::access`, also validated (`client.rs:753-769`). |

There is one deliberately unchecked path: `ShoalDatabase::unarchive_queries` uses
`rkyv::access_unchecked` (`shared/traits.rs:341-345`). It is not called on the live request
path, which goes through the validated `Queries::access`.

The length prefixes themselves are never validated. On the server, `BytesMut::zeroed(len)`
allocates whatever the peer claims (`shard.rs:66`); on the client,
`AlignedVec::with_capacity(len)` does the same (`client.rs:524`). A corrupt or hostile length
is an unbounded allocation on either side.

## Design notes

**Length-prefix plus rkyv, nothing else.** There is no envelope, no message type byte, and no
schema id. Both sides are compiled from the same schema, so the payload type is known
statically — the protocol carries no type information because it does not need to.

**The correlation id is per bundle, not per query.** Routing responses needs only the id;
ordering within the bundle is the `index` field's job. That keeps the preamble at 24 bytes
and lets the server stream responses as they complete.

**Vectored writes on both sides.** Header and payload are separate allocations that are never
concatenated, on either direction of the connection.

## Limitations

Every one of these is a consequence of the format having no envelope. What each would take to fix
is designed in [D2](../direction/framing.md), which proposes one 8-byte header carrying a version,
a message type, flags, and a bounded length — and one flag day rather than six.

- **No version field.** Client and server must be compiled from the same schema *and* the
  same rkyv layout. A mismatch is undefined behaviour caught, at best, by `bytecheck`
  ([D2](../direction/framing.md#the-schema-fingerprint) — the fingerprint, not the version byte,
  is what catches the schema half of this).
- **No authentication, no TLS.** Anything that can reach the port can read and write any
  table ([D3](../direction/authentication.md), [D4](../direction/encryption.md)).
- **No error responses.** `ResponseAction` has five variants and none carries an error
  ([item 56](../appendix/known-issues.md#56-a-response-cannot-say-that-a-read-failed)), so
  server-side failures are panics, which appear to the client as a closed connection. This is also
  what stops a read that failed being distinguishable from a read that found nothing
  ([D2](../direction/framing.md#the-error-channel)).
- **Little-endian assumed** for all length fields.
- **Unbounded allocation** from unvalidated length prefixes on both sides, and **no maximum message
  size** to validate one against ([D2](../direction/framing.md#the-header-itself)).
- **No deadline on a frame.** A peer that sends a length prefix and then stops parks the reader
  indefinitely ([D6](../direction/connection-pool.md#deadlines)).
- **No keepalive or ping.** The pool's health check calls `peer_addr()`, which does not
  detect a dead peer (`client.rs:76-91`, marked TODO)
  ([D6](../direction/connection-pool.md#health-checks-that-work)).

**What the transport itself is not.** TCP is a decision rather than an inheritance, and the case
for keeping it — including why QUIC's stream multiplexing is already implemented one layer up in
`channel_map`, and why its mandatory crypto would end the zero-copy read described under
[Alignment](#alignment) — is in [D1](../direction/transport.md).
