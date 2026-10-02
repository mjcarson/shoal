# S12. The wire and the client

## Context

A client reaches an object by its path (R9), reads and writes it at any offset (R12), and
the object may be larger than any buffer either peer holds (R13). The client's wire today
carries one whole frame a request and one a response, each read into memory before it is
used, and each a rkyv archive.

This page is what the wire gains so that an object's bytes cross it in bounded frames that
are never an archive, and what a caller sees: a handle on a bucket, with a file's operations
on it.

## What exists today

- **A frame is eight bytes of header and a body**: version, type, two flag bytes and a
  32-bit length ([Wire Protocol](../architecture/wire-protocol.md#the-header)). There are
  twenty-six message types (`shoal-proto/src/shared/protocol.rs:172-235`). The flag `LAST`,
  "This frame is the last one for its query", is reserved and nothing sets it (`:351`).
- **A body is read whole.** The server allocates the body's length and fills it before
  anything is routed (`shoal-core/src/server/request_body.rs:55-74`). A frame is bounded at
  64 MiB (`protocol.rs:154`), and a client's hello always offers exactly that
  (`shoal-client/src/client.rs:558`), so a response over 64 MiB is answered with
  `ResponseTooLarge` whatever the server is configured to
  (`shoal-core/src/server/shard.rs:784`).
- **An unknown type ends the connection.** A request that is not a bundle of queries is
  handed to `read_control_frame`, which knows a topology subscription and an admin
  operation and refuses anything else as `UnexpectedMessageType` (`shard.rs:457-493`). The
  client's reader is as strict (`client.rs:2368-2417`).
- **One capability bit is spent.** `CLIENT_CAP_READ_OPTIONS` is bit zero of a byte in the
  hello (`shoal-proto/src/shared/protocol/read.rs:63`). The client lane is otherwise exact
  at `CLIENT_WIRE_VERSION`, 4.
- **A connection is shared.** The client holds a pool of ten to fifty connections
  (`shoal-client/src/client/builder.rs:63-64`), a reader task on each matches frames to
  waiters by the bundle's id, and any bundle may travel on any of them.
- **The client's operations are bundles of queries**: `send`, `stream`, `stream_unordered`
  and `exec` (`client.rs:1410`, `:2084`, `:2137`, `:1792`). It links no engine
  ([F15](../features/client-server-split.md)).
- **`Cancel` is type 12, reserved and unwired**
  ([todos](../appendix/todos.md#cancel-and-what-it-would-actually-buy)).
- **Encryption is the kernel's.** rustls does the handshake and hands the keys to kTLS, so
  the bytes this page describes are plaintext to the process either way
  ([F14](../features/encryption-in-transit.md)).

## The design

### The operations

| Operation | Does | Atomic |
| --- | --- | --- |
| `stat(path)` | Returns size, times and the user map | — |
| `read_at(path, offset, length)` | Reads a range | Each stripe at one committed state ([S9](read-path.md)) |
| `write_at(path, offset, bytes)` | Writes in place, extending if it passes the end | For each stripe ([S7](write-path.md#writes-that-span-stripes)) |
| `truncate(path, length)` | Cuts or extends with zeros | The cut is ([S3](objects.md#size-holes-and-truncate)) |
| `put(path, stream)` | Replaces the whole object | Yes: readers see the old or the new |
| `get(path)` | Reads the whole object as a stream | As `read_at` |
| `delete(path)` | Removes the object | Yes |
| `open(path)` | Returns a handle that reads, writes and seeks | As the calls it makes |

There is no list, by the decision of 2026-10-02, and no rename.

### Three message types and a capability bit

| Type | Direction | Carries |
| --- | --- | --- |
| `ObjectOp` | client to server | A fixed header: the bucket's id, the operation, offset, length, the read level, the caller's identity; then the path |
| `ObjectData` | both | The operation's id, an offset, and bytes. `LAST` on the final one |
| `ObjectAnswer` | server to client | The operation's id and its result: a stat, a count of bytes, or a typed refusal |

They take the next discriminants, 27 to 29. Because an unknown type closes a connection, a
client sends them only to a server whose hello ack granted a new capability bit, the way
read options are gated today. That makes the feature an addition and not a new client wire
version: no existing frame changes.

**An `ObjectData` body is bytes.** It is not an archive, it is not validated as one, and
neither peer walks it. That is the constraint the overview states, applied: the reason a
large value is not a row is that a row's payload is touched about six times on a round
trip.

### Ranged frames

A frame of object bytes is bounded, at 1 MiB proposed, far under the frame bound. Nothing
this page adds ever asks either peer to hold an object, or a stripe, in one buffer.

- **A write** is an `ObjectOp` and then as many `ObjectData` frames as its bytes need. The
  server stages as it reads ([S7](write-path.md)) and stops reading the connection while
  what it holds for that write is over a bound, which is how a connection owing too many
  answers is treated today ([C2](../distributed/transport.md#backpressure)). One
  `ObjectAnswer` ends it.
- **A read asks for a range and is answered with that range.** A long read is a sequence of
  ranges, each asked for when the caller wants it, with a few asked for ahead. The server
  never sends what was not asked for.

The second point is why `Cancel` is optional ([S1](prerequisites.md#optional)). A reader
that seeks away or drops its handle stops by not asking for the next range; what it cannot
take back is the range in flight, which is bounded by the window.

**A frame carries whole stripe units** where it can, and is read straight into a buffer
aligned for direct I/O, so that the bytes a holder stages on this node are the bytes that
came off the socket. Whether the checksum a client computes over a unit can be the one the
device stores, so that a unit is checksummed once from the caller to the disk, is part of
[Q21](contract.md#questions-to-answer).

### Sharing a connection with queries

An object frame of 1 MiB ahead of a small query's answer on the same connection delays it
by the megabyte. Two things bound that: the frame's size, and the client's choice of
connection. The client may keep some of its pooled connections for object bytes alone.
Whether it should, and what a shared connection really costs a small query's tail, is
[X11](spikes.md#x11-streamed-bodies)'s to measure before it is designed
([Q26](contract.md#questions-to-answer)).

### The client's handle

```rust
/// A handle to one bucket of a database
pub struct Bucket<'a, S: QuerySupport, B: BucketSupport<S>> {
    /// The client this bucket is reached through
    client: &'a Shoal<S>,
    /// The bucket this handle names
    marker: PhantomData<B>,
}
```

Its methods are the operations in the table above, each taking the path first:
`read_at(path, offset, &mut buf)`, `write_at(path, offset, &bytes)`, `truncate(path, length)`,
`put(path, stream)`, `get(path)`, `stat(path)`, `delete(path)` and `open(path)`.

```rust
// reach the bucket our schema declared
let posters = client.bucket::<Posters>();
// replace a whole object from a stream
posters.put("alien/one-sheet.png", file).await?;
// patch four kibibytes of it in place
posters.write_at("alien/one-sheet.png", 4096, &patch).await?;
// open it as a file
let mut poster = posters.open("alien/one-sheet.png").await?;
// seek a mebibyte in, which sends nothing
poster.seek(SeekFrom::Start(1 << 20)).await?;
// read from there
poster.read_exact(&mut buf).await?;
```

`ObjectFile` implements tokio's `AsyncRead`, `AsyncWrite` and `AsyncSeek`. A seek is
arithmetic on the client and sends nothing.

Every operation that changes anything travels under an identity, as `exec` gives a bundle
one today, so a retry after a lost answer is the same write and not a second one
([S7](write-path.md#writes-that-span-stripes)). A read may name a read level and a session
token, as a query may.

### What the client does not do

It does not hold a pool map, choose a device, encode or talk to a holder. A node
coordinates every operation. That keeps the client the thin thing it is and costs a network
crossing on every byte ([S9](read-path.md#where-the-bytes-travel)).

It is not precluded. The frames between nodes that stage and read pieces
([S13](isolation.md#a-lane-for-object-bytes)) are designed so that a client could one day
send them, and the day is after [D7](../direction/shard-aware-routing.md) and after a
measurement says the crossing is worth removing.

## Alternatives rejected

**One frame an object.** It is what exists, and it stops at 64 MiB and at whatever memory
either peer has.

**A body that is an archive.** rkyv would validate and walk bytes that have no structure.

**An HTTP or S3 endpoint as the interface.** S3 compatibility is not asked for, and a
second listener with its own authentication, encryption and framing is a second client
wire. A gateway that speaks S3 to callers and this protocol to a node is the way to add it
later, and nothing here stands in its way.

**A stream the server pushes until told to stop.** It needs `Cancel` wired and credits
counted on both sides. A range the client asks for needs neither.

**A separate port for objects.** It would keep big frames away from small ones for certain,
and it doubles what has to be listened on, encrypted, authenticated and rolled. A
connection set aside in the client's connection pool gets most of the benefit.

**Client-side placement from the start.** See above.

## What it costs

- **Three message types and a capability bit**, and a server reader that is no longer "a
  bundle, or a control frame".
- **A second kind of body**, read into aligned buffers and never validated.
- **A window of memory for each stream**, on both peers ([S13](isolation.md#memory)).
- **A small query's tail** on a connection it shares with object frames, until X11 says
  what that is.
- **The client grows**: a handle type, a stream's state, and the retry of a write that
  spans frames.

## What it breaks

- "One frame per response, not one per bundle"
  ([Wire Protocol](../architecture/wire-protocol.md#server--client)): a read is answered by
  many.
- "Every body is an archive or a fixed layout": `ObjectData` is neither.
- "The client's operations are bundles of queries."
- "A response over the client's frame bound fails": still true of a query's, and no longer
  a ceiling on what can be read.

## Invariants to uphold

- A frame of object bytes is bounded, and no peer holds more of one stream than its window.
- Object bytes are never an archive and are never walked by a validator.
- An object message type is sent only to a peer that granted the capability.
- The server sends no range that was not asked for.
- Every operation that changes an object carries an identity, and its retry is the same
  operation.
- The client half links no engine and no erasure code.

## Prerequisites

[S1](prerequisites.md#required): more than one frame for one query. [S2](buckets.md) for the
generated client half. `Cancel`, only if ranges turn out not to be enough
([S1](prerequisites.md#optional)).

## How it would be measured

[X11](spikes.md#x11-streamed-bodies): megabytes a second through one connection, plaintext
and under kTLS, at frame sizes from 64 KiB to 8 MiB; the memory one stream holds; what a
small query's tail does on a connection carrying object frames and on one that is not; and
whether a connection can be handed to the executor that owns the device its bytes are for.
Loopback on europa for what a core costs; across the lab for what 1 GbE allows, labelled as
the network's number and not the design's.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `object_frames_are_refused_without_the_capability` | A client that was not granted the bit sends no object type, and a server that receives one from such a client closes that connection alone | M13 |
| `a_stream_holds_no_more_than_its_window` | A write and a read of an object far larger than the window complete with either peer's memory for the stream bounded | M15 |
| `a_seek_reads_only_what_it_asked_for` | A read at an offset deep in a large object transfers that range and no other | M15 |
| `retried_write_across_frames_is_the_same_write` | A connection cut in the middle of a write's frames, and the write retried under its identity, leaves the object as one write would | M13 |
| `malformed_object_frame_ends_one_connection` | A frame with a length, an offset or a type that cannot be right closes its connection and leaves every other client answered | M13 |

## Related

[S7](write-path.md) and [S9](read-path.md) for what an operation does on the server;
[S13](isolation.md) for the frames between nodes and the memory a stream holds;
[S2](buckets.md) for the generated half; [Wire Protocol](../architecture/wire-protocol.md)
and [The Client](../api/client.md) for what exists; [D7](../direction/shard-aware-routing.md)
for the client that routes.
