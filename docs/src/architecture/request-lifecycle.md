# Request Lifecycle

This is the page that makes the rest of the server legible. It follows a single insert and a
single get from the client's socket to the response, naming every hop.

## The whole path

```
 CLIENT                          SERVER (shard A = coordinator)      SERVER (shard B = owner)
 ──────                          ────────────────────────────        ────────────────────────
 Shoal::send(queries)
   rkyv::to_bytes ──────────┐
   pool.get()               │
   write_vectored           │
     [len][archive]  ═══════╪══════▶ client_rx_relay
                            │          read_exact(8) → len
                            │          read_exact(len) → RequestBody
                            │          ServerMsg::Client ──┐
                            │                              ▼
                            │                        shard loop
                            │                          handle_client
                            │                            RequestBody::freeze → Bytes
                            │                            Queries::access   (validates once)
                            │                          send_to_shard
                            │                            route_archived
                            │                              ring.find_shard(pkey)
                            │                            ServerMsg::Query ═════▶ shard loop
                            │                              {body, offset, keys}     handle_query
                            │                                                       unarchive_queries
                            │                                                       deserialize_query
                            │                                                       narrow_to(keys)
                            │                                                       tables.handle
                            │                                                         ↓
                            │                                              ┌──────────┴──────────┐
                            │                                     read path│                     │write path
                            │                                              ▼                     ▼
                            │                                        Some(response)    storage.commit
                            │                                              │           pending.add(pos)
                            │                                              │           returns None
                            │                                              │                     │
                            │                                              │        (later, when idle)
                            │                                              │           tables.flush()
                            │                                              │           StreamWriter::write
                            │                                              │              ↓ background task
                            │                                              │           ServerMsg::DataFlushed
                            │                                              │           DataFlushed (wakeup)
                            │                                              │           handle_flushed
                            │                                              │           pending.get(flushed)
                            │                                              ▼                     ▼
                            │                                            Shard::reply ◀──────────┘
                            │                                              rkyv::to_bytes
                            │                                              client_map[peer].send
                            │                                                     │
                            │  client_tx_relay ◀────────────────────────────────── ┘
                            │    write_vectored [uuid][len][archive]
   TcpProxy ◀═══════════════╪═══════════════════════════════════════════════════════
     read_exact(24)         │
     channel_map[uuid]      │
   ShoalResultStream::next ─┘
     reorder by index
```

## 1. Connection setup

When a client connects, the accepting shard mints a peer id and splits the socket:

```rust
let stream = tcp_sock.accept().await?;
stream.set_nodelay(true)?;
let client = Uuid::new_v4();                       // TODO: detect collisions?
let (tcp_rx, tcp_tx) = stream.split();
let (client_tx, client_rx) = kanal::unbounded_async();
glommio::spawn_local(client_rx_relay(client, tcp_rx, node_local_tx.clone())).detach();
glommio::spawn_local(client_tx_relay::<S>(client_rx, tcp_tx, hello.max_frame_bytes));
comms.broadcast(&ServerMsg::NewClient { client, client_tx }).await?;
```

`shoal-core/src/server/shard.rs:120-138`

The broadcast is the important part: **every** shard gets the sender for this client's socket
and stores it in `client_map` (`shard.rs:619-625`). That is what lets shard B reply directly
to a client whose socket belongs to shard A, without routing the response back through the
coordinator.

`set_nodelay(true)` disables Nagle's algorithm — Shoal writes complete messages and wants
them on the wire immediately rather than coalesced.

## 2. Reading the request

Before any of this, the connection shakes hands: the client sends a `Hello` naming the protocol
version, its schema fingerprint, the largest frame it will accept and the authentication mechanisms
it can do, and the server answers with a `HelloAck` that either agrees or refuses
([Wire Protocol](wire-protocol.md#the-handshake)). If that ack named a mechanism — which only
happens when the server's config asked for one — an
[authentication exchange](wire-protocol.md#the-authentication-exchange) follows and has to succeed
before anything below runs ([F12](../features/authentication.md)). All of it runs in the
per-connection task, before the stream is split, under one deadline.

```rust
let mut preamble = [0u8; protocol::REQUEST_PREAMBLE_LEN];
tcp_rx.read_exact(&mut preamble).await // EOF ⇒ client died, break
let header = protocol::decode_request(&preamble, max_frame_bytes)?; // else log and break
let wire_trace = read_trace_context(&mut tcp_rx, &header).await?; // None unless the flag is set
let payload_len = header.request_payload_len()?; // the body minus whatever the context took
let span = info_span!(parent: None, "Shoal::request", ..); // adopts wire_trace if there is one
let data = RequestBody::read_from(&mut tcp_rx, payload_len).await // else log and break
kanal_tx.send(ServerMsg::Client { peer, span, data, base }).await // else log and break
```

`client_rx_relay`, `shoal-core/src/server/shard.rs`

**The trace context is its own read, and the body length is not the frame length.** A client that
is tracing puts 26 bytes between the header and the payload
([F35](../features/wire-trace-context.md)), and they cannot go into the front of `RequestBody`'s
buffer: that buffer is sized to hold the archive exactly, so the archive starts at offset 0 and is
accessed in place. `header.body_len()` counts the context; `header.request_payload_len()` is what
the body actually is.

Clean EOF ends the loop, and so does everything else — a version this build does not speak, a
message type it does not know, a length past `max_frame_bytes`, a truncated body, or a shard
channel that has gone. All of them log and `break`, which ends this connection and leaves every
other client on the shard alone. **None of them panics**, which was not true before
[F10](../features/framing-and-protocol-evolution.md)
([Resolved #34](../appendix/resolved/unvalidated-length-prefix.md)).

When this loop ends, the task that owns it cancels the write relay, which drops the other half of
the split stream and closes the socket.

## 3. Coordinating: fan-out

```rust
let body = data.freeze();
let archived = Queries::access(&body)?;
self.send_to_shard(peer, &span, &body, archived, stamps).await
```

`shoal-core/src/server/shard.rs`, `Shard::handle_client`

`Queries::access` validates the archive with `bytecheck`, and **that is all the coordinator does
to the bundle** ([F26](../features/archive-routed-requests.md)). It does not deserialize it. The
body is frozen into a `Bytes` first so that the archive and every shard the bundle routes to
borrow the same buffer, and a clone of it is a refcount rather than a copy.

This is the one validation the bundle gets, and everything downstream depends on it having
happened — see [Executing](#4-executing).

Fan-out routes each query by shard and attaches metadata:

```rust
let end_index = base_index + last_offset;
for (offset, kind) in queries.queries.iter().enumerate() {
    let index = offset + base_index;
    let end = index == end_index;
    QueryKinds::route_archived(kind, &self.ring, &mut found);
    let gather = if found.len() > 1 { /* register, reply here */ } else { None };
    for (shard_info, keys) in found.drain(..) {
        let meta = QueryMetadata::new(client, bundle_id, index, end, gather.clone(), stamps);
        let msg = ServerMsg::Query { meta, body: body.clone(), offset, keys };
        self.comms.send(&shard_info.contact, msg).await?;
    }
}
```

`shoal-core/src/server/shard.rs`, `Shard::send_to_shard`

`route_archived` groups a query's partition keys by the shard that owns them and emits **one
message per shard, naming only that shard's keys**. Shards are deduplicated, so a shard owning
two of the keys gets one message naming both rather than the same query twice, and no shard is
asked about partitions it does not own.

Every field it reads is a `u64` or an `Option<usize>` sitting inline in the archive. It never
touches a row, a filter or a sort key, which is what lets a bundle of megabyte rows be routed for
the cost of its keys.

A shard is handed either `Some(keys)` or `None`, and the difference matters: `None` is not "every
key", it means **do not narrow**. Every write takes it, because a write names its partition in a
field the narrowing does not reach.

~~It is also where a sorted query's `sort_select` is normalized.~~ **That moved.** The coordinator
never deserializes a selection now, so normalization — a set of sort keys put in sort order and
deduplicated — happens in `narrow_to`, on the shard that executes the query. The reason it exists
at all is unchanged: a query arriving over the wire is deserialized straight into its struct
without meeting a constructor
([Sort keys were accepted and ignored](../appendix/resolved/sort-keys.md)). A range needs no
normalizing, since it is already an ordered pair ([F1](../features/sort-key-ranges.md)).

`QueryMetadata` carries the client id, the bundle id, the query's index within the bundle, an
`end` flag, ~~`Span::current()`~~ **the query's own `Coordinator::route` span**, and `gather`
(`server/messages.rs`). The span is passed in rather than read from the ambient context, and it is
opened once per **query** rather than once per bundle — reading `Span::current()` there took
`send_to_shard`'s function-level span, so every query in a batch shared one parent
([Resolved #89](../appendix/resolved/fragmented-query-traces.md)). Index and `end`
are how the client reassembles an ordered stream from responses that arrive out of order, which
is why the index is computed **once per query** rather than once per target shard — every shard
answering one query must answer it under the same index.

### Gathering a split query

The client is owed exactly one response per query index; `ShoalResultStream` advances
`next_index` once per response and parks out-of-order ones in a `BTreeMap` keyed by index, so a
second response at an index it has already passed is unreachable.

So when a query is split across more than one shard, the splitting shard registers a `Gather`
under `(query id, index)` and sets `meta.gather` to its own contact. Each executing shard sends
its share back as `ServerMsg::Gathered` instead of replying to the client. The splitting shard
merges the shares — a get is their union, an exists is their disjunction — puts the merged rows
back into the order the query named its partitions in, applies the query's limit to that union,
and replies once.

```rust
merged.order_by_partitions(&gather.partition_order);
if let Some(limit) = gather.limit {
    merged.truncate(limit);
}
```

`Gather` carries `partition_order` because the narrowed queries do not: routing hands each shard
only its own keys, so the order the client asked for exists nowhere else by the time the shares
come back. The coordinator reads that order out of the archive with `archived_partition_keys`,
which is one of the two things it still looks at per query. `order_by_partitions` is a **stable** sort by where each row's partition was
named, which leaves the sort-key order each shard produced within a partition untouched.

The two lines cannot be swapped. Truncating first keeps the rows that arrived first, which is
whichever shard was quicker — the defect this replaced
([26, 39](../appendix/resolved/partition-order.md)).

A query answered by one shard alone leaves `gather` as `None` and keeps the direct
shard-to-socket reply below, so the common path pays nothing for any of this.

The gather entry is only released when every shard has reported. A shard that dies mid-query
leaks it and the client waits forever, since there are no timeouts anywhere
([Known Issues #15](../appendix/known-issues.md#15-no-backpressure-anywhere)).

## 4. Executing

The owning shard receives `ServerMsg::Query`, and the first thing it does is turn its share of
the bundle back into a query ([F26](../features/archive-routed-requests.md)):

```rust
// SAFETY: the coordinator validated these exact bytes with `Queries::access` before
// sharing them, and a `Bytes` cannot be written to, so nothing has changed them since.
let archived = unsafe { D::unarchive_queries(body) };
let query = D::deserialize_query(&archived.queries[offset])?;
let query = match keys {
    Some(keys) => query.narrow_to(keys),
    None => query,
};
```

**This is the only copy a request pays for**, and it is paid here rather than on the coordinator.
Every `String`, `Vec` and filter the query carries is materialized on the shard that is about to
read them. It is stamped as its own stage, `query_decode`, which is why the stage report has
twenty spans rather than nineteen.

The read is unchecked, and the safety argument is the whole of why routing from the archive is
sound: the coordinator validated this buffer once and `Bytes` is immutable, so revalidating here
would mean each shard walking the *whole bundle* to reach one query in it. `unarchive_queries` is
an `unsafe fn` so that precondition cannot be dropped silently.

A query that was parked on a partition read comes back as `ServerMsg::Released` instead, carrying
the query itself — it was decoded and narrowed when it first arrived, and there is no bundle left
to read it out of.

Then it calls into the generated dispatch layer:

```rust
if let Some((addr, query_id, response)) = self.tables.handle(meta, query).await {
    match &gathered_meta.gather {
        // this is our share of a query someone else split, so send it back to them
        Some(contact) => self.comms.send(contact, ServerMsg::Gathered { .. }).await?,
        // this query was ours alone to answer
        None => self.reply(addr, query_id, span, response).await?,
    }
}
```

`shoal-core/src/server/shard.rs`

`tables.handle` is generated by `shoal-derive/src/traits/db.rs:55-75`: it matches the
`QueryKinds` variant, calls `handle` on the corresponding table field, and rewraps the result
in the matching `ResponseKinds` variant.

**The `Option` is the fork in the road.** `Some` means "answer now"; `None` means "no answer
yet", for one of two reasons:

| Reason | Where it resumes |
| --- | --- |
| The write must be durable before it can be acknowledged | `handle_flushed`, below |
| The partition must be read from disk first | `ServerMsg::Partition`, [Query Execution](../tables/query-execution.md) |

### Read path

A get against a resident partition returns immediately. Rows are filtered and cloned into a
`Vec<R>`, wrapped in `ResponseAction::Get`, and returned as `Some`
(`tables/persistent/sorted.rs:436-524`).

### Write path

An insert writes to the intent log first, then to memory:

```rust
let intent = SortedIntents::Insert(row);
let pos = self.storage.commit(&intent).await.unwrap();
let row = match intent { SortedIntents::Insert(row) => row, _ => unsafe { unreachable_unchecked() } };
...
self.pending.add(meta, pos, action);
None            // an insert never returns anything immediately
```

`shoal-core/src/server/tables/persistent/sorted.rs:333-377`

Note the shape: the row is wrapped in an intent, serialized, then *unwrapped again* to avoid
a clone — hence the `unreachable_unchecked`.

`pos` is the intent log offset one past this record. `pending.add` files the response against
it, and the response is released once the log has been fdatasynced that far. See
[Durability model](../storage/overview.md#durability-model).

## 5. Flushing and releasing responses

At the bottom of every iteration of the shard loop:

```rust
// if we have no more messages then flush our current queries to disk
if self.shard_local_rx.is_empty() {
    self.tables.flush().await?;
}
// sweep our tables only when that sweep could do something
if self.data_flushed || self.tables.compaction_due() {
    // check for any flushed response to handle
    self.handle_flushed().await?;
}
// check if we need to evict any data
if *self.memory_usage.borrow() > self.conf.resources.memory {
    self.evict_data().await?;
}
```

`shoal-core/src/server/shard.rs`

**The sweep is gated, the flush is not.** `handle_flushed` used to run unconditionally here and
`data_flushed` did not exist; [F5](../features/flushed-sweep-gate.md) put it behind the two
conditions that are the only things which can make it do work — a write landing, and a log growing
past the size it rotates at. `tables.flush()` above it stays unconditional, because going idle is
exactly when the partial buffer has to go out.

**Flush-when-idle is the core write optimisation.** Under load the queue is never empty, so
writes accumulate in the `StreamWriter`'s DMA buffer and go out in full-buffer batches. When
the shard goes quiet, the partial buffer is flushed so a lightly loaded system does not
stall. Batching is free and adaptive; no timer is involved.

The completion path is asynchronous. `StreamWriter::write` spawns a detached task
(`.../fs/stream.rs:189-194`) which, on completion, posts `ServerMsg::DataFlushed` back to the
shard (`.../fs/stream.rs:109-115`). The watermark itself lives in `FlushState` and is advanced by
the completion rather than by the message, so the message is only a wakeup — but since F5 it is a
*required* one, because it is what opens the gate above. `handle_flushed` then pops every pending
response at or below the watermark:

```rust
self.tables.handle_flushed(&mut self.flushed).await?;
while let Some((client, query_id, span, response)) = self.flushed.pop() {
    self.reply(client, query_id, span, response).await?;
}
```

`shoal-core/src/server/shard.rs:543-548`

`self.flushed` is drained with `pop()` — from the back — so acknowledgements are sent in
reverse order of completion. Harmless, since the client reorders by index, but surprising.

## 6. Replying

```rust
let archived = rkyv::to_bytes::<_>(&response)?;
match self.client_map.get(&client) {
    Some(client_tx) => client_tx.send((query_id, span, archived)).await?,
    None => panic!("{} Missing client channel? {client}", self.info.name),
}
```

`shoal-core/src/server/shard.rs:489-494`

The `Span` travels with the response so `client_tx_relay` can re-enter it while writing, and
the write itself is vectored to avoid concatenating the header and payload:

```rust
let mut bufs = &mut [
    IoSlice::new(query_id.as_bytes()),
    IoSlice::new(&len),
    IoSlice::new(&archived),
][..];
while !bufs.is_empty() {
    match tcp_tx.write_vectored(bufs).await {
        Ok(0) => { /* log and end this connection */ }
        Ok(n) => IoSlice::advance_slices(&mut bufs, n),
        Err(error) => { /* log and end this connection */ }
    }
}
```

`shoal-core/src/server/shard.rs:93-108`

## 7. Client demultiplexing

The client runs one `TcpProxy` per pooled connection. Each reads a 24-byte preamble — eight bytes
of header and then the query id — looks that id up in a shared concurrent map, and forwards the
payload. The response preamble is fixed size in a way the *request* preamble no longer is, because
nothing about tracing travels back this way: the entry it looks up carries the span the query was
sent in, which is how the answer rejoins the trace that asked for it
([F35](../features/wire-trace-context.md)).

```rust
let mut preamble = [0u8; protocol::RESPONSE_PREAMBLE_LEN];
self.reader.read_exact(&mut preamble).await
let frame = protocol::decode_response(&preamble, self.max_frame_bytes)?;
let mut aligned_buff = AlignedVec::<16>::with_capacity(frame.payload_len);
aligned_buff.resize(len, 0);
self.reader.read_exact(&mut aligned_buff).await?;
match self.channel_map.pin_owned().get(&query_id) {
    Some(tx) => tx.send(ClientMsg::Response(aligned_buff)).await?,
    None => return Err(Errors::ProtocolError(...)),
}
```

`shoal-client/src/client.rs:1485-1497`, `TcpProxy::read_frame`

Reading straight into an `AlignedVec<16>` is what makes the response path genuinely
zero-copy: the buffer is aligned for rkyv, so `ShoalResponse` can hold a pointer into it and
hand out `&Archived<T>` without a copy or a parse ([The Client](../api/client.md)).

Finally, `ShoalResultStream::next` reorders by index, holding out-of-order responses in a
`BTreeMap` until their turn (`client.rs:862-955`).

## Design notes

**Two asynchrony mechanisms, one shape.** Waiting on durability and waiting on a disk read
are both expressed as "return `None`, resume later on a `ServerMsg`". Keeping them uniform is
why the shard loop stays a flat `match`.

**Responses bypass the coordinator.** Because `NewClient` is broadcast, the owning shard
writes to the client's socket channel directly. The coordinator is on the request path only.

**Tracing spans are threaded through by hand.** `QueryMetadata` carries a `Span`, `reply` takes
one, and `client_tx_relay` opens a child of it around the socket write — so a trace spans the whole
lifecycle including the asynchronous flush. ~~`client_tx_relay` enters it~~ — entering it put the
write *under* the span for anything reading `Span::current()` and produced no span at all for
anything reading the trace, which is one of four gaps in
[Resolved #89](../appendix/resolved/fragmented-query-traces.md). The root of that trace is opened
in `client_rx_relay` when the frame lands, so the lifecycle this page describes and the trace of it
now start in the same place; see
[Observability](../operations/observability.md#how-one-query-stays-one-trace) for the whole shape.

## Limitations

- ~~The request path deserializes and then clones per shard; it is not zero-copy.~~ **The clone is
  gone and the deserialize moved** ([F26](../features/archive-routed-requests.md)). The coordinator
  routes from the archive without deserializing anything, and each shard deserializes only its own
  query — so the bundle is walked once rather than once plus a clone per destination, and that walk
  happens on the shard that will read the row rather than on the single coordinator every request
  passes through. The request path is still not *zero*-copy: a query is materialized once, and
  [TODOs](../appendix/todos.md) has what executing against the archive would take.
  **Every copy on this path is O(bytes), and there are about six of them per round trip** — ~~the
  zeroed request buffer,~~ ~~the bundle deserialization,~~ the per-query deserialization, the row
  copied into a partition and out of one, the intent log's serialize/checksum/copy, and the
  response serialization. The request buffer's zeroing came off this list with
  [F25](../features/read-buffers-are-filled-not-zeroed.md), along with the client's matching one;
  the kernel copy that fills it is still here and always will be. F26 did not remove an item from
  the list so much as shrink one and move it off core 0 — except on the write path, where it
  removed the second copy of every inserted row outright. None of that is
  visible at a 64 byte row and it is most of the cost at 4 MiB; see
  [Row size and what it costs](../tables/row-size.md#the-payload-is-walked-about-six-times-per-round-trip).
  **That list is a read/write mixture**, and three of its items are inside `FileSystem::commit`,
  which a read never enters. A *get* walks the payload seven times, two of them kernel copies —
  the same page now traces the read path hop by hop, and three of those hops went unfiled until it
  was traced.
- **The response relay is serial per connection.** `client_tx_relay` writes one response to
  completion before starting the next, so a wide response blocks every narrow one queued behind it
  on that socket ([O35](../appendix/optimizations.md)).
- ~~The length prefix is unvalidated, so a bad length is an unbounded allocation.~~ Bounded by
  `max_frame_bytes` since [F10](../features/framing-and-protocol-evolution.md).
- ~~Socket and channel errors are panics rather than per-connection teardown.~~ Both relays tear
  down the connection now; the panics elsewhere in the server are
  [item 16](../appendix/known-issues.md#16-panics-on-the-hot-path).
- ~~A response too large to frame closes the connection with nothing on the wire saying why.~~
  Fixed by [F11](../features/error-channel.md): the relay writes an `Error` frame naming the query
  and both sizes, and keeps serving every other query on that connection
  ([Resolved #56, 61](../appendix/resolved/response-error-channel.md)).
- `end` is computed incorrectly for streamed bundles, and underflows on empty ones.
- Reordering the gathered rows rehashes each row's partition key, since a response carries rows
  and not the partition they came from ([Optimizations](../appendix/optimizations.md#o18-the-gathered-reorder-rehashes-every-rows-partition-key)).
