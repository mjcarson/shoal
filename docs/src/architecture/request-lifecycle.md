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
                            │          read_exact(len) → BytesMut
                            │          ServerMsg::Client ──┐
                            │                              ▼
                            │                        shard loop
                            │                          handle_client
                            │                            Queries::access
                            │                            deserialize
                            │                          send_to_shard
                            │                            ring.find_shard(pkey)
                            │                            ServerMsg::Query ═════▶ shard loop
                            │                                                     handle_query
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
glommio::spawn_local(client_tx_relay::<S>(client_rx, tcp_tx)).detach();
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

```rust
let mut len_bytes: [u8; 8] = [0; 8];
tcp_rx.read_exact(&mut len_bytes).await // EOF ⇒ client died, break
let len = u64::from_le_bytes(len_bytes) as usize;
let mut data = BytesMut::zeroed(len);
tcp_rx.read_exact(&mut data).await.unwrap();
kanal_tx.send(ServerMsg::Client { peer, data }).await.unwrap();
```

`shoal-core/src/server/shard.rs:51-74`

Clean EOF is handled; every other socket error is a `panic!`
(`shard.rs:58-62`). The length prefix is trusted completely — `BytesMut::zeroed(len)`
allocates whatever the peer asked for, so a malicious or corrupt length is an unbounded
allocation.

## 3. Coordinating: fan-out

```rust
let archived = Queries::access(&data)?;
let queries = <Queries<D::ClientType> as RkyvSupport>::deserialize(archived)?;
self.send_to_shard(peer, queries).await
```

`shoal-core/src/server/shard.rs:466-470`

`Queries::access` validates the archive with `bytecheck` (`shared/queries.rs:89-102`), then
`deserialize` **fully materialises the bundle**. On a branch named `ZeroCopyResponses` this is
worth noticing: the request path is not zero-copy. Every query is deserialized here and then
narrowed into one owned query per target shard.

Fan-out splits each query by shard and attaches metadata:

```rust
let end_index = queries.base_index + query_count;
for (index, kind) in queries.queries.into_iter().enumerate() {
    let index = index + queries.base_index;
    let end = index == end_index;
    kind.split_by_shard(&self.ring, &mut found);
    let gather = if found.len() > 1 { /* register, reply here */ } else { None };
    for (shard_info, query) in found.drain(..) {
        let meta = QueryMetadata::new(client, queries.id, index, end, gather.clone());
        self.comms.send(&shard_info.contact, ServerMsg::Query { meta, query }).await?;
    }
}
```

`shoal-core/src/server/shard.rs`

`split_by_shard` groups a query's partition keys by the shard that owns them and emits **one
query per shard, naming only that shard's keys**. Shards are deduplicated, so a shard owning two
of the keys gets one query naming both rather than the same query twice, and no shard is asked
about partitions it does not own.

It is also where a sorted query's `sort_select` is normalized — a set of sort keys put in sort
order and deduplicated — once, for the same reason: this is the one place every query passes
through no matter who built it, and a query arriving over the wire is deserialized straight into
its struct without meeting a constructor
([Sort keys were accepted and ignored](../appendix/resolved/sort-keys.md)). A range needs no
normalizing, since it is already an ordered pair ([F1](../features/sort-key-ranges.md)).

`QueryMetadata` carries the client id, the bundle id, the query's index within the bundle, an
`end` flag, `Span::current()` for tracing, and `gather` (`server/messages.rs`). Index and `end`
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

`Gather` carries `partition_order` because the narrowed queries do not: `split_by_shard` hands
each shard only its own keys, so the order the client asked for exists nowhere else by the time
the shares come back. `order_by_partitions` is a **stable** sort by where each row's partition was
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

The owning shard receives `ServerMsg::Query` and calls into the generated dispatch layer:

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
        Ok(0) => panic!("No bytes were written?"),
        Ok(n) => IoSlice::advance_slices(&mut bufs, n),
        Err(error) => panic!("Ahhh error?: {error:#?}"),
    }
}
```

`shoal-core/src/server/shard.rs:93-108`

## 7. Client demultiplexing

The client runs one `TcpProxy` per pooled connection. Each reads a 24-byte preamble, looks up
the query id in a shared concurrent map, and forwards the payload:

```rust
let mut preamble: [u8; 24] = [0; 24];
self.reader.read_exact(&mut preamble).await
let query_id = Uuid::from_slice(&preamble[..16])?;
let len = u64::from_le_bytes(preamble[16..24].try_into()?) as usize;
let mut aligned_buff = AlignedVec::<16>::with_capacity(len);
aligned_buff.resize(len, 0);
self.reader.read_exact(&mut aligned_buff).await?;
match self.channel_map.pin_owned().get(&query_id) {
    Some(tx) => tx.send(ClientMsg::Response(aligned_buff)).await?,
    None => return Err(Errors::ProtocolError(...)),
}
```

`shoal-core/src/client.rs:504-539`

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

**Tracing spans are threaded through by hand.** `QueryMetadata` carries a `Span`, `reply`
takes one, and `client_tx_relay` enters it around the socket write — so a trace spans the
whole lifecycle including the asynchronous flush.

## Limitations

- The request path deserializes and then clones per shard; it is not zero-copy.
- The length prefix is unvalidated, so a bad length is an unbounded allocation.
- Socket and channel errors are panics rather than per-connection teardown.
- `end` is computed incorrectly for streamed bundles, and underflows on empty ones.
- Reordering the gathered rows rehashes each row's partition key, since a response carries rows
  and not the partition they came from ([Optimizations](../appendix/optimizations.md#o18-the-gathered-reorder-rehashes-every-rows-partition-key)).
