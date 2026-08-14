# The Client

`shoal-core/src/client.rs` — the whole client, 1300 lines. Unlike the server it runs on
tokio, not glommio, so it is an ordinary async library.

## Construction

```rust
let client = Shoal::<TestDbClient>::new("127.0.0.1:12000").await?;
```

Against a server that requires authentication ([F12](../features/authentication.md)) there is a
second constructor, which is the only difference at the call site — everything after it is
identical:

```rust
let client = Shoal::<TestDbClient>::with_credentials(
    "127.0.0.1:12000",
    Credentials::scram("reader", "hunter2"),
)
.await?;
```

`Shoal::new` is `with_credentials` with `Credentials::none()`, and both delegate to one private
`connect` so that the ten line `where` clause exists once. Credentials given to a server that
requires nothing are ignored rather than used — the server picks the mechanism, so a client that
holds them still works against every server that has not turned authentication on.

The type parameter is the generated `*Client` marker implementing `QuerySupport`
([Derive Macros](derive-macros.md#step-2-emit-the-supporting-types)), which is what ties the
client to a specific schema.

```rust
pub struct Shoal<S: QuerySupport> {
    pool: bb8::Pool<ShoalConnectionManager>,
    pub channel_map: Arc<HashMap<Uuid, AsyncSender<ClientMsg>>>,   // papaya
    channel_queue_tx: AsyncSender<(AsyncSender<ClientMsg>, AsyncReceiver<ClientMsg>)>,
    channel_queue_rx: AsyncReceiver<(AsyncSender<ClientMsg>, AsyncReceiver<ClientMsg>)>,
    is_shutting_down: Arc<AtomicBool>,
    proxy_handle: JoinHandle<()>,
    phantom: PhantomData<S>,
}
```

`shoal-core/src/client.rs:93-108`

## Split connections

The client's central trick: **write halves go in the pool, read halves go to a proxy.**

```rust
async fn connect(&self) -> Result<Self::Connection, Self::Error> {
    let stream = TcpStream::connect(&self.server_addr).await?;
    stream.set_nodelay(true)?;
    let (tcp_rx, tcp_tx) = stream.into_split();
    self.proxy_tx.send(tcp_rx).await...;
    Ok(tcp_tx)      // only the write half is pooled
}
```

`shoal-core/src/client.rs:63-74`

```
   Shoal::send  ──▶ pool.get() ──▶ OwnedWriteHalf ──▶ socket
                                                        │
   ShoalTcpProxy ◀── proxy channel ◀── OwnedReadHalf ◀───┘
        │
        │ spawns one TcpProxy task per connection
        ▼
   channel_map[query_id] ──▶ ShoalResultStream
```

The consequence is that **responses are not tied to the connection that sent the request.**
A bundle may go out on connection 3 and its responses arrive on connection 3's read half, but
the demultiplexing is by query id through a shared map, so the streams do not care. This is
what lets the pool hand out connections freely.

Pool settings:

| Setting | Value |
| --- | --- |
| `min_idle` | 10 |
| `max_size` | 50 |
| `connection_timeout` | 5 s |
| `idle_timeout` | 300 s |
| `max_lifetime` | 1800 s |

`shoal-core/src/client.rs:140-148`

Health checking is nominal:

```rust
async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
    // TODO implement a ping/pong type request?
    conn.peer_addr()?;
    Ok(())
}
fn has_broken(&self, conn: &mut Self::Connection) -> bool {
    conn.peer_addr().is_err()
}
```

`shoal-core/src/client.rs:76-90`

`peer_addr()` reads local socket state; it does not probe the peer. A server that has gone away
without closing the socket will not be detected.

## Query ids and channel reuse

Every result stream registers a query id in the shared map:

```rust
let (tx, rx) = match self.channel_queue_rx.try_recv()? {
    Some((tx, rx)) => (tx, rx),
    None => kanal::unbounded_async(),
};
loop {
    if self.channel_map.pin().get(&*query_id).is_none() {
        self.channel_map.pin().insert(*query_id, tx.clone());
        break;
    }
    *query_id = Uuid::new_v4();
}
```

`shoal-core/src/client.rs:183-206`

Two things going on. Channel pairs are recycled through `channel_queue` — a finished stream
returns its channels (`client.rs:1012-1015`) rather than dropping them, so a high-throughput
client stops allocating. And ids are checked for collision and regenerated, so a UUIDv4
collision cannot silently cross two streams' responses.

`channel_map` is a `papaya::HashMap`, a lock-free concurrent map, because the proxy tasks read
it from tokio worker threads while the application registers and removes entries.

Note the check-then-insert is not atomic — two threads could pass the `get` before either
`insert`s. Two independently generated v4 UUIDs colliding makes this unreachable in practice.

## Sending

### One-shot

```rust
pub async fn send(&self, mut queries: Queries<S>) -> Result<ShoalResultStream<S>, Errors>
```

`shoal-core/src/client.rs:209-250`

Archive, register the id, grab a connection, write vectored, return a stream. Note the archive
is built *before* the id is finalised by `track_response` — if `track_response` regenerates the
id on collision, the archived bytes carry the old one. Unreachable in practice, but the
ordering is wrong.

Convenience wrappers:

| Method | Behaviour |
| --- | --- |
| `send_one(query)` | One query, one response, checks success (`client.rs:313-344`) |
| `exec(queries)` | Drains the stream, collects failures into `Errors::BulkError` (`client.rs:265-301`) |
| `exists(query)` | Returns `bool`; missing data is not an error (`client.rs:360-401`) |

`exists` is constrained by a marker trait so only exists queries can be passed:

```rust
pub trait ExistsQuery {}
pub async fn exists<Q: ExistsQuery + Into<S::QueryKinds>>(&self, query: Q) -> Result<bool, Errors>
```

`shoal-core/src/shared/traits.rs:40-44`, `client.rs:360-363`

A small, effective use of the type system: the response-kind mismatch this would otherwise
produce is a compile error rather than a runtime `UnexpectedResponseKind`.

### Streaming

```rust
let (mut query_stream, mut result_stream) = client.stream()?;
```

`stream()` and `stream_unordered()` (`client.rs:404-464`) return a pair sharing one query id.
Queries can be pushed indefinitely; `ShoalQueryStream::send` advances `base_index` by the
number of queries sent so indices stay globally ordered across bundles
(`client.rs:1270-1305`).

Both set `unbounded_queries: true`, which makes the result stream **ignore the server's `end`
flag** and terminate only on `ShoalQueryStream::close`, which posts a local
`ClientMsg::End(base_index)` directly into the response channel (`client.rs:1308-1313`). This
is why the server's `end` computation being wrong for streamed bundles never surfaced in
streaming tests — the flag was simply not consulted on that path. It is
[fixed](../appendix/resolved/sorted-limit.md) now, but nothing on this path depends on it.

## Result streams

| Type | Guarantee | State |
| --- | --- | --- |
| `ShoalResultStream` | Strict index order | `BTreeMap<usize, ClientMsg>` of early arrivals |
| `ShoalUnorderedResultStream` | As they arrive | `BTreeSet<usize>` of seen indices, for completion only |

Ordered reassembly (`shoal-core/src/client.rs:880-978`): if the head of `pending` is `next_index`,
pop and return it; otherwise wait for the next message, return it if it is `next_index`, else stash
it.

The unordered stream returns everything immediately and tracks `next_index` only to know when
the stream is complete (`client.rs:1160-1210`) — it advances `next_index` past every
contiguous run of seen indices so it can recognise the end.

**Memory:** an ordered stream holds every out-of-order response until the gap fills. One slow
partition blocks the stream and buffers everything behind it. The unordered stream exists
precisely to avoid that, at the cost of ordering.

### The reorder buffers

This is the only client state that spans responses, it is the part most likely to be wrong, and it
has [no test at all](../appendix/test-coverage.md#the-streaming-client-apis). So what makes it
sound is worth writing down separately from what it does — three facts hold it up, and none of them
is local to the code that depends on them.

**One query index yields exactly one response.** `pending` is a `BTreeMap<usize, ClientMsg>` keyed
by the response's index, and `insert` on a `BTreeMap` overwrites. That would silently drop a
response if two ever shared an index. They cannot: `ResponseAction` has no variant that a query
answers more than once — a get answers `Get(Option<Vec<T>>)` with every row it found, not a row at a
time, and the `Error` variant [F11](../features/error-channel.md) added is applied *in place of*
whatever a query answered rather than alongside it, at one site per table — and the shard replies
once per query it handles
(`shard.rs:717-744`), merging the shares of a split query before replying rather than forwarding
each (`:765-822`). **Anything that makes a query answer twice breaks the reorder buffer, not just
the caller's row count.** That is the sharp end of
[item 52](../appendix/known-issues.md#52-a-resident-hit-in-exists-answers-a-query-a-blocked-clone-will-answer-again),
which is a path where one does.

**`End` cannot collide with a response.** The terminator shares the same map:
`self.pending.insert(index, ClientMsg::End(index))`. It is safe because
`ShoalQueryStream::close` posts `ClientMsg::End(self.base_index)` (`client.rs:1348`) *after*
`base_index` has been advanced past every query sent (`:1341`), so its index is always one past the
last response index rather than equal to one.

**A stashed response keeps its own arrival time.** `wait_for_next_response` takes a stashed
`ShoalResponse` back apart into its buffer and its stamps before re-wrapping it (`:960-961`), so a
response that waited in the buffer is not re-stamped when it finally comes out. A stage profile
would otherwise attribute the wait to the server.

**What is *not* upheld: releasing the slot.** The `channel_map` entry and the pooled channel pair
are released inside the `if end` arm of `next` (`:1032-1039`), and neither stream type implements
`Drop`. A stream abandoned before its last response — or one whose `next` returns `Err` — leaves
both behind, permanently
([item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client)).
`send_one` and `exists` avoid it only because a single-query bundle's one response *is* the end of
its stream.

## ShoalResponse

The zero-copy wrapper — a self-referential struct:

```rust
pub struct ShoalResponse<S: QuerySupport> {
    _buff: AlignedVec,
    archived: *const <S::ResponseKinds as Archive>::Archived,
    phantom: PhantomData<S>,
}
```

`shoal-core/src/client.rs:710-717`

`_buff` owns the bytes read off the socket; `archived` points into them. Nothing is copied and
nothing is parsed — accessing a row is a pointer cast.

The safety argument, from the source:

```rust
// SAFETY: AlignedVec is Send, and our pointer points into our own buffer
// which we own and control. As long as we never expose &mut access to _buff,
// this is safe to send across threads.
unsafe impl<S: QuerySupport> Send for ShoalResponse<S> where ... {}

// SAFETY: We never mutate the buffer after construction, and references
// obtained from get() are safe to share across threads as long as
// Archived<T> is Sync.
unsafe impl<S: QuerySupport> Sync for ShoalResponse<S> where ... {}
```

`shoal-core/src/client.rs:725-743`

It holds: `_buff` is never exposed mutably, and `AlignedVec`'s heap allocation does not move
when the struct moves. The invariant to preserve is "no `&mut` to `_buff`". `inner(self)`
consumes the struct to return the buffer for recycling (`client.rs:764-766`), which is fine.

Accessors, all going through `unsafe { &*self.archived }`:

| Method | Returns |
| --- | --- |
| `access::<T>()` | `Option<&ArchivedVec<Archived<T>>>` — the rows, still archived |
| `suceeded(opts)` | Whether the query met the success criteria *(sic)* |
| `kind()` | Which `ResponseActionNames` this is |
| `get_exists()` | `Option<bool>` |
| `get_index()` | The response's index |
| `format_response()` | `(headers, rows)` as strings, for shoalctl |

`shoal-core/src/client.rs:768-832`

Typical use:

```rust
let response = client.send_one(TestRecordGet::new(vec![key])).await?;
let access = response.access::<TestRecord>()?.unwrap().first().unwrap();
let record = TestRecord::deserialize(access).unwrap();
```

`shoal/tests/persistent_sorted_table.rs:80-88`

Note that `access` hands back archived rows — deserializing is the caller's choice, so a caller
reading one field of one row never materialises the rest.

## Success criteria

```rust
pub struct QuerySuceededOpts {
    pub insert: bool,
    pub update: bool,
    pub get: bool,
    pub delete: bool,
    pub exists: bool,
}
```

`shoal-core/src/client.rs:682-694`

Defaults to `true` everywhere (`client.rs:696-707`), so by default **a get that finds nothing
is an error**, and so is an update that matched no row. `send_one` and `exec` apply the
default, which is why the tests treat a missing row as a failure. To treat absence as normal,
pass an opts value with `get: false`, or use `exists`.

## Shutdown

```rust
impl<S: QuerySupport> Drop for Shoal<S> {
    fn drop(&mut self) {
        self.is_shutting_down.store(true, Ordering::Relaxed);
        self.proxy_handle.abort();
    }
}
```

`shoal-core/src/client.rs:467-474`

The flag lets in-flight proxies distinguish a clean shutdown from a server failure — EOF while
shutting down returns `Ok(())` rather than an error (`client.rs:506-513`). `abort()` stops the
acceptor loop; per-connection proxies are separate tasks and are not aborted, ending when
their sockets close.

## Design notes

**Split the socket, pool one half.** A pool of full-duplex connections would need each response
routed back to the connection that sent it, forcing the caller to hold the connection for the
whole query. Splitting means the write half is held only for the write, and responses are
demultiplexed by id — so a slow query never occupies a connection.

**Recycle channels, not just connections.** Kanal channel pairs are pooled through
`channel_queue`, so steady-state operation allocates neither connections nor channels.

**Zero-copy responses, honestly.** Reading into an `AlignedVec<16>` and holding a pointer is
what makes the response path genuinely allocation-free per row. Note this is only true on the
response side; requests are fully deserialized server-side
([Request Lifecycle](../architecture/request-lifecycle.md#3-coordinating-fan-out)) despite the
branch name.

**What these choices cost later.** Two of them are load-bearing for work that has not been done.
The flat pool of interchangeable connections is what
[D7](../direction/shard-aware-routing.md#what-it-breaks) would have to give up to route a query to
the shard that owns its tablet. And the zero-copy read is the property
[D4](../direction/encryption.md#the-options) has to work around, because
the conventional way to add TLS decrypts into a buffer the TLS library owns and copies from there —
which is correct, measurably slower, and would not be caught by anything in this repository.

## Limitations

Most of this list is one page: [D6](../direction/connection-pool.md) designs a pool with deadlines,
a health check that works, a builder, an endpoint list, and a `Drop` — the pieces are small
individually and four of them ~~wait on a message type the wire format does not have~~ now need
only a call site, since `Ping`, `Pong`, `Cancel` and `GoAway` are defined and unwired message types
([F10](../features/framing-and-protocol-evolution.md)).

**A connection now shakes hands before it is used.** `Shoal::new` opens ten connections and each
exchanges a `Hello`/`HelloAck` carrying the protocol version, a compile-time fingerprint of the
schema, the largest frame each side will accept, and — since
[F12](../features/authentication.md) — which authentication mechanisms the client can do and which
one the server picked. A client built from a different schema than the server is refused with both
fingerprints in the error:

```rust
Err(Errors::Handshake(ConnectError::Protocol(ProtocolError::SchemaMismatch { ours, theirs })))
```

Because `bb8` retries a failed connect with backoff until its five second connection timeout
elapses, and a schema mismatch is permanent, that error takes about five seconds to arrive. The
same is true of a refused credential, for the same reason.

**Authentication is per connection, and the pool opens ten.** The credentials live on
`ShoalConnectionManager`, which is where `bb8` makes a connection, so a connection the pool opens
to replace a dead one re-authenticates with no code anywhere else. The cost of that is that a cold
`Shoal::with_credentials` runs ten SCRAM exchanges concurrently, each of them three round trips and
a PBKDF2 derivation on both ends. Nothing measures it
([O30](../appendix/optimizations.md), and [F12](../features/authentication.md#performance)).

- Health checks do not detect a dead peer
  ([D6](../direction/connection-pool.md#health-checks-that-work)).
- No retry, no reconnect logic above bb8, and no request timeout anywhere
  ([D6](../direction/connection-pool.md#deadlines), and
  [retries](../direction/connection-pool.md#retries), which are only safe for `Get` and `Exists`).
- Ordered streams buffer unboundedly behind a gap.
- Only one endpoint is ever known — `Shoal::new` takes the first address `lookup_host` returns
  (`client.rs:130`), so there is no failover
  ([D6](../direction/connection-pool.md#a-builder)).
- ~~No server-side error channel, so failures arrive as closed connections.~~ Built as
  [F11](../features/error-channel.md): a query that failed comes back as
  `Errors::Server { code, msg, .. }`, and `response.error()` answers it directly. What remains is
  that a *frame-level* failure names the bundle rather than one query in it, and that `send_one`
  still treats an empty get as a failure
  ([item 55](../appendix/known-issues.md#55-a-get-that-found-nothing-is-reported-as-a-query-that-failed)).
- ~~**No authentication and no encryption**, so anything that can reach the port can read and
  write any table.~~ Half built as [F12](../features/authentication.md): a client can prove who it
  is with `with_credentials`, and a server can refuse one that cannot. What remains is that there
  is no TLS, so the username and the whole exchange are visible on the path
  ([D4](../direction/encryption.md)); that a principal that authenticated may still read and write
  any table, because there is no authorization; and that a server with no `auth` section — the
  default — still requires nothing of anybody.
- **Credentials cannot be changed on a live client.** They are given to the constructor and held
  by the pool for the life of the client, so rotating a password means building a new `Shoal`
  ([D6](../direction/connection-pool.md#a-builder)).
- **A query's response type is not checked at compile time.** `access::<T>()` takes the row type
  from the caller and a wrong one fails at runtime with `Errors::WrongType("Wrong Type!")`, which
  names neither type ([D8](../direction/typed-queries.md)). Since
  [F11](../features/error-channel.md) a query that *failed* is no longer reported that way — it
  answers `Errors::Server`, so "you asked for the wrong type" now means only what it says.
- **The client links glommio**, and therefore io_uring, whether or not it will ever start a server —
  the `server` feature that appears to make it optional does not work
  ([item 54](../appendix/known-issues.md#54-shoaldb-needs-three-crates-the-caller-has-never-heard-of),
  [D5](../direction/runtimes.md)).
- `ShoalResultStream::skip(0)` panics with an integer underflow
  (`client.rs:1001-1008`) — the decrement precedes the zero check.
- **A stream that is not drained to its end leaks its slot in `channel_map` and its pooled channel
  pair**, because neither stream type implements `Drop`
  ([item 60](../appendix/known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client)).
  ~~the release is inside `next`'s `if end` arm~~ — [F11](../features/error-channel.md) moved it so
  that it also runs when `next` returns `Err`, which was one of the three ways to leak. Dropping the
  stream early and `skip`ping past the end are the other two, and both remain.
  Responses the server later sends for that query are then delivered into an unbounded channel with
  no reader.
- `Shoal::send` archives the bundle before the id is finalised.
- Two large blocks of commented-out code remain (`client.rs:549-603`, `:1048-1114`).
- `suceeded` and `QuerySuceededOpts` are misspelled in the public API.
- **Nothing measures any of this.** `client.rs` carries no `tracing` spans and no `hotpath` scopes,
  so every macro benchmark number includes the client and none can attribute anything to it
  ([TODOs](../appendix/todos.md#benchmark-coverage-the-harness-does-not-have)). The
  `transport/*` workloads that would give it a number are unbuilt.
