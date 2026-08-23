# TODOs and Unbuilt Work

Two lists: the `TODO` markers actually present in the source, and the larger pieces that the
code implies but does not contain.

Open defects are catalogued separately in [Known Issues](known-issues.md), and fixed ones in
[Resolved Issues](resolved-issues.md). Several entries here overlap; where they do, the issue
entry has the detail.

## In-code TODOs

**Eight** `TODO` comments and no live `todo!()` outside `target/`. The five struck-through rows
below have been done and are kept for their links.

This said "seven" until the [August 2026 review](review-2026-08.md) counted them; eight rows were
listed below it the whole time. Every citation in the tables was re-resolved in the same sweep and
all eight had drifted.

### Storage

| Location | TODO | What finishing it involves |
| --- | --- | --- |
| ~~`.../fs/compactor.rs:201`~~ | ~~`does anything else need to be done to remove this partition from archive maps?`~~ | **Done.** The answer was yes. `MapIntent::Remove` was added, the prune path logs it and drops the entry from `to_archive` after the sync: [Resolved Issues #5](resolved/resurrected-deletes.md). |
| `.../fs/compactor.rs:471` | `make size configurable` | Move `MIN_ARCHIVE_COMPACTABLE` and the hardcoded 50% utilisation threshold into `FileSystemTableConf`. |
| `.../fs/map.rs:368` | `make issue about SerializedMap not needing to track active` | `SerializedMap` does not persist the active archive id, so every restart mints a new one and orphans the previous active archive until compaction reclaims it. Either persist it or document the churn as intended. |
| ~~`.../fs/loader.rs:128`~~ | ~~`todo!("Add back onto loader channel")`~~ | **Done**, and not by requeueing. The answer was that a read has to report how it went whether it succeeded or not, because a load completing is the only thing that releases the queries parked on it: [Resolved Issues #16, 51](resolved/partition-load-failure.md). |
| ~~`.../fs/loader.rs:137`~~ | ~~`handle this error`~~ | **Done.** A read task can no longer fail — it reports every outcome to its shard itself, so the shutdown drain has no error to decide about. Same page. |
| ~~`.../fs/loader.rs:157`~~ | ~~`do something with this error`~~ | **Done.** Same, on the steady-state path. Same page. |

### Server

| Location | TODO | What finishing it involves |
| --- | --- | --- |
| `shard.rs:63` | `do something with this error` | `client_rx_relay` panics on any non-EOF socket error. Should tear down the one connection. |
| `shard.rs:141` | `detect collisions?` | Client UUIDs are generated without checking `client_map`; a collision panics at `shard.rs:916`. The client does exactly this check for query ids (`client.rs:193-203`) and could be copied. |
| `shard.rs:147` | `do this with a task queue?` | Per-client relay tasks run on the executor's default queue, so client IO is unprioritised relative to background writes. |
| ~~`.../persistent/unsorted.rs:867`~~ | ~~`handling a partition missing`~~ | **Done.** The startup-path `panic!` is a `warn!` and a skipped intent now. See [Resolved Issues #9](resolved/orphaned-update-intents.md). |

### Client and UI

| Location | TODO | What finishing it involves |
| --- | --- | --- |
| `shoal-client/src/client.rs`, `is_valid` | `implement a ping/pong type request?` | `is_valid` calls `peer_addr()`, which cannot detect a dead peer. ~~Needs a message-type field the wire format does not have~~ — the format has one since [F10](../features/framing-and-protocol-evolution.md), and `Ping` and `Pong` are message types 7 and 8 with nothing behind them. What is left is a send, a handler, and a deadline: [D6](../direction/connection-pool.md#health-checks-that-work). No longer a flag day. |
| `shoal-client/src/client.rs`, `ShoalQueryStream::send` | `make it so we don't need to do this` | `ShoalQueryStream::send` overwrites `queries.id` on every bundle. The stream's id should be set at construction. |
| `shoalctl/src/app.rs:465` | `Handle insert mode for editing rows` | Insert mode edits the query bar only; result rows are read-only. Writing would also need SHQL to parse mutations. |

## Larger unbuilt work

Implied by the code's shape but not present.

**Where the client half of this now lives.** Six of the entries below are about the client or the
wire it speaks, and they have grown a design rather than staying sketches. That design is the
[Direction](../direction/overview.md) chapter, which says how each would be built, what it would
cost, and in what order — this page still records *that* they are unbuilt, and each entry points
at the page that now carries the *how*. Nothing there is built either.

### Distribution

`ShardContact` has one variant:

```rust
pub enum ShardContact {
    /// This shard is on our current node
    Local(usize),
}
```

`shoal-core/src/server/shard.rs:183-187`

The `match` in `Comms::send` (`comms.rs:46-58`) has one arm. Everything above it — the tablet
map, `ShardInfo`, the `Join` broadcast — is already shaped for a multi-node cluster; the transport
and membership are missing. Adding a `Remote` variant is the seam.

The tablet map ([items 11, 12, 37](resolved/tablet-ring.md)) changed what this costs. Ownership is
now *stored* per tablet rather than derived from a hash, so the multi-node work is to make that
table authoritative and movable rather than to reimplement routing. Two consequences worth
knowing before starting:

- **Replication widens the stored value.** `tablets: Vec<u16>` becomes a replica set per tablet,
  primary first. On a token ring the replica set is a walk that skips vnodes belonging to a node
  already chosen and filters by rack and DC; with tablets it is simply the value. That is the
  ugliest part of the equivalent Cassandra code and Shoal does not have to write it.
- **Somebody has to own the table.** A derived mapping needs no coordination — that is the whole
  virtue of consistent hashing, and it is what is being given up. Scylla puts the tablet table
  under Raft. This is less additive than it sounds, since membership and failure detection need
  consensus anyway.

Also needed for a real cluster: replication (there is exactly one copy of every partition),
membership and failure detection, and rebalancing.

The *client* half of this is now designed separately.
[D7](../direction/shard-aware-routing.md) covers routing a query to the shard that owns its tablet
from the client rather than from a coordinator, which is a prerequisite for multi-node routing and
not a substitute for it — the transport and membership work above is unchanged by it.

### Rebalancing

Today the shard count is part of the on-disk format — intent logs are `Shard-N-active` and
each shard has its own archive map. Changing `resources.cores` between restarts now **refuses to
start** rather than silently stranding data, since `StorageMeta` records the count a directory was
written by ([Partitioning](../architecture/partitioning.md#limitations)) — a better failure, but
not a fix.

Two pieces are needed, in this order.

**Persist the tablet assignment.** `Ring::new` recomputes `i % shard_count` on every start, so a
tablet is movable in principle only: nothing can move one and have the move survive a restart.
The map has to become durable state before it can become editable state. It is small — 4096
entries — and `StorageMeta` is already the file that per-node durable facts belong in.

**Key storage by tablet rather than by shard.** This is the larger half and the reason changing
`cores` cannot work today. If intent logs and archive maps were named by tablet id instead of by
`Shard-N`, moving a tablet between cores or nodes would be moving a file and flipping one map
entry, rather than rehashing everything. It was deliberately not built with the tablet map, for a
sequencing reason worth recording: what the layout should be depends on how migration streams
data, and that protocol does not exist yet. Building the layout first risks building the wrong one
and migrating twice. The tablet id — the name that makes it expressible — now exists either way.

Still needed on top of both: a way to discover files belonging to shards that no longer exist,
and a rebalancer that decides *when* to move a tablet rather than merely how.

A third piece appears once the map is editable, and it is on the client side:
[D7](../direction/shard-aware-routing.md#5-staleness-which-is-what-makes-it-safe) — any client
holding a copy of the map holds a stale one during a move, so the map has to carry a version and
a query routed against a stale one has to be forwarded rather than refused. That is a constraint on
how rebalancing is built, not a consequence of it, which is why it is worth knowing before starting.

### Sort-key range predicates — built

`WHERE title >= 'M' AND title < 'N'`, and the cursor to page a partition with, are
[F1](../features/sort-key-ranges.md). Two pieces of what that section asked for were deliberately
left out and are still open.

**`BETWEEN`.** `title BETWEEN 'M' AND 'N'` is sugar over `>= AND <=`. Its inner `AND` collides
with the `AND` that joins clauses, so `where_conditions` needs to know it is inside a `BETWEEN`
when it meets one, and the completion state machine needs its own `Expecting` states for the same
reason — `AND` is already a transition out of `Continuation`. It buys nothing the two operators do
not, which is why it was not built with them.

**Prefix ranges over a composite sort key.** Several `#[shoal(sort)]` fields make a tuple `Sort`,
and the typed API can already range over whole tuples since a tuple is `Ord`. What neither front
end can do is bound a *prefix* — `(author, title) >= ('Le Guin', ..)` — which needs synthesized
minimum and maximum values for the remaining elements, so `Sort` would have to name them. SHQL
cannot reach a composite sort key at all
([item 42](known-issues.md#42-shql-cannot-express-a-composite-sort-key)).

Also still true, and worth keeping in front of anyone who reaches for a range to make a read
cheaper: a **cold partition is read whole either way**, since there is no index within a partition
on disk. A range changes what is deserialized and what crosses the wire, not what is read.

### Projections — built

A get can be answered with a named subset of a table's fields instead of whole rows, in the typed
API and in SHQL, and it reads only those fields out of an archived partition
([F2](../features/projections.md)). Four pieces of what a projection could be were deliberately left
out.

**A column list in SHQL.** `SELECT title, year FROM Movie` is still a parse error. A response is an
archive of a concrete type and the client reads it without deserializing, so an arbitrary set of
columns has no type to be. Answering one would mean a per-query row shape and a client that walks
bytes by offset instead of by type — a different protocol, not an extension of this one.

**Deriving a projection from a field list.** A projection is a struct that has to be written out,
and there is nothing that turns `#[shoal(project(id, title))]` into one, or that says "everything
but `data`". This is only ergonomics, but it is the ergonomics people will ask for first.

~~**Projections on ephemeral tables.** An `EphemeralTable` cannot be a field of a `#[db]` struct, so
there is nowhere to declare a projection for one. It carries an `ShoalProjection<Row = Self>` bound
and answers with whole rows. Fixing it is the same work as making ephemeral tables usable in a
database at all.~~ Done by [F9](../features/ephemeral-tables.md), and by exactly that route: the
ephemeral tables are aliases for the persistent ones, so they carry the projection support the
persistent tables already had rather than gaining any of their own. Covered by
`shoal/tests/ephemeral_sorted_table.rs::projection_returns_only_its_own_fields`.

**A projection on `MemItem`, so the projection read path has an ephemeral control.** `Item`
declares `ItemKeys` and [F9](../features/ephemeral-tables.md)'s `MemItem` declares nothing, so
there is no storage-free control for reading a projection the way there now is for reading whole
rows. It is one struct and one attribute; it was left out because F2 shipped without a projection
benchmark at all and adding one is a separate piece of work from adding the table it would run on.

**An instrumented run of `macro/insert_ephemeral`.** Its `profiles()` is `false`, so the `hotpath`
and `stage-profile` layers never run it. Subtracting its profile from `macro/insert_unsorted`'s
would attribute the storage layer function by function rather than as one wall clock difference.
Turning it on doubles the two most expensive phases of a capture, which is why it is off; the
right shape is probably a flag that opts a named workload into the instrumented layers for one
capture rather than a permanent `true`.

**A fast path through the ephemeral tables.** They are the persistent tables with the disk
removed, so an insert is still wrapped in an intent, parked in `PendingResponse`, and released on
a shard sweep rather than answered inline, and every partition is still behind a `MaybeLoaded`
that can only be the loaded arm. A leaner table was
[deliberately not built](../features/ephemeral-tables.md#alternatives-rejected) — a second
implementation of the table interface with no trait keeping it honest is how the unsorted
delete/update asymmetry happened. The version worth having instead is inside the existing tables,
guarded on the engine: `commit` could report that its response needs no parking, and the
`check_disk` probe could be skipped for an engine that never has anything on disk. Both are
measurable against `macro/insert_ephemeral` and `macro/get_ephemeral`, which is what makes them
worth attempting at all.

**A projection that leaves out the partition key.** A projection has to carry its table's partition
key, because the shard collecting the shares of a split get asks each row which partition it came
from. Lifting that means carrying the grouping on the wire instead, which is
[O18](optimizations.md#o18-the-gathered-reorder-rehashes-every-rows-partition-key).

Also still true, and the same caveat a range carries: a **cold partition is read whole either way**.
A projection changes what is deserialized and what crosses the wire, not what is read.

### Intersection across partitions (a real `AND` on one field)

`WHERE keyword = 'giant worm' AND keyword = 'alien'` reads as "movies with both keywords" and
is currently a parse error telling you to write `IN (...)` if you meant either of them. There is
no way to ask for both.

The reason is what a partition holds. `MovieByKeyword` is partitioned by keyword and sorted by
title, so each keyword's partition is the list of titles carrying it. Answering the
intersection means reading every named partition in full, keeping the titles each one holds, and
returning only the titles present in all of them. Three things make that different from the union
a get does today:

- **It cannot early exit on a limit.** A union can stop as soon as it has enough rows, because
  every row it has is an answer. An intersection cannot: a title is only known to qualify once
  every partition has been checked, so `LIMIT 2` still reads all of them. The
  `PendingGet::filled_before` rule (`server/tables/persistent.rs`) that lets a limited get skip
  later partitions has to be switched off for one.
- **It has to happen after the shares are gathered,** not on each shard. Two keywords will
  usually live on different shards, and neither can tell whether a title it holds is in the
  other's partition. So the intersection belongs beside `order_by_partitions` in
  `Shard::handle_gathered` (`server/shard.rs`), which means `ResponseAction::merge` needs to
  learn a second mode rather than always taking the union.
- **It needs a row identity to intersect on.** For a sorted table the sort key is the natural
  one — two `MovieByKeyword` rows are the same movie when their titles match — but that is a
  convention of this schema rather than something the table declares. A general answer wants
  the row type to name the field the intersection is over, which is a new `#[shoal(...)]`
  attribute.

The cheaper alternative is a secondary index: a table keyed by movie holding its keywords, so
the question becomes a single partition read and a filter rather than a scatter-gather. That is
a larger feature but it is the one that scales, since the intersection above is `O(rows in the
largest partition)` however few rows come back.

Until one of those exists, the parse error is deliberate. The spelling used to be accepted and
answered as a union, which is the opposite of what it says.

### Full boolean `OR`

`OR` currently joins conditions on one field, where it means the same thing as `IN`. Crossing
fields is rejected. There are three separate cases hiding behind "support `OR`", and they are
not equally hard.

**Same field — built.** `keyword = 'a' OR keyword = 'b'` is a union of partitions, which is
what `split_by_shard` already does with a list of keys.

**Between filters, under a pinned partition — answerable, unbuilt.**
`keyword = 'a' AND (title = 'x' OR watched = true)` is a per-row predicate evaluated inside a
partition that has already been selected, so nothing about the access path changes. What
changes is the filter representation. A generated `*Filter` is a struct of `Option<Vec<T>>`
fields and `is_filtered` is a straight-line conjunction over them
(`shoal-derive/src/tables.rs`), which can express "any of these values, for every named field"
and nothing else. A disjunction needs:

- a recursive predicate tree replacing the filter struct, which has to be an rkyv `Archive`
  type — recursive, so its archived form is boxed (`rkyv::boxed::ArchivedBox`) rather than
  inline
- an evaluator over that tree for both `is_filtered` and `is_filtered_archived`
- parentheses and precedence climbing in the parser, since without grouping the tree can only
  ever be one level deep and is not worth having

**A partition key `OR`'d with anything — not answerable.** `keyword = 'a' OR title = 'Alien'`
asks for every row titled `Alien` in any partition. The only access path is by partition key
and there is no scan, which is also why the `WHERE` clause is mandatory. This case has to stay
an error whatever else is built, so `OR` will always be legal between some field pairs and
illegal between others.

There is a structural problem past the parser too. With standard precedence,

```sql
WHERE keyword = 'a' AND title = 'x' OR keyword = 'b' AND title = 'y'
```

is two disjuncts with *different filters per partition set*. `SortedGet` carries one `filters`
for all of its `partition_keys` (`shared/queries/sorted.rs`), so this has to become two gets.
That breaks the one-response-per-query contract `Gather` (`server/shard.rs`) and the client's
index-ordered stream (`client.rs`) both rely on, and leaves `LIMIT` undefined across them.

The way through is to normalise the parsed expression into disjunctive normal form, reject any
disjunct that does not constrain a partition key, and group the rest into `(partition_keys,
filter)` pairs. Disjuncts sharing a filter collapse into one get. The ones that do not need
either a get that carries a filter per partition group, or a response model where several gets
answer under one index — which is the same machinery a real `ORDER BY` across partitions would
want, so it is worth building once rather than twice.

### An error channel in the protocol

> **Landed as [F11](../features/error-channel.md).** What follows is what is left of this entry,
> and the part of it that was wrong.

`ResponseAction` now carries an `Error(ResponseError)` variant and the frame format constructs its
reserved `Error` type, so a server-side failure has somewhere to go. This entry claimed that
"items 51, 55, and 56 all want the same variant". Two of them did.
[Item 55](known-issues.md#55-a-get-that-found-nothing-is-reported-as-a-query-that-failed) did not:
a get that found nothing is a query that *worked*, and what it wants is for `send_one` to take a
`QuerySuceededOpts` rather than always using the default. Filing it here made it look blocked on a
wire change for a year when it was a local edit to one function.

What this entry still holds:

- **The eight `storage.commit(..).unwrap()` sites.** This entry's original claim — that an error
  variant "would unlock replacing most hot-path panics with recoverable errors"
  ([Known Issues #16](known-issues.md#16-panics-on-the-hot-path)) — is now true and untaken. A full
  disk on an ordinary insert still panics the shard, and it now has somewhere to report instead.
- **`Flags::IS_ERROR` on a response frame whose payload is an error.** F11 sets it on `Error`
  frames only. Setting it on responses would mean threading a flag through `client_tx_relay`'s
  `(Uuid, Span, StageStamps, AlignedVec)` tuple, and so through `ServerMsg::NewClient`,
  `client_map` and `Shard::reply`, to teach the relay about a payload it exists to treat as opaque.
  Worth doing when something needs to branch on it without decoding the payload; not before.
- **An index in the `Error` frame.** Its query id is a bundle id, so a frame-level failure fails a
  whole result stream rather than the one query in it that failed. The two reserved bytes after the
  code are where an index would go.

### Backpressure

Every channel is unbounded ([Known Issues #15](known-issues.md#15-no-backpressure-anywhere)).
Bounding them requires deciding what to do when a shard is saturated — shed load, block the
coordinator, or reject the client — ~~which requires the error channel above~~. **That
prerequisite is met.** [F11](../features/error-channel.md) made shedding sayable:
`ErrorCode::Shedding` is defined and a query the server declined can be reported as declined rather
than as an empty result. What is left is the bound itself and the policy that decides when it is
hit, in [D6](../direction/connection-pool.md#bounded-channels).

### Timeouts

Nothing anywhere has a deadline: no query timeout on the client, no timeout on a blocked
query waiting for a partition, and no timeout on the connection pool beyond the initial
connect.

The client half is designed in [D6](../direction/connection-pool.md#deadlines), which adds one
thing this entry does not: **a deadline without a way to cancel converts a slow query into a
leak.** A client that gives up has to tell the server, or the server keeps working and writes into
a channel with no reader — the same failure as
[item 60](known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client),
reached from the other side. ~~That needs a `Cancel` message type~~ — and **it does not need one at all**: the leak that
sentence names was closed from the other end by [F11](../features/error-channel.md), so a deadline
without a `Cancel` is a deadline and not a leak. See [`Cancel`, and what it would actually
buy](#cancel-and-what-it-would-actually-buy).

~~A partition load that never completes parks its queries permanently.~~ A partition read that
*fails* now releases them ([Resolved Issues #16, 51](resolved/partition-load-failure.md)). One
that neither completes nor fails still parks them permanently, and that is what a deadline here
would cover — the difference matters, because the first was a bug in the read path and the
second is the absence of a deadline.

### `Cancel`, and what it would actually buy

**Dropped from [D6](../direction/connection-pool.md)'s scope**, deliberately, and recorded here
rather than left implied. `Cancel` is message type 12, defined and unwired since
[F10](../features/framing-and-protocol-evolution.md). D6 treated it as a prerequisite for both the
deadlines and the `Drop`, on two premises that
[F11](../features/error-channel.md) had already made false: an orphaned response is a `WARN` and a
`continue` rather than a killed read loop, and the `channel_map` entry a `Drop` has to remove comes
out with a synchronous call. So without `Cancel` there is no leak, no hang and no wrong answer —
only wasted server work and response bytes written to a socket whose reader discards them, which is
a performance claim, and [Optimizations](optimizations.md) forbids acting on one before a benchmark
exists that would show it. Nothing in `shoal-bench` abandons a stream.

There are two depths, and **the cheap one does not buy what the expensive one is for**:

| Depth | What it costs | What it buys |
| --- | --- | --- |
| Drop at the connection relay | a `protocol/cancel.rs`, a message-type dispatch replacing `decode_request`'s `.expect(Queries)` in `client_rx_relay`, and an `Rc<RefCell<HashSet<Uuid>>>` shared with `client_tx_relay` | the bytes are not written. The shard still does the work |
| Cancel at the shard | a `ServerMsg::Cancel` broadcast the way `NewClient` is, plus an **expiring** cancelled-set on every shard — a cancel can arrive before, during or after its query — and a lookup on the path `macro/transport/send_one/small` measures | the work stops |

The case that motivates cancellation at all is a timeout storm: a short deadline against a slow
server, at concurrency, with the server grinding on queries nobody will read. Only the second row
addresses that, and it is the row that puts a lookup on the hot path and an unbounded-unless-expired
set on every shard.

**One thing to settle before building either.** The wire query id is a *bundle* id
(`shoal-proto/src/shared/queries.rs`, `Queries::default`), so a `Cancel` naming one cancels every
query in that bundle. On the streaming path a whole session shares one id, which makes `Cancel` and
`ShoalQueryStream::close` near-synonyms. A per-query cancel needs an index, and
[F11](../features/error-channel.md) already identified where one would go: the two reserved bytes
after the error code.

### Choosing a default for the query deadlines

[F17](../direction/connection-pool.md#deadlines) ships `Deadlines::request` and `Deadlines::idle` as
`Option<Duration>`, both `None`. That is the choice that breaks nothing on landing and it gives
nobody the stability D6 exists for — a caller who never reads the docs keeps the hang.

The reason it was not decided there is that a non-`None` default changes the behaviour of every
existing caller, including `shoal-bench`, and the evidence for a number does not exist yet: nothing
has run the `transport/*` workloads with deadlines on. Do that first, at both row widths, and pick
from what the tail actually looks like rather than from a round number.

### Retrying more than a single-query bundle

[F17](../direction/connection-pool.md#retries) retries `send_one` and `exists` only — single-query
bundles where nothing has been delivered to the caller yet — on `ErrorCode::ConnectionLost`, under
`RetryPolicy::ReadsOnly`.

`exec` and `send` are harder for a reason that is not idempotency: both may have handed responses
to the caller before the connection died, so replaying the bundle would deliver some of them twice.
Retrying them means either buffering until the bundle completes, which gives up the streaming the
API exists for, or replaying only the indices not yet seen, which needs the server to answer a
bundle partially. `stream` is harder again, since its bundle is open-ended.

Note this is a *different* problem from the one below, and the two are often confused: this one is
about a client that has already returned rows, and that one is about a server that has already
applied a write.

### An idempotency key, so a write can be retried

A client cannot retry a write. `Get` and `Exists` are idempotent and `Insert`, `Update`, and
`Delete` are not (`shared/responses.rs:27-39`), so a bundle lost to a dead socket can only be
replayed if the server can recognise a repeat. That needs a per-query key the server remembers for
long enough to answer the second copy with the first one's answer — a server feature, and the only
thing standing between [D6](../direction/connection-pool.md#retries) and retrying anything rather
than only reads.

Worth knowing before designing it: [FoundationDB](../direction/prior-art.md#foundationdb) does not
solve this, it sidesteps it — it retries the *transaction*, so the request-level question never
arises. Shoal has no transaction to retry, so it has to answer the question directly.

### An in-process client for a colocated application

The one genuinely compelling non-tokio client is an application already running on glommio that
wants to query a Shoal server in the same process. Serving it with a glommio TCP client is the
wrong shape ([D5](../direction/runtimes.md#recommendation)): the right one hands a `Queries` bundle
straight to the local shard's channel and skips the socket, the framing, and both serializations.

It is a different feature from runtime portability and is filed separately so that portability is
not built to serve a case it serves badly. It is also the only path in this repository that could
answer what the wire actually costs, by being the same query with the wire removed.

### Per-table authorization

~~[D3](../direction/authentication.md) gives a connection a principal and deliberately stops
there.~~ **[F12](../features/authentication.md) built the principal**, so this is unblocked rather
than blocked. A connection that authenticated carries a `Principal` — a name and the mechanism that
proved it — and **nothing reads it**.

What that principal may read or write is a server-side catalog problem — somewhere to store grants,
a check on the query path, and a way to express them in SHQL — and none of it is client design.

Filed rather than sketched, because ~~a design written before there is any notion of a principal
would be a design for nothing~~ that reason has expired and the work has not been done. It is the
thing authentication exists to enable, so it should be picked up immediately after, not much later.

### The rest of authentication

Four pieces [F12](../features/authentication.md) deliberately did not build, smallest first.

**SASLprep.** RFC 5802 says to normalize a password before deriving from it and the build does not,
so two clients that disagree about the Unicode normalization of a non-ASCII password derive
different keys. It costs a dependency (`stringprep`) and is the identity function on ASCII, which
is every password this database has been given. The place it goes is `ScramClient::new` and
`StoredCredential::from_password`, and both have to change together or existing credentials stop
matching.

**Credential reload.** Users are read once, at startup, and derived per shard. Adding, removing or
rotating one means restarting the server. The awkward part is not re-reading the file, it is that
each shard holds its own `Rc<CredentialStore>` and nothing exists to hand all of them a new one.

**Rate limiting a login.** Nothing bounds how often a peer may connect and guess, and each guess
costs the server a PBKDF2 derivation — which is the wrong side of that trade, since the client can
send a garbage proof without doing one. Also filed in [Known Issues](known-issues.md), because the
absence is a defect rather than a missing feature.

**A re-auth ticket.** [D3](../direction/authentication.md#what-it-costs) proposes one to collapse
SCRAM's rounds on the second and subsequent connections of a pool. It is worth having **only behind
TLS**: a ticket in `Hello` is a bearer token, replayable by anything that sees it, which is exactly
what the options table rejected a bearer token for. Filed here rather than acted on, and it should
not be picked up before [D4](../direction/encryption.md). **D4 has since been built**
([F14](../features/encryption-in-transit.md)), so the precondition is met and this is now merely
unbuilt — and it matters more than it did, because kTLS made TLS session resumption unavailable, so
the ticket went from one of three mitigations for the fifty-handshake problem to one of two.

### The rest of encryption

Three pieces [F14](../features/encryption-in-transit.md) deliberately did not build, and one it
could not.

**Key updates, and the confidentiality limit that goes with them.** Once the kernel owns the record
layer, rustls is out of the data path and neither peer generates a `KeyUpdate` — so none occur, and
the AES-GCM message limit rustls normally enforces is enforced by nothing. Both peers being ours is
what makes this survivable rather than a hole. `ktls::enable` keeps the `KernelConnection` for the
life of the connection precisely so that handling one is a call site rather than a redesign:
`update_tx_secret` and `update_rx_secret` hand back the new keys, and the socket takes them the same
way it took the first pair. What is missing is the trigger — a byte counter and a
`setsockopt`, or reading the kernel's key-expiry signal.

**Session resumption**, which kTLS cost outright. A ticket after the handshake is a non application
record and a plain `read` fails it with `EIO`, so `send_tls13_tickets = 0`. Recovering resumption
means either handling control messages through `recvmsg` with a `CMSG` loop — which reaches the
read path this whole feature exists to leave alone — or the re-auth ticket above, at the Shoal
layer rather than the TLS one. The second is the cheaper answer and is why that entry now matters
more.

**mTLS.** `AuthMechanism::MutualTls` is still defined and still refused, in `server_auth` and in the
client's `authenticate`. There is a TLS layer to read a certificate subject off of now, so this is
the new arm in two matches [D3](../direction/authentication.md) predicted, plus a client
certificate on `TlsClientOptions`, a CA on the server's config, and a subject-to-`Principal`
mapping. The last of those is the part with a real design question in it.

**Channel binding for SCRAM.** F14 makes `tls-exporter` available, which closes what
[F12](../features/authentication.md) had to decline: its gs2 header is still `n`, and a `y` there
without support is exactly the downgrade `y` exists to detect. Note the interaction — the exporter
has to be taken from rustls *before* `dangerous_into_kernel_connection` consumes the session.

### Archive map reconstruction

Archives write a size prefix before each partition specifically so a map could be rebuilt by
scanning — the comment says so (`.../fs/compactor.rs:306-310`). No such path exists, so
`ShoalError::MapCorruption` is fatal even though every byte of data is intact.

### Observability

Nothing a monitoring system can read exists — no metrics endpoint, no counters, no gauges
([Observability](../operations/observability.md#what-is-missing)). Two pieces of this were
carved off by [item 9](resolved/orphaned-update-intents.md) and are worth naming separately,
because that item deliberately stopped short of both.

**A metrics surface.** `RecoveryStats` is counted per table, summed per shard by the derive
generated `ShoalDatabase::recovery_stats`, and emitted as one event per shard. The accessor
exists precisely so something can read the numbers rather than parse them out of logs; nothing
does. Whatever gets built should expect to carry more than recovery — resident bytes, LRU depth,
compaction backlog and blocked-query count are the other obvious first residents.

**Aggregating across shards.** The recovery summary is per shard, and there is no pool-wide
total, because `ShoalPool::start` spawns its shard threads and returns without joining them —
there is no moment at which every shard has finished starting. Giving the pool that moment is
the actual work here, and it would be useful well beyond recovery: a readiness endpoint needs
exactly the same thing.

A third piece is smaller but blocks testing either of the above: `trace::setup` is never called
by the library, ~~only by the example binary~~ **or by anything else** — the example does not call
it either, and neither does `shoal-workload`, `shoalctl` or any test. So no test can observe any
event the server emits ([Test Coverage](test-coverage.md)), the `tracing` section of `shoal.yml`
configures nothing, and every `#[instrument]` and `event!` in the workspace — including the client's
since [F16](../features/client-builder.md) — dispatches to nobody. Filed as
[item 69](known-issues.md), where the two decisions it needs are written down: whether a library
should install a *global* subscriber at all, and how a benchmark capture keeps getting the same
one it has always had.

**Promote eviction drift to a `WARN`.** The eviction event now carries a `drift` field — the gap
between what a pass actually dropped and what the shard counter moved by, which is non zero only
when the counter had already floored ([Resolved #13](resolved/eviction-log-underflow.md)). It is
deliberately an `INFO` field rather than a warning, because
[item 22](known-issues.md#22-size-accounting-inconsistencies) makes drift ordinary and a warning
would fire on healthy runs. Once item 22 is closed, non zero drift becomes an invariant violation
and should say so at `WARN`. Doing it before then trains people to ignore it.

### Benchmark coverage the harness does not have

[F3](../features/performance-harness.md) built three measurement layers and left four gaps in
them, and [F5](../features/flushed-sweep-gate.md) found a fifth. Each was deliberate, and each is
worth more than most of what is above it on this page, because the harness decides what evidence
any future optimization can produce.

[F8](../features/purpose-built-workloads.md) closed several of these and narrowed another. What it
did **not** close is marked below; what it added instead is at the end of this section.

**A micro-benchmark of the storage write path.** This is the important one. `write_helper`
dominates the profile at 32.6 ms per call, five orders of magnitude above the partition insert
it persists ([Performance Baseline](../performance/baseline.md)) — and it is the one
layer with no confidence interval around it, so any change to it can only be judged by a
measurement whose spread is 10.5%. It was not built because timing `StreamWriter::write` and
`start_sync` means driving a glommio `LocalExecutor` from inside criterion's sampling loop,
through `Criterion::iter_custom`. If that proves unworkable, a standalone binary emitting the
same JSON shape is an acceptable substitute.

**Still open, and the substitute now exists.** [F8](../features/purpose-built-workloads.md) built
that standalone binary — `shoal-workload` — and gave `ServerNeed` a `None` arm precisely so a
workload can drive engine internals inside its own `LocalExecutor` with no server at all. Nothing
uses that arm yet. Writing `micro/write_path` against it is now a matter of writing the workload
rather than of solving the executor problem.

~~**An `Async` vs `Fsync` capture.** The single comparison that would isolate the cost of the
durability barrier, on a config change alone, with no code change. It is cheap. It has not been
run. [F8](../features/purpose-built-workloads.md) made it nearly free to build — `ConfOverrides`
already carries per-workload configuration and `insert_unsorted` is the arm it would pair against
— and deliberately did not build it, to keep the first workload set reviewable. It is the cheapest
remaining item on this page.~~ **Built**
([F20](../features/configuration-sweeps.md)), as two arms of a nine-knob sweep rather than as one
comparison. This entry was right that it was cheap and right about why it had not been run, and it
missed the reason: there was nowhere to put the answer. One number, on no page, comparable to
nothing. What it costs to *build* an isolated capture was never the binding constraint — what it
costs to make one **readable** was, which is why the durability pair arrived alongside eight other
sweeps and a page whose job is to say which of the nine moved anything measurable.

The arm it pairs against is not `insert_unsorted`, as this entry expected, but the grid's reference
cell `macro/grid/unsorted/r50/1024` — because a barrier is a property of a *workload's* write path
and the reference cell is the one every other page already quotes. `macro/conf/storage/durability/r50/fsync`
duplicates that cell deliberately, since an arm that ran in a different storage directory under a
different port is not a control for one that did not.

**`wire_codec` and `routing` benches.** `rkyv` round trips over `Queries` and `ResponseKinds`,
and `Ring::find_shard` / `split_by_shard`. Between them they are what O1, O18, O19 and O20 are
about, and none of those four can currently be adjudicated at all
([Optimizations](optimizations.md#which-entries-a-benchmark-can-currently-adjudicate)).

**Correction, filed while building [F8](../features/purpose-built-workloads.md).** These were
listed here, under the heading about macro coverage, as though they needed the workload harness.
They do not. An `rkyv` round trip and `Ring::find_shard` are pure CPU over plain data and need no
server, no executor and no storage backend — they belong beside `shoal/benches/partitions.rs` in
the **micro** layer, where they would get criterion's sampling and a confidence interval instead of
a wall clock with an 11% spread. They were not built there because nobody had noticed they could
be. Nothing blocks them.

**Second correction, filed while writing [Direction](../direction/overview.md).** These two, plus
the `transport/*` workloads below and client-side `tracing` spans, are step 0 of that whole chapter
— **nine design pages, and not one of them can be adjudicated until they exist**. ~~`wire_codec` is
what would catch a [D2](../direction/framing.md) header that accidentally unaligned the payload~~ —
it is built ([F10](../features/framing-and-protocol-evolution.md)) and that claim about it was
wrong: a criterion benchmark measures nanoseconds and a misaligned rkyv access is a `bytecheck`
failure or undefined behaviour, so the alignment guard is a unit test and `wire_codec`'s value is
the three questions it does answer — what the header costs, what validating an arriving bundle
costs ([O1](optimizations.md)), and what building a response costs ([O2](optimizations.md)). The
plaintext-versus-TLS pair [D4](../direction/encryption.md) needs is a *precondition* rather than
a follow-up; and `routing` is a hard dependency of [D7](../direction/shard-aware-routing.md), whose
whole value rests on a hop nobody has measured. That raises what these are worth considerably
above what this entry claimed when it was filed against four `O` numbers.

**The `transport/*` half of that is now built** ([F13](../features/transport-workloads.md)), which
takes the D4 precondition with it — the plaintext arms exist and ~~the TLS arms are one axis away~~
~~**the TLS arms are built too** ([F14](../features/encryption-in-transit.md)), so the pair is
complete at sixteen arms and has not been captured.~~ **It has now been captured**, as
`f14-encryption`, together with forty-eight sweep arms that vary row width, load depth and client
count. See [F14](../features/encryption-in-transit.md#performance). What that capture did *not*
answer is the connect cost, for the reason [O30](optimizations.md) now records.
The client-side `tracing` spans and the two micro benchmarks are still missing, so the *subtraction*
this paragraph wanted is not available: a transport sample bounds the client and the server
together and does not separate them.

**A table-layer bench, over `PersistentSortedTable::get`.** The gap that was not known to be a gap.
The micro layer stops at `SortedPartition`, so everything between a query arriving at a table and
reaching a partition is unmeasured: the `partitions` and `blocked` map lookups, `to_blocked`,
`PendingGet`'s slot bookkeeping, and the replay path a get takes when it parks on a disk read.
`optimizations.md` listed **O5**, **O12** and **O13** as adjudicable by `partition_sorted/*` and
`seek_bytes/*`; none of those benches builds a table at all, so all three were uncovered while
appearing covered. O13 is the reason this matters rather than a bookkeeping point — its quadratic
term migrated from the blocked path onto the resident get path, widening as it went, and no
benchmark was positioned to notice. What is needed is a get over *n* partition keys, *n* varying,
against both a resident table and one whose partitions have to be read, so the O(n²) term is
visible as a curve rather than argued from the source.

The obstacle is that a `PersistentSortedTable` needs a storage backend and a loader channel, so
this benchmark needs a glommio `LocalExecutor` inside criterion — the same `iter_custom` problem
the write-path benchmark above has, and a reason to solve it once for both.

**Half answered from above, and the criterion half is still open.**
[F8](../features/purpose-built-workloads.md) built `macro/fanout/{resident,evicted}/n` across
*n* ∈ {1, 2, 4, 16, 64, 256}: a get over *n* partition keys, both arms, exactly the shape this
item asks for. It needed no executor inside criterion, because driving
`PersistentSortedTable::get` through a live server and a real client does not need one — the
obstacle is real from below and absent from above.

It is **not** the benchmark this item asks for and does not close it. Every fanout sample includes
the wire, the routing, `split_by_shard`, the response merge and the client, so a number from it is
not a cost of `PersistentSortedTable::get`; it is a cost of a query that reaches one. What it can
do is answer the *question* O13 poses, since a quadratic term bends the curve against a flat
control at *n* = 1 whatever constant overhead sits on top. What remains here is the isolated,
criterion-sampled version — and that still wants the executor.

**Half of this closed on the way past.** [F4](../features/validated-archives.md) gave `MaybeLoaded`
a defaulted buffer parameter, which made the enum constructible outside a running server for the
first time, and `partition_sorted/maybe_loaded/*` now measures the real `get` and `exists` on both
arms. That is one layer below where O5, O12 and O13 live, so **it does not adjudicate any of them** —
but it does mean the boundary this gap describes moved from `SortedPartition` up to
`PersistentSortedTable`, and that the remaining obstacle really is the executor and nothing else.
It also settled the shape the eventual bench should have: a control that the change cannot reach and
a null on the other arm of the same dispatch, which is what caught
[O24](optimizations.md#o24-two-benchmarks-move-with-the-shape-of-the-binary-around-them).

**A fifth gap, added by [F5](../features/flushed-sweep-gate.md): the shard loop is unreachable from
a test.** `Shard` has no `#[cfg(test)]` module, and constructing one takes a live glommio reactor, a
ring, a channel mesh and a storage backend, so the loop's control flow — flush-when-idle, the
flushed sweep gate, the eviction trigger — is only ever exercised end to end through a real server.
F5 could pin the two *premises* its gate rests on as unit tests, but not the gate. That is why its
integration coverage catches a broken gate by hanging rather than by failing an assertion, which is
the worst way to learn something is wrong. This is the same executor-inside-criterion obstacle as
the two benchmarks above, arriving from the test side instead, and it is a third reason to solve it
once.

~~**Per-query macro timing.** `--per-query` was specified and not built. The macro harness still
takes one `Instant` per batch and copies it across every query in it, so there is no per-query
service time.~~ **Done.** [F8](../features/purpose-built-workloads.md) added a second driver: a
`per_query` workload runs at a bounded concurrency with one query per slot, so send-to-response is
that query's service time. Both drivers exist on purpose — saturating is still the only way to
measure throughput, and stamping each query *under* saturation measures queueing rather than
service. Which of the two a number came from is recorded per workload in the artifact, and a
comparison never joins one to the other. The batch-level numbers were kept for exactly the reason
this item gave: the recorded spread figures were measured that way.

**Multi-capture comparison, on the micro layer.** ~~`scripts/compare.sh` takes one capture per
side.~~ `shoal-bench compare` still does, for the micro layer. It needs to take
several, because one cannot be trusted: across four identical repeats `get_key/4096` moved 22%
and reported its outlying value with a ±0.2% confidence interval
([Performance Baseline](../performance/baseline.md#what-the-micro-layer-can-actually-resolve)).
The protocol therefore requires a confirming repeat, and the tool cannot express it — the
confirmation is a manual step today, which means it is a step that will be skipped. Taking
`--against` several times per side and comparing observed ranges rather than point estimates
would fix it, and would also give a real per-benchmark noise band instead of the two duration
tiers, which are themselves only an approximation fitted to four repeats.

~~The same is true of the macro layer.~~ **Done for the macro layer.** [F7](../features/bench-runner.md)
made every macro capture keep each run's own distribution, and a macro comparison is judged on
whether the two observed intervals are disjoint rather than on a percentage. What remains is the
micro half above. Note the asymmetry is not an oversight: the macro layer already ran five times
per capture and only threw the runs away, whereas making the micro layer do the same means
running criterion several times over, which costs minutes rather than seconds.

**Provenance drift in `docs/perf/sources.json`.** [F7](../features/bench-runner.md) decides whether
a capture still describes the current code by hashing the sources each layer measures, and the
list of those sources is maintained by hand. It will drift. The design makes drifting safe in one
direction only — a missing path yields a capture wrongly called *unaffected*, never one wrongly
called *fresh* — but a `--strict-stale` mode that treats any commit move as stale regardless of
digests would give a way to distrust the list deliberately.

~~Two smaller things: the macro layer needs a 65 MB dataset that is not in the repository and that
no script fetches, so a clean checkout cannot reproduce that layer at all;~~ **the dataset is
gone** — every workload generates its rows from `--seed`, so a clean checkout reproduces the macro
layer with nothing fetched ([F8](../features/purpose-built-workloads.md)). Still open: `client.rs`
has neither `tracing` spans nor `hotpath` scopes, so the share of measured latency that is the
harness's own is unknown.

### The row-size axis

Six benchmarks that between them would turn most of
[Row size and what it costs](../tables/row-size.md) from an argument into a measurement. That page
names four mechanisms behind the throughput fall past a kilobyte and could isolate none of them,
because the axis had a 64× hole in it, ran at one load depth, ran at one mixture, and had no
per-stage attribution at more than one width. Ordered cheapest first, which is also the order they
were worth reaching for.

**All six are built** ([F22](../features/row-size-benchmarks.md)) and `f22-row-size` **captured
them**. Five answered; the sixth joined zero queries and answered nothing
([item 76](known-issues.md#76-the-stage-layer-joins-nothing-for-any-grid-arm-and-reports-it-as-a-layer-that-ran)).
Each entry below is struck through with what it turned out to cost, including the two places it cost
more than it said, and now with what it said. What none of the six closes is at the end.

**What they came back with**, shortest form — the argument is on
[Row size and what it costs](../tables/row-size.md#what-it-settled--five-of-six-ran):

| # | Verdict |
| ---: | --- |
| 1 | Answered. Response decode ×432.8, encode ×72.2 over 64 B → 64 KiB, against a control flat to 0.25% |
| 2 | Answered, **and the entry's shape was wrong** — 1.22× at 64 KiB rows, but the gain is in records per buffer, not at the threshold |
| 3 | Answered, and the knee is in the *tail* rather than in throughput: 52× p99/p50 at 128 KiB, recovering to 7.4× at 4 MiB |
| 4 | Answered, and it was most of the wide-arm latency — 1.3–2.5× spread at every width at depth 1 |
| 5 | Answered. Write path owns below ~64 KiB, read path above it |
| 6 | **Did not run.** Zero joined records at all three widths |

**The new gap, and it is cheap.** Filling the 8 KiB → 512 KiB hole left the *other* one: the fixed
widths still jump 1024 → 8192, and the intent log's staging buffer sits at 4096 in the middle of it.
Two arms at 2 KiB and 4 KiB on the persistent unsorted table would bracket the boundary and say
whether there is a discontinuity there at all — which is the one thing
[O34](optimizations.md#o34-a-record-wider-than-the-staging-buffer-defeats-intent-log-batching) still
cannot state, now that the `latency_buffer` sweep has established the setting is worth 1.22× at
64 KiB. Two arms, appended after the existing widths so no identifier moves.

~~**A width axis on `wire_codec`.** The cheapest item in this whole section. `shoal/benches/wire.rs`
sweeps *bundle size* (1, 10, 100 queries) and *response cardinality* (16, 256, 1024, 4096 rows) with
a fixed row of about thirty bytes, so it never varies payload **width** at all. Adding a width axis
measures the per-byte half of [O1](optimizations.md) and [O2](optimizations.md) directly, in the
micro layer, with criterion's confidence interval rather than a wall clock — and it needs no server,
no executor and no storage backend. Seconds of machine time, one file.~~ **Built**
([F22](../features/row-size-benchmarks.md)), as a second row type and four `wire_codec/width/*`
groups rather than a width added to the existing ones — widening `TitleByKeyword` would have kept
all thirty nine existing identifiers and changed what every one of them measured, against a frozen
baseline. Right that it was one file and seconds of machine time. Not yet captured.

~~**The `latency_buffer` sweep repeated above the buffer.** `macro/conf/storage/latency_buffer/*` runs
its five rungs at the grid's reference cell of 1 KiB, which is below the step at 4096 that
[O34](optimizations.md) is about, so it reports 1.06× and a `yes` in the *Real?* column for a setting
whose actual effect it cannot see. The same five rungs at 8 KiB — and ideally again at 64 KiB —
adjudicate O34 outright. Ten arms.~~ **Built** ([F22](../features/row-size-benchmarks.md)), at
exactly ten arms. A repeat carries its width in its identifier and the existing forty eight do not,
so no capture was orphaned, and `arms::conf_sweeps` keys on the width as well as the knob — a knob
at two widths is two sweeps, or the difference between the widths reads as a difference between two
values of the setting. Not yet captured.

~~**Fill the 64× hole: 16, 32, 64, 128 and 256 KiB.** The width sweep goes 8 KiB → 512 KiB with nothing
between, so the knee's location and sharpness are inferred rather than measured, and a step at 4096
is indistinguishable from a slope that starts near it. Twenty arms across the four tables. **Note
what it costs:** this edits `grid.rs`, which moves the source fingerprint of *every* grid workload,
so every existing capture is correctly reported as no longer describing them. That is a reason to
batch it with the two items below rather than to skip it.~~ **Built**
([F22](../features/row-size-benchmarks.md)), batched with the two below exactly as this entry said
to, at twenty arms. The widths are a second array rather than five entries spliced into `WIDTHS`,
because a workload's position in `workload_ids::IDS` decides its port. This entry was right about
the fingerprint cost and it did not mention the other one: the grid growing in front of the
configuration sweep re-ported all forty eight of its arms. Not yet captured.

~~**A depth-1 arm at each width.** Every grid arm runs at 32 outstanding queries, at every width. At
4 MiB that is 128 MiB in flight against a key space of sixty four partitions, so the arm measures
queueing and partition contention as much as service time — and a latency past the knee of a
throughput curve is a measure of queue depth, which is the condition
[Tuning](../operations/tuning.md#start-here) says invalidates everything else. Eleven arms on one
table separate the two. This is [what F17 left undone](#what-f17-left-undone)'s depth-ladder entry,
and the wide end is where it bites hardest.~~ **Built**
([F22](../features/row-size-benchmarks.md)), at fifteen arms rather than eleven — the axis had grown
by the time it was taken, and the reference width was already `macro/grid/depth/1`, which the two
ladders now share rather than mint twice.

~~**The width axis repeated at `r0` and `r100`.** The 1 KiB → 8 KiB divergence between the persistent
and ephemeral halves is a *write*-path effect measured under a mixture that is half reads. Sweeping
width at `r0` says how large it is when the whole mixture is writes, and at `r100` isolates the
read-path per-byte cost with no intent log in the way. Twenty-two arms per table, or eleven if only
`r0` is taken. This is the concrete case behind
[what F17 left undone](#what-f17-left-undone)'s "no interaction between axes".~~ **Built**
([F22](../features/row-size-benchmarks.md)), at both ends and on all four tables rather than the one
this entry costed — 120 arms. It needed no renderer change to be *selected* correctly, because the
existing charts filter on the read share an arm recorded rather than on its name.

~~**The stage breakdown at more than one width.** The one that would change the most. The stages layer
records nineteen points per query and runs against a single workload, so nothing says *which* stage
grows with bytes. Running it at 1 KiB, 8 KiB and 512 KiB would attribute the fall to `decode`,
`durable_write`, `reply` or the socket, and would replace most of the argument on the row-size page
with a measurement. It needs no new workload — only the `stage-profile` build pointed at three
existing arms instead of one.~~ **Built** ([F22](../features/row-size-benchmarks.md)). Right that it
needed no new workload and wrong that it needed nothing else: the two instrumented layers shared one
list, so pointing the stage layer at three arms would have tripled the hotpath phase, and both
layers handed every workload the *same* artifact path, so the second report would have landed on top
of the first. That second half is [item 73](known-issues.md), half fixed and half still open. The
list and the artifact are now per layer and per workload.

**What the six do not close.** The grid is still a cross rather than a cube: `r0` and `r100` are
swept at a load depth of 32 and the depth-1 ladder is swept at `r50`, so a cost that appears only at
one query outstanding under a pure write mixture is invisible to both. The depth-1 ladder is one
table, so the persistent-against-ephemeral subtraction that makes a width effect attributable to
storage ([F9](../features/ephemeral-tables.md)) does not exist for the depth axis — the cheapest
thing that would fix it is the same fifteen arms on `unsorted_mem`. The `wire_codec` width axis
stops at 64 KiB, because every criterion sample builds a response and sixteen 64 KiB rows is already
a megabyte of it. And the per-stage breakdown is drawn at the mean of every query rather than at a
rank, so *which* stage makes the tail is a question nobody has asked yet; the report already holds
six ranks, so this is a renderer change and not a capture.

### What F8 left undone

Workloads were specified and deliberately not built, to keep the first set reviewable. **Five of
them are one file each against an existing trait**, and the harness already carries everything they
need:

- **`sort_select/{all,keys,range}`** — the three arms of `SortSelect`.
  [F1](../features/sort-key-ranges.md) shipped with no benchmark at all and this is what would
  give it one.
- **`projection/{full,projected}`** — a narrow projection against a whole wide row. Same for
  [F2](../features/projections.md); the `ItemKeys` projection already exists in the workload
  schema for it.
- ~~**`transport/{send_one,send_batched,stream,stream_unordered}`** — the four client transport
  modes over an identical query mix. This is also the only thing that could say how much of a
  measured latency is the harness's own, which is the open item above, **and it is what every page
  of [Direction](../direction/overview.md) is blocked on** — the client is the one layer of this
  system whose total has never been bounded.~~ **Built**
  ([F13](../features/transport-workloads.md)), as **eight** workloads rather than four. This entry
  specified the mode axis and missed the one that decides whether the set can do its job: a second
  axis on row width, a 256-byte row against a MiB one. The page most blocked on these is
  [D4](../direction/encryption.md), whose question is a *per-byte* cost, and four 256-byte
  workloads would have answered it with a number near zero — at that width the fixed per-response
  costs dominate and the four modes spread nearly 4×, while at a MiB they collapse into one number
  because the wire is the whole cost. What remains open is the subtraction the entry hoped for: a
  sample still includes the server's work, and separating the two needs the client-side spans
  [O28](optimizations.md) asks for.
- ~~**`durability/{fsync,async}`** — see the `Async` vs `Fsync` item above.~~ **Built**
  ([F20](../features/configuration-sweeps.md)), as `macro/conf/storage/durability/r50/{fsync,async}`.
  Note it is *not* under `macro/durability/` as this entry names it: a configuration arm belongs
  with the other configuration arms, because what it varies is the server rather than the query.
- **`mutate/{update,delete,exists}`** — entirely unmeasured today.

**Two need something that does not exist yet**, and are not one file each:

- **`compaction`** needs a way to force a compaction and observe it from a client, which means a
  server-side counter first. Its wall clock would otherwise be dominated by the write path it is
  trying to isolate.
- **`recovery`** needs a second `ShoalPool::start` in one process — which is the thing
  [F8](../features/purpose-built-workloads.md) deliberately avoided, since glommio pins its shards
  at start and there is no supported way to undo that — or the two-process crash-test shape in
  `shoal/tests/utils.rs`. The `RestartAfterSeed` arm restarts *between* phases, in separate
  process lifetimes, which is not the same thing.

**A readiness signal in `ShoalPool::start`.** F8 replaced the five-second sleep with a probe that
retries a real query until one is answered, which is correct but is still the client working
around a missing server facility. `ShoalPool::start` spawns its shard threads and returns without
joining them, so there is no moment at which the pool knows every shard has bound. Giving it that
moment would remove the probe, and a readiness endpoint needs exactly the same thing — as does the
pool-wide recovery summary described earlier on this page.

**The evicted fanout arms warm up as they run.** A restart empties memory, but the first query to
touch a partition faults it back in and it stays there. The fanout arms read 4,096 partitions
repeatedly, so at *n* = 256 a full run touches each about fifteen times and roughly 93% of its
reads hit something already resident. The consequence is visible in `f8-powersave`: the evicted
arms' medians converge on the resident arms' as *n* rises while their p99s separate by up to 11×.
The cold reads are in the tail rather than the middle. Fixing it means either many more partitions
— which makes the seed phase dominate the run — or evicting between queries, which measures
eviction as well as reading. Neither is obviously right, which is why neither was done.

**Confirming the O13 curve.** `macro/fanout` was built to make O13's quadratic term visible and
the first full capture does not establish it — see
[Optimizations](optimizations.md#which-entries-a-benchmark-can-currently-adjudicate). What is
needed is a capture on a `performance` governor and a committed tree, and a second one confirming
it, which is the protocol this repository already requires of the micro layer. Until then O13
remains argued from the source.

**A frozen macro reference.** `docs/perf/baselines/B1-performance.json` is the frozen *micro*
baseline and is unaffected by F8. There is no frozen macro reference for the new workloads:
`f8-powersave` was taken on a `powersave` governor and a dirty tree, so it is a first capture
rather than a baseline. Establishing one means a `performance`-governor run on a committed tree,
committed as `B2-workloads`. Note `promote` cannot enforce this — it only knows about micro
baselines — so it is a convention rather than a mechanism.
[F17](../features/workload-grid.md) sharpens what such a reference should be taken over: the grid's
reference cell is the one point four separate sweeps cross at, so a `B2` that covered the grid would
be a reference for the whole cross rather than for a list of unrelated workloads.

### What F17 left undone

The grid is a **cross, not a cube** — each axis swept fully against a fixed reference of the others.
What that buys is a capture of four to five hours instead of an overnight one; what it costs is
every entry below.

- **No interaction between axes is measured.** A cost that appears only at a wide row *under a
  write-heavy mixture* is invisible to both sweeps, because the width sweep runs at `r50` and the
  mixture sweep runs at 1 KiB. The cheapest thing that would find one is a third sweep at a second
  reference — say the width axis again at `r0` — which is eleven more arms per table rather than the
  two hundred and sixty four a full cube costs. ~~Nobody has looked for such an interaction; the
  claim that there is none is an assumption, not a finding.~~ **Somebody has now looked, and the
  assumption is wrong.** Over the octave 1 KiB → 8 KiB the persistent arms lose 31% of their
  throughput and the ephemeral arms lose 6%, which is a width effect that lives entirely in the
  write path and is being measured under a mixture that is half reads. The `r0` sweep this bullet
  proposes is exactly what would size it, and is [filed above](#the-row-size-axis) with the reason.
  See [Row size and what it costs](../tables/row-size.md). ~~The `r0` sweep this bullet proposes is
  exactly what would size it~~ — **that sweep is built**
  ([F22](../features/row-size-benchmarks.md)), at both ends of the mixture and on all four tables
  rather than the eleven arms per table this bullet costed. ~~It has not been captured, so the
  interaction is still known to exist and still unsized.~~ **Captured and sized**: over 1 KiB →
  8 KiB the pure-write arm falls to 58.8% of its own 64 B rate while the pure-read arm holds at
  96.7%, so that octave is the write path and essentially nothing else. Past ~64 KiB the two cross
  and the read path falls about three times as fast. The interaction is real, it is now measured at
  both ends of the mixture, and what remains unmeasured is the *cube* — a cost that needs a pure
  write mixture **and** a particular load depth together is still invisible.
- **YCSB workload E (short range scans) and F (read-modify-write) are not built.** E needs a scan
  over a sorted table whose partitions hold many rows, which is a different seeding shape from
  anything the grid does — every grid arm writes one row per partition so that the four tables stay
  comparable. F needs a read and a write of the same key inside one logical operation, which the
  disjoint-range write scheme deliberately makes impossible. Both are real gaps against the
  published YCSB set and both are more than one file.
- **Writes are inserts, never updates in place.** The reasoning is on
  [F17](../features/workload-grid.md) and it is sound, but it means the update path — the one an
  `#[shoal(update)]` query drives — is measured by nothing at all, which the `mutate/*` entry above
  already asks for.
- **The depth ladder covers one cell.** Every other arm in the grid is assumed to sit at the same
  point on its own throughput curve as the reference cell does, and that assumption has not been
  checked at the wide end, where the byte budget makes an arm's query count two orders of magnitude
  smaller. **The wide end is now known to be where this bites.** At 4 MiB the depth of 32
  is 128 MiB outstanding against a key space of sixty four partitions — fewer keys than the load is
  deep — so those arms measure queueing and partition contention as much as service time, and
  [Row size and what it costs](../tables/row-size.md#what-the-capture-cannot-tell-you) cannot
  attribute its own percentiles because of it. ~~A depth-1 arm at each width is
  [filed above](#the-row-size-axis).~~ **Built** ([F22](../features/row-size-benchmarks.md)): the
  whole width axis at one outstanding query, on the persistent unsorted table, crossing the ladder
  at `macro/grid/depth/1`. Every arm that is not on that table is still assumed to sit where the
  reference cell does.
- **The skew sweep measures a resident table.** Its gap is locality inside a table that fits in
  memory, not a hit rate against disk, so it is a floor on what skew is worth rather than an
  estimate of it. Measuring the other case needs a working set deliberately larger than
  `resources.memory`, which is a seeding shape nothing here has.
- **No comparison against another database.** The identifiers and `ScaleFacts` are shaped so a
  foreign system's numbers could be described in the same schema — the axes are recorded as fields
  rather than only in the identifier string, which is the part that would otherwise have to be
  reverse-engineered. Nothing that would consume such a capture is built, and a fair comparison
  needs more thought than a schema: the other system's client, its durability setting and its own
  saturation point all have to be argued about before a number means anything.

### What F20 left undone

The configuration sweep is a cross for the same reason the grid is, and pays the same price. It also
inherits one gap the grid does not have: it measures a machine as much as it measures a database.

- **No interaction between two settings is measured.** A write-behind depth that only pays off at a
  large buffer shows as two flat sweeps, and nothing would say so. The cheapest thing that would
  find one is a second reference for the storage half — the write-behind ladder repeated at
  `latency_buffer/64Ki` — which is five more arms rather than the twenty-five a full cross of those
  two costs. As with the grid: the claim that there is no interaction is an assumption.
- **The storage knobs are swept at `r50` only.** A setting that only bites under sustained writing
  is being asked half a question. Repeating the six storage sweeps at `r0` is twenty-three more
  arms and would say whether the barrier's group-commit behaviour changes the shape of any of them.
- **The storage knobs are swept at one row width as well**, and for `latency_buffer` that is worse
  than the mixture gap above. The sweep runs at the reference cell's 1 KiB rows against a 4096 byte
  staging buffer, where three records already share an aligned write — so it is measuring the flat
  side of a step. `StreamWriter::prep` stops batching entirely once a record exceeds the buffer
  ([O34](optimizations.md)), which is where the setting's whole effect lives and where the sweep
  never goes. The 1.06× it reports is a `yes` in the *Real?* column for a question asked at the one
  width whose answer is no. ~~The five rungs repeated at 8 KiB are
  [filed above](#the-row-size-axis).~~ **Built** ([F22](../features/row-size-benchmarks.md)), at
  8 KiB and at 64 KiB, and the configuration page now labels every sweep with the width it ran at so
  the 1.06× row no longer stands alone. What this bullet did not say, and what building it showed:
  the *other* five storage knobs are still swept at 1 KiB alone, and nothing has asked whether any
  of them is a step too.
- **The memory sweep brackets one working set.** Its rungs are sized against the reference cell's
  own — about 1.6 MiB a shard from the seed plus half as much again from the run — so `1Mi` and
  `4Mi` straddle it and the rest are flat. What that cannot say is whether the *ratio* it finds
  holds at a working set a hundred times larger, where compaction and eviction have far more to do.
  Answering that needs a seeding shape deliberately larger than the limit, which is the same thing
  the skew sweep's entry above asks for; building it once would serve both.
- **Nothing checks that the recommendations are acted on.** `every_sweep_covers_the_shipped_default`
  asserts the sweep brackets what `shoal.yml` says; nothing asserts `shoal.yml` says what the page
  recommends. That is deliberate for now — the committed file is the baseline every historical
  capture was taken under, and retuning it invalidates all of them — but it means the page can
  recommend a value nobody ever adopts and no test will mind.
- **The results are a property of one machine and one device.** A buffer size is rounded up to the
  disk's O_DIRECT alignment and the shard curve is a curve in one box whose client shares its cores.
  Nothing here transfers to other hardware as a number. A second capture on different hardware would
  say which of the *shapes* transfer, and there is no second machine.
- **`ServerNeed::None` is still unused.** F20 configures servers rather than avoiding them, so the
  arm F8 added for a workload that drives engine internals with no server at all remains as empty
  as the entry above says.

### What F21 left undone

- **Nothing checks the groups cover the registry.** A workload in no group is reachable only by
  prefix, which is the state everything was in before groups existed — so a family added without a
  group silently reverts to it.
- **A group's summary can drift from its membership.** `every_group_selects_something` checks a
  group is non-empty. Nothing checks that `isolating` still describes what it holds, which is the
  same class of gap [F18](../features/results-pages.md) records for its page prose.
- **`FULL_MACRO_CAPTURE_SECS` is a hand-maintained constant.** Every `~capture` projection is
  proportionally wrong until somebody updates it, and nothing detects that it has gone stale. The
  honest fix is for a capture to record its own end-to-end duration in `CaptureMeta`, which nothing
  does today — the artifacts record what was measured and never what the measuring cost.

**A sorted table cannot have an integer sort key.** `RkyvSupport` is implemented for `String` and
for nothing else, so `#[shoal(sort)] at: u64` does not compile. The F8 workload schema works
around it with zero-padded decimal strings, which is what makes lexicographic order agree with
numeric order for a range scan. This is a missing impl rather than a design decision: nothing about
the sort path requires the key to be a string, and every sorted table in the wild will hit it.

### Archive checksums

Intent log records and the map snapshot are checksummed; archive payloads are not. Corruption
there is caught only if rkyv validation happens to reject it.

One class of thing that used to arrive here is no longer corruption at all. An archive that was
not on disk was created empty and read short, so validation was where "this file is missing"
surfaced, wearing the costume of a bad payload. That now fails at the open with an error naming
the archive ([Resolved #57](resolved/missing-archive.md)), which is worth knowing before reading a
validation failure as evidence of on-disk damage.

~~and several call sites `.unwrap()` that result.~~ Not any more.
[F4](../features/validated-archives.md) moved that validation to the one place a partition arrives
from disk and made it a `Result` there, so the thirteen `.unwrap()`s it used to be spread across are
gone. **That makes this entry more valuable rather than less**, for two reasons. Validation now
happens exactly once per read, so it is the single point where a checksum would belong beside it;
and it is the only thing standing between a corrupt archive and an unchecked read, which is now a
statement about one function rather than about thirteen call sites. The `access_unchecked`-everywhere
form of [O3](optimizations.md), which F4 deliberately did not take, is still gated on this.

### Quarantining a damaged intent log

A compaction now says when it deletes a log it could not read to the end
([item 44](resolved/compaction-tail-loss.md)), but it still deletes it, so an operator who reads
the warning has nothing left to look at. The obvious answer — keep the file — does not work as
stated: a log left in the intent directory is not inert, because `find_inactive_intent_logs`
matches on the name, so every subsequent startup replays it, hits the same damage, and re-drops
the same tail forever.

What is wanted is a rename out of that pattern — a `.corrupt` sideline the compactor moves the
file to instead of removing it — plus something that stops those accumulating without bound. Both
halves are small; the reason this is filed rather than done is that neither has a caller yet.
Nothing reads a quarantined log, and until a repair tool or an operator workflow exists, moving
files into a directory nothing ever looks at is worse than the warning alone.

The same argument applies on the recovery path, which discards a damaged tail with the same
finality ([Recovery](../storage/recovery.md#truncation-and-corruption)).

### Storage engine abstraction

~~`StorageSupport` and `Loaders` are written as extension points but have exactly one
implementation. Until a second exists, treat the abstraction as unproven — a memory-backed engine
for tests would be the natural first user.~~ [F9](../features/ephemeral-tables.md) built that
second user, and it was indeed the natural one: `NoStorage` is a memory-backed engine, and adding
it needed one new `Loaders` variant and one guard in the generated loader spawn — the trait itself
did not have to move.

**What it did not prove.** `NoStorage` answers "nothing" to most of the trait. Every method that
describes how bytes reach a device — `commit`'s buffering, `read_intents`, `load_partition`,
`fill_durability` — still has exactly one real implementation, so a second *persisting* engine
would still be the first thing to exercise them.

The original note's warning stands and is worth repeating: the storage tests deliberately do *not*
want a memory-backed engine underneath them. They run against a real filesystem on purpose,
because glommio silently disables O_DIRECT on tmpfs, and running them on `NoStorage` would hide
exactly the alignment and `fdatasync` behaviour they exist to check.

### Build and packaging

- No `rust-toolchain.toml` despite requiring nightly, so the failure mode is a confusing
  compile error ([Building](../getting-started/building.md)).
- The `../glommio` path dependency makes the build non-reproducible from this repository
  alone and blocks publishing.
- No CI configuration in the repository.
- The workspace is split across two editions — `shoal`, `shoal-core` and `shoal-derive` are 2021;
  `shoal-bench` and `shoalctl` are 2024. That is not a problem in itself, but
  [item 35](known-issues.md#35-a-refcell-borrow-is-held-across-three-awaits-in-the-compactor) turns
  correct for free under 2024's temporary scoping, so bumping `shoal-core` would silently change a
  latent defect into a non-defect. Whoever bumps it should read that item first and delete it
  deliberately rather than discover it was fixed.

### Documentation

Two things that no test, and no build, will ever notice.

**Nothing checks the links.** `docs/src/` holds about 1,175 internal links and anchors.
`mdbook build` verifies that a file exists — `create-missing = false` in `book.toml` — and says
nothing about a `#fragment` that names a heading which has since been reworded. The
[August 2026 review](review-2026-08.md) resolved all of them by hand and found four broken,
three of which were anchors. A `mdbook-linkcheck` backend, or thirty lines of script in CI, closes
this permanently; it is filed rather than done because there is no CI to put it in, which is the
item above.

**Nothing checks `CLAUDE.md`.** It names APIs it does not exercise and is read before every change,
which is the worst combination available; four of its claims had gone stale by the time anyone
looked ([Resolved #25](resolved/claude-md-drift.md)). The cheapest real improvement is not a
checker but a deletion: its usage sketch restates what `shoal/examples/tmdb.rs` already does
correctly and is compiled, so shrinking the sketch to a pointer removes the only part of the file
that can name a function.

### Tests

Covered and uncovered flows are catalogued in [Test Coverage](test-coverage.md), along with the
counts from an actual run. The short version: compaction and archive rotation, multi-log recovery,
the streaming client APIs, and concurrency are the gaps worth closing first — and the suite's test
binaries all bind the same ports, which
[item 38](known-issues.md#38-integration-test-binaries-all-bind-the-same-ports) covers.

Multi-shard routing is no longer on that list; it gained coverage with
[item 7](resolved/sorted-limit.md).

## Dead code

| Location | Status |
| --- | --- |
| `server/cursor.rs`, `server/response.rs` | Not in the module tree; reference removed APIs. |
| `shoal-core/src/client.rs:549-603`, `:1048-1114` | Large commented-out blocks. |
| ~~`.../fs.rs:74-98`~~ | ~~The previous intent-log writer, commented out.~~ **Gone** — the block is no longer in the file. |
| `shoalctl/src/components/tab.rs:548`, `:558` | `next`/`prev`, never called — the compiler warns about them on every build. |
| ~~`EphemeralTable`~~ | ~~Cannot be used in a `#[db]` database.~~ Deleted by [F9](../features/ephemeral-tables.md), which replaced it with aliases over the persistent tables. |
| `shoal/examples/basic.rs.bak` | A `.bak` file in the source tree. |
