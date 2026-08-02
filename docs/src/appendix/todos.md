# TODOs and Unbuilt Work

Two lists: the `TODO` markers actually present in the source, and the larger pieces that the
code implies but does not contain.

Open defects are catalogued separately in [Known Issues](known-issues.md), and fixed ones in
[Resolved Issues](resolved-issues.md). Several entries here overlap; where they do, the issue
entry has the detail.

## In-code TODOs

Ten `TODO` comments and one live `todo!()` outside `target/`. The two struck-through rows below
have been done and are kept for their links.

### Storage

| Location | TODO | What finishing it involves |
| --- | --- | --- |
| ~~`.../fs/compactor.rs:201`~~ | ~~`does anything else need to be done to remove this partition from archive maps?`~~ | **Done.** The answer was yes. `MapIntent::Remove` was added, the prune path logs it and drops the entry from `to_archive` after the sync: [Resolved Issues #5](resolved/resurrected-deletes.md). |
| `.../fs/compactor.rs:343` | `make size configurable` | Move `MIN_ARCHIVE_COMPACTABLE` and the hardcoded 50% utilisation threshold into `FileSystemTableConf`. |
| `.../fs/map.rs:351` | `make issue about SerializedMap not needing to track active` | `SerializedMap` does not persist the active archive id, so every restart mints a new one and orphans the previous active archive until compaction reclaims it. Either persist it or document the churn as intended. |
| `.../fs/loader.rs:128` | `todo!("Add back onto loader channel")` | A live `todo!()`. A load request that fails to spawn should be requeued rather than panicking the loader — which currently strands every query blocked on that partition forever. |
| `.../fs/loader.rs:137` | `handle this error` | Loader task errors `panic!` during shutdown drain. |
| `.../fs/loader.rs:157` | `do something with this error` | Same, on the steady-state path. |

### Server

| Location | TODO | What finishing it involves |
| --- | --- | --- |
| `shard.rs:60` | `do something with this error` | `client_rx_relay` panics on any non-EOF socket error. Should tear down the one connection. |
| `shard.rs:126` | `detect collisions?` | Client UUIDs are generated without checking `client_map`; a collision panics at `shard.rs:623`. The client does exactly this check for query ids (`client.rs:192-203`) and could be copied. |
| `shard.rs:132` | `do this with a task queue?` | Per-client relay tasks run on the executor's default queue, so client IO is unprioritised relative to background writes. |
| ~~`.../persistent/unsorted.rs:867`~~ | ~~`handling a partition missing`~~ | **Done.** The startup-path `panic!` is a `warn!` and a skipped intent now. See [Resolved Issues #9](resolved/orphaned-update-intents.md). |

### Client and UI

| Location | TODO | What finishing it involves |
| --- | --- | --- |
| `client.rs:82` | `implement a ping/pong type request?` | `is_valid` calls `peer_addr()`, which cannot detect a dead peer. Needs a protocol-level ping, which needs a message-type field the wire format does not have. |
| `client.rs:1272` | `make it so we don't need to do this` | `ShoalQueryStream::send` overwrites `queries.id` on every bundle. The stream's id should be set at construction. |
| `shoalctl/src/app.rs:436` | `Handle insert mode for editing rows` | Insert mode edits the query bar only; result rows are read-only. Writing would also need SHQL to parse mutations. |

## Larger unbuilt work

Implied by the code's shape but not present.

### Distribution

`ShardContact` has one variant:

```rust
pub enum ShardContact {
    /// This shard is on our current node
    Local(usize),
}
```

`shoal-core/src/server/shard.rs:167-172`

The `match` in `Comms::send` (`comms.rs:46-56`) has one arm. Everything above it — the tablet
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

`ResponseAction` can express only booleans and rows
(`shared/responses.rs:26-38`). A server-side failure has nowhere to go, which is why the
server is full of `panic!`s — there is no way to say "that query failed" to a client. Adding
an error variant would unlock replacing most hot-path panics with recoverable errors
([Known Issues #16](known-issues.md#16-panics-on-the-hot-path)).

### Backpressure

Every channel is unbounded ([Known Issues #15](known-issues.md#15-no-backpressure-anywhere)).
Bounding them requires deciding what to do when a shard is saturated — shed load, block the
coordinator, or reject the client — which requires the error channel above.

### Timeouts

Nothing anywhere has a deadline: no query timeout on the client, no timeout on a blocked
query waiting for a partition, and no timeout on the connection pool beyond the initial
connect. A partition load that never completes parks its queries permanently.

### Archive map reconstruction

Archives write a size prefix before each partition specifically so a map could be rebuilt by
scanning — the comment says so (`.../fs/compactor.rs:220-221`). No such path exists, so
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
by the library, only by the example binary, so no test can observe any event the server emits
([Test Coverage](test-coverage.md)).

**Promote eviction drift to a `WARN`.** The eviction event now carries a `drift` field — the gap
between what a pass actually dropped and what the shard counter moved by, which is non zero only
when the counter had already floored ([Resolved #13](resolved/eviction-log-underflow.md)). It is
deliberately an `INFO` field rather than a warning, because
[item 22](known-issues.md#22-size-accounting-inconsistencies) makes drift ordinary and a warning
would fire on healthy runs. Once item 22 is closed, non zero drift becomes an invariant violation
and should say so at `WARN`. Doing it before then trains people to ignore it.

### Archive checksums

Intent log records and the map snapshot are checksummed; archive payloads are not. Corruption
there is caught only if rkyv validation happens to reject it, and several call sites
`.unwrap()` that result.

### Storage engine abstraction

`StorageSupport` (`.../storage.rs:225`) and `Loaders` (`:189-193`) are written as extension
points but have exactly one implementation. Until a second exists, treat the abstraction as
unproven — a memory-backed engine for tests would be the natural first user. Note that the
storage tests deliberately do *not* want this: they run against a real filesystem on purpose,
because glommio silently disables O_DIRECT on tmpfs and a memory-backed engine would hide
exactly the alignment and `fdatasync` behaviour they exist to check.

### Build and packaging

- No `rust-toolchain.toml` despite requiring nightly, so the failure mode is a confusing
  compile error ([Building](../getting-started/building.md)).
- The `../glommio` path dependency makes the build non-reproducible from this repository
  alone and blocks publishing.
- No CI configuration in the repository.

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
| `client.rs:544-598`, `:1025-1091` | Large commented-out blocks. |
| `.../fs.rs:74-98` | The previous intent-log writer, commented out. |
| `shoalctl/src/components/tab.rs:527`, `:537` | `next`/`prev`, never called. |
| `EphemeralTable` | Cannot be used in a `#[db]` database ([Table Types](../tables/table-types.md#ephemeraltable)). |
| `shoal/examples/basic.rs.bak` | A `.bak` file in the source tree. |
