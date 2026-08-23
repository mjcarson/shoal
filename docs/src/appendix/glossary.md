# Glossary

Terms as Shoal uses them. Several differ from their usual meaning in other databases; those
are called out.

---

**Accessible** — A partition state: raw bytes read from an archive, not yet deserialized.
Reads and filters run directly against the archived form. See
[Partitions](../tables/partitions.md#maybeloaded).

**Archive** — A uuid-named file holding many serialized partitions packed back to back.
Immutable once written; updated partitions are rewritten into the active archive and the old
copy becomes garbage. `<throughput_sensitive.path>/<table>/archives/<uuid>`.

**Archive map** — The per-shard, per-table index from partition key to `ArchiveEntry`
(`{archive uuid, offset, size}`). Also the authority on whether a partition exists on disk at
all. Persisted as a checksummed snapshot plus its own intent log. See
[Archives and the Archive Map](../storage/archives-and-map.md).

**Active archive** — The one archive currently receiving newly compacted partitions. Rotated
rather than compacted in place, to avoid invalidating entries written earlier in the same
compaction pass.

**Authentication mechanism** — How a peer proves who it is: `AuthMechanism::ScramSha256`, or
`MutualTls`, which is defined and refused until there is TLS. A client offers a *set* of them in
its `Hello` and the server names exactly one in its `HelloAck`, walking its own preference order —
so the server chooses and the client cannot negotiate itself down to the weaker of two. Zero means
"none", which is what a server requiring nothing writes and what every peer wrote before
[F12](../features/authentication.md).

**Batch timing** (`per_batch`) — A benchmark sample taken once per batch of queries and charged
to every query in it, so each is charged for the ones ahead of it. What a saturating workload
produces, and what **every** macro percentile recorded before
[F8](../features/purpose-built-workloads.md) is. Never comparable with a *service time*; the
artifact records which one a number is. Contrast **Service time**.

**Blocked query** — A query parked in `blocked: HashMap<u64, Vec<...>>` waiting for a
partition to be read from disk. Re-injected as a fresh `ServerMsg::Query` when the read
completes, rather than being resumed as a suspended future. See
[Query Execution](../tables/query-execution.md).

**check_disk** — A flag on `SortedPartition` meaning "there may be more of this partition in
an archive". Set on creation, cleared once the full archive copy has been merged in.

**Compaction** — Two distinct operations sharing one background task. *Intent compaction*
folds a sealed intent log into archives. *Archive compaction* reclaims space from archives
whose live fraction has dropped below 50%. See [Compaction](../storage/compaction.md).

**Connection-level error** — An error frame whose query id is nil. It is about the connection
rather than about any one query, and it ends the read loop that receives it. Every other error frame
names the bundle it belongs to.

**Control and null** — A pair of benchmarks differing in exactly one axis, one of which the
change under test cannot reach. The shape [F4](../features/validated-archives.md) settled on and
that caught [O24](optimizations.md). `macro/get_resident` and `macro/get_archived` are one; the two
arms of the fanout curve are another.

**Coordinator** — A *role*, not a component. The shard whose TCP listener accepted a client's
connection routes that client's queries to their owning shards. Any shard can be a
coordinator; there is no coordinator process, despite CPU 0 being reserved and called "the
coordinator cpu".

**Ephemeral table** — A table that keeps everything in memory and writes nothing to disk.
`EphemeralSortedTable` and `EphemeralUnsortedTable` are aliases for the persistent tables with a
`NoStorage` engine underneath, so they are the same tables rather than a separate implementation.
Nothing they hold is ever evicted, and nothing survives a restart. See
[F9](../features/ephemeral-tables.md).

**`NoStorage`** — The storage engine that stores nothing. The second implementation of
`StorageSupport` beside `FileSystem`, and what makes a table ephemeral.

**Decoy credential** — What `CredentialStore::lookup` hands back for a user that does not exist: a
salt derived from the username and a per-process key, the default iteration count, and two keys no
password derives to. It exists so that an unknown user and a wrong password fail at the same step,
in the same words, after the same work — a store that answered "no such user" early would make a
login a way to enumerate accounts. See [F12](../features/authentication.md).

**Error code** — The class of a failure, as a `u16` with pinned discriminants
(`protocol::error::ErrorCode`). It rides in both halves of the error channel: in an `Error` frame's
body, and inside `ResponseAction::Error` as a raw number, because the protocol module holds no rkyv
and the payload is archived. A code this build does not recognize reads back as `Unknown` rather
than failing to decode. See [F11](../features/error-channel.md).

**Error frame** — A frame a server sends when a query failed and no response can carry it — in
practice, when the response itself was too large to frame. Its body is a query id, a code, two
reserved bytes and a message. The query id sits at exactly the offset a response frame's does, so a
client reads one preamble for both and dispatches afterwards. Not to be confused with
`ResponseAction::Error`, which is the same failure attached to a query rather than to a frame.

**Evictable** — A partition eligible for eviction. `Accessible` partitions always are;
`Loaded` partitions only once their generation has been compacted. See
[Memory and Eviction](../tables/memory-and-eviction.md).

**Generation** — A per-table counter incremented on each intent log rotation. Names the epoch
whose writes are now sealed. A `Loaded` partition records the generation it was last modified
in; comparing against the flushed generation is what makes eviction safe.

**Intent** — One logged mutation: `Insert`, `Delete`, or `Update`. The unit of the write-ahead
log. `Insert` carries the whole row; `Update` carries only changed fields, which is why
replaying one requires the base partition.

**Intent log** — Shoal's write-ahead log, one per shard per table. Records are framed
`[size][gxhash checksum][rkyv payload]`. The *active* log receives writes; on rotation it
becomes `-inactive-<generation>` and awaits compaction. Confusingly, the archive map has its
own separate intent log. See [The Intent Log](../storage/intent-log.md).

**Loaded** — A partition state: fully deserialized in memory, tagged with the generation it
was last modified in.

**Loader** — `FsLoader`, the per-shard background task that reads partitions from archives and
posts them back as `ServerMsg::Partition`. Must only ever hold its own shard's channel — see
*unsafe Send invariant*.

**MaybeLoaded** — The enum wrapping every resident partition: either `Loaded` or `Accessible`.
The seam where lazy deserialization lives. Its buffer is a defaulted type parameter, which is what
makes the `Accessible` arm reachable from a test — see *ValidatedArchive*.

**MaybeRow** — Either a `Row` or a `Tombstone`. Sorted partitions store these rather than rows
directly.

**ValidatedArchive** — What `MaybeLoaded::Accessible` holds: a partition's archived bytes plus the
fact that they were validated when the read that produced them landed. Its only constructor
validates and its accessor does not, so a query seeks an evicted partition rather than re-running
rkyv's validator over the whole buffer first ([F4](../features/validated-archives.md)).

**Principal** — Who a connection belongs to: a name and the mechanism that proved it, produced by
a completed authentication exchange. Logged by the connection task and consulted by **nothing** —
it is what per-table authorization was filed as blocked on, and authorization is still unbuilt.
See [F12](../features/authentication.md).

**Partition** — The unit of storage, caching, eviction, and IO, addressed by a `u64` partition
key. A sorted partition holds a `BTreeMap` of rows; an unsorted partition holds exactly one
row.

**Partition key** — A `u64` produced by `gxhash`ing the `#[shoal(partition)]` fields. Because
it is a hash, **partition keys collide**, and nothing detects it.

**Projection** — A struct naming a subset of a table's fields, which a get can ask to be answered
with instead of whole rows. Declared with `#[derive(ShoalProjection)]` and listed on the database
field holding its table. A projection reads only the fields it names out of an archived row, and
answers in a response variant of its own so the client can name its type. It must carry its
table's partition key. See [F2](../features/projections.md).

**Identity projection** — The projection of a row into itself: a clone from a resident row, a
deserialize from an archived one. It is what a get that named no projection is answered with, which
is what makes a projected get and an unprojected one the same code path.

**Pending response** — A response held in `PendingResponse` against the intent log offset one
past its record, released once the durability watermark passes it. See
[Durability model](../storage/overview.md#durability-model).

**Recovery stats** — `RecoveryStats`, the counts of everything replaying a table's intent logs
had to discard. Three of its four counters mean data was lost; `updates_after_delete` does not,
and exists so the other three can be trusted. Summed across a shard's tables and reported once
when the shard finishes starting. See
[Recovery](../storage/recovery.md#what-recovery-discards).

**SCRAM-SHA-256** — The password mechanism, RFC 5802 with RFC 7677's hash. The server stores a
salt, an iteration count and two derived keys rather than a password; the client proves it knows
the password without sending it; and the exchange authenticates the *server* to the client as well,
through a final signature the client checks. Three round trips on top of the handshake.

**kTLS** — Kernel TLS. The handshake happens in userspace with rustls; the negotiated keys are then
handed to the kernel with two `setsockopt` calls per direction, and from that moment the kernel does
the record layer. `read()` returns plaintext into whatever buffer the caller names, which is why
encryption cost Shoal's response path no copy and no code — see
[F14](../features/encryption-in-transit.md). Needs the `tls` kernel module, which `setsockopt` does
not autoload.

**Upper layer protocol (ULP)** — The kernel's hook for stacking something on top of a TCP socket.
`setsockopt(TCP_ULP, "tls")` is what attaches its TLS module, and reading the option back is the
only way to tell a kTLS socket from a plaintext one from outside the kernel.

**Stored credential** — The four fields a server keeps per user: `salt`, `iterations`,
`stored_key`, `server_key`. None of them is a password and none can be turned back into one.
`stored_key` **is** still a secret: anything that can read it can replay it as a login.

**Ring** — The tablet map, still named `Ring` in the source. Maps a partition key to the tablet
holding it, and that tablet to the shard that owns it. Built whole from the shard count before
any shard starts. See [Partitioning](../architecture/partitioning.md).

**Tablet** — A slice of the partition key space named by the top 12 bits of the key, and the unit
ownership is recorded for. 4096 of them, assigned to shards round robin. Ownership is *stored*
per tablet rather than derived from a hash, which is what would let a tablet be moved between
shards. See [Partitioning](../architecture/partitioning.md#the-tablet-map).

**Service time** (`per_query`) — A benchmark sample covering one query and nothing else, taken by
running at a bounded concurrency with one query outstanding per slot. What the `per_query`
workloads [F8](../features/purpose-built-workloads.md) added produce, and the first such numbers
this repository has recorded. Costs throughput to measure, so a `per_query` workload's wall clock
is *not* a throughput figure. Contrast **Batch timing**.

**Shard** — One glommio executor pinned to one core, owning a slice of every table, its own
intent logs, archives, and background tasks. Named `Shard-N`, where N comes from a startup
counter, not the core id. Shard names become filenames, which is why shard count is part of
the on-disk format.

**SHQL** — Shoal Query Language. A small `SELECT`-only parser: `SELECT <*|projection> FROM t
WHERE f = v [AND ...] [LIMIT n]`. Equality and `IN` on any field, plus `<`, `<=`, `>`, `>=` on a sort key;
`WHERE` mandatory, no `ORDER BY`. Rows come back partition by partition in the order the query
named them, and in sort-key order within each, so a `LIMIT` takes the first of those. See
[SHQL](../api/shql.md).

**Sort key** — The `#[shoal(sort)]` fields, ordering rows within a sorted partition. It addresses
a row for a delete or an update, decides the order a read returns a partition's rows in, and
**selects** them: a get or an exists naming sort keys seeks those rows and answers about them
alone, and one bounding them by a range seeks the span between the bounds. A query narrowing
itself neither way asks for the whole partition. See **Sort select** below.

**Sort select** — `SortSelect`, the three ways a sorted get or exists can choose rows: `All`,
`Keys([..])`, or `Range(..)`. It is an enum rather than a set of fields so that "these keys *and*
this range" is not a state a query off the wire can arrive in. `All` is the only arm that means
every row; an empty `Keys` list means none. See [F1](../features/sort-key-ranges.md).

**Sort range** — `SortRange`, a pair of `Bound<Sort>`. An exclusive lower bound is a **cursor**:
handed the sort key of the last row of a page it names the next page, which is how a large
partition is paged through without reading all of it.

**Sorted table** — `PersistentSortedTable`. Many rows per partition, ordered by sort key.

**Staging buffer** — The one `DmaBuffer` a `StreamWriter` fills before writing it out. How many
records share it is what the group commit below has to amortize an `fdatasync` across, which is why
it sizes itself rather than being a constant. See
[The Intent Log](../storage/intent-log.md#how-wide-the-buffer-is).

**StreamWriter** — The DMA-aware append writer behind the intent log. Hands out buffer slices
via `prep`/`consume` and writes full buffers through detached background tasks. Sizes each staging
buffer to hold about eight of the widest record the last one held, between the `buffer_size` floor
and the `max_buffer_size` ceiling ([F23](../features/self-sizing-staging-buffer.md)).

**Tombstone** — A `MaybeRow::Tombstone` marking a deleted row. Necessary because a delete may
target a row still sitting in an unread archive, so the deletion must be recorded in a form
that survives a later merge. Both table types use it: a sorted partition tombstones one entry
in its `BTreeMap`, an unsorted partition — which has only one row — becomes a tombstone whole.
Removed for real at compaction, which also drops the partition's `ArchiveEntry` when nothing
is left of it.

**Unsorted table** — `PersistentUnsortedTable`. Exactly one row per partition, so a partition
is either fully resident or not resident at all.

**unsafe Send invariant** — `ServerMsg` asserts `Send` by hand even though its `Partition`
variant carries a non-`Send` glommio `ReadResult`. The rule that makes this sound — a
`Partition` message may only be sent on its own shard's channel — is enforced only by
convention. The most dangerous thing in the codebase to change unknowingly. See
[Known Issues](known-issues.md#unsafe-send-invariant).

**Watermark** — An intent log offset below which everything satisfies some property. The
writer tracks two in `FlushState`: `written_pos` (every byte below it has been `write_at`
completed) and `synced_pos` (every byte below it has been `fdatasync`ed). Both are *contiguous
low-water marks*, not maxima — io_uring completions arrive out of order, so a maximum would
cover data still in flight. Pending responses are released against `synced_pos` by default.
See [Intent Log](../storage/intent-log.md#completion-notification).

**Pad region** — Filler written after a partial flush to round it up to a block boundary so it
can be written with O_DIRECT. Starts with `PAD_SENTINEL` (`u64::MAX`) so replay skips it rather
than mistaking it for the end of the log. See
[Intent Log](../storage/intent-log.md#pad-regions).

---

## Terms that mean something unusual here

| Term | Elsewhere | In Shoal |
| --- | --- | --- |
| Coordinator | A distinct node or process | A role any shard plays per connection |
| Intent log | — | The write-ahead log |
| Flushed | Durable on stable storage | Handed to the kernel; no `fdatasync` on the normal path |
| Sorted | Supports ordered scans and range queries | Rows are stored ordered, but no read predicate uses the order |
| Distributed | Multiple nodes | Multiple shards in one process |
| `sync` | Force to stable storage | On `StreamWriter`, issues a background write and returns. `sync_blocking` is the real one — but on glommio's `DmaStreamWriter`, `sync` *does* fsync |

**Workload** — One purpose-built benchmark in `shoal-bench`, isolating one path through the
engine: it generates its own rows from a seed, drives a server it owns, and writes one block of
the macro artifact. Its identifier — `macro/get_archived`, `macro/fanout/evicted/64` — is the key
every comparison joins on, so **renaming one orphans every capture taken before the rename**. Not
to be confused with the *macro layer*, which is the set of all of them. See
[F8](../features/purpose-built-workloads.md).
