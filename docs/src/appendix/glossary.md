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
partition to be read from disk. Re-injected as a `ServerMsg::Released` when the read
completes, rather than being resumed as a suspended future. It carries the query itself, unlike
`ServerMsg::Query` which carries the bundle a query arrived in — a blocked query was decoded and
narrowed when it first arrived and is never decoded again
([F26](../features/archive-routed-requests.md)). See
[Query Execution](../tables/query-execution.md).

**check_disk** — A flag on `SortedPartition` meaning "there may be more of this partition in
an archive". Set on creation; cleared once the full archive copy has been merged in, when a
partition is built out of an archive that was already read, or when storage answers that there is
no archive at all ([Resolved #80](resolved/never-flushed-partitions.md)). It decides two things:
whether a query is parked on a disk read, and whether a get may be answered out of the rows the
shard is already holding ([F27](../features/grouped-responses.md)).

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

**Identity projection** — The projection of a row into itself. It is what a get that named no
projection is answered with, which is what makes a projected get and an unprojected one the same
code path — and it is the only projection that copies **nothing**: `IDENTITY` lets a resident row be
pointed at where its partition holds it ([F27](../features/grouped-responses.md)) and
`ARCHIVED_IDENTITY` lets an archived one be written straight out of its archive
([F28](../features/rearchived-rows.md)). Neither constant can be set by a projection that is not
its own row.

**Mirror** — The serializer `shoal-derive` emits to write an archived value back into its own
layout, which rkyv has no way to do: `Rearchive`, one impl per row and per projection, resolved
field by field against rkyv's own archived struct. A field whose type the derive cannot see inside
falls back to being materialized on its own, and `#[shoal(rearchive)]` opts a nested type that
implements the trait back in. See [F28](../features/rearchived-rows.md).

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
| Intent log | — | ~~The write-ahead log~~ A standalone node's write-ahead log; a cluster node's is the shared WAL below ([F40](../features/replication.md)) |
| Flushed | Durable on stable storage | Handed to the kernel; no `fdatasync` on the normal path |
| Sorted | Supports ordered scans and range queries | Rows are stored ordered, but no read predicate uses the order |
| Distributed | Multiple nodes | Multiple shards in one process. What it would take to mean the other thing is [Distributed Shoal](../distributed/overview.md), whose [vocabulary](../distributed/overview.md#vocabulary) fixes the words below for that part alone |
| Node | A machine or process in a cluster | ~~Unbuilt.~~ One Shoal process with a `NodeId` and a storage directory ([C1](../distributed/node-identity.md)); since [F37](../features/node-identity-control-plane.md) every process has one, minted the first time its directory is claimed |
| Node id | A hostname or an index | A random uuid minted once per storage directory, kept in the marker, never derived from an address ([F37](../features/node-identity-control-plane.md)) |
| Cluster id | A cluster name | A random uuid minted once, at the one explicit bootstrap, and adopted by every joiner; a directory naming another one is refused ([F37](../features/node-identity-control-plane.md)) |
| Marker | A lock file | `shoal-meta.json`: format, shard count, node id, cluster id, shard layout, last observed topology version, and since [F39](../features/membership.md) a `mode` - `standalone`, `cluster` or `joining` - and an `incarnation`, at format 3; format 2 is read and rewritten. ~~Only the last field is ever rewritten~~ The topology, the incarnation and a joiner's one-time cluster are the fields that move ([Resolved #45](resolved/storage-marker-format.md), [F37](../features/node-identity-control-plane.md)) |
| Replica set | The copies of a piece of data | ~~Unbuilt.~~ The `min(RF, N)` shards on distinct nodes holding a tablet - `placement[(t + k) % N]`, each on shard `(t / N) % shards` - derived from the map by one rule rather than stored per tablet ([C4](../distributed/tablet-map.md), [F40](../features/replication.md)) |
| Tablet group | A Raft group | One `openraft` group per table and distinct replica set, identified by `GroupId::of(table, members)`, hosted by the shard each member names; every tablet whose replicas are that set is served by it, so a placement of `N` nodes of `S` shards has `N × S` groups a table ([F40](../features/replication.md)) |
| Shard address | A host and port | `ShardAddr { node, shard }`, eighteen bytes: what a group's members are and what its log ids and votes name ([F40](../features/replication.md)) |
| Shared WAL | A write-ahead log | A cluster node's log: one format 2 file per shard under `wal/Shard-N/`, every group's entries multiplexed into it as frames that name their group, written in batches with one `fdatasync` each, whose completion is what openraft counts as a durable vote ([F40](../features/replication.md)) |
| Command | A request | `Command { table, tablet, request: {bundle, index}, payload }`: a write's intent serialized once and proposed through its tablet group, applied by every replica in committed order with its result derived there ([F40](../features/replication.md)) |
| Checkpoint | A snapshot | Per group, the last log id whose effect the table's archives hold, recorded in `wal/Shard-N/checkpoint.json` and moved by the compactor; what openraft's snapshot is metadata of, and where a restart re-applies from ([F40](../features/replication.md), [Q3](../distributed/protocol.md#q2-q3-and-q4-at-m4)) |
| Primary | A leader node | ~~Unbuilt.~~ A *role*: the first member of a tablet's replica set, which initializes its group and is preferred as its leader; ~~the one replica that orders that tablet's writes~~ the group's leader orders the writes, and after a failover that may be another member ([C5](../distributed/replication.md), [F40](../features/replication.md)) |
| Epoch | A term or generation | ~~Unbuilt.~~ The per-group leadership term, persisted in the shared WAL as a vote frame and established by the group's own election ([C13](../distributed/protocol.md#identity-and-progress)); replication messages carry it and receivers reject an obsolete one ([C7](../distributed/failover.md#fencing)). Not the compaction *generation* |
| Consistency level | How many replicas a read or write waits for | ~~Unbuilt.~~ `One`/`Quorum`/`All` for writes - `Quorum` a durable majority and this node's apply, `All` every voter, `One` refused ([F40](../features/replication.md)) - and `One`/`Primary`/`Quorum` for reads, of which `One` is built ([C6](../distributed/reads.md)) |
| Outcome unknown | A timeout | `OutcomeUnknown`: a write that was proposed and not answered within its deadline may commit later; `Shedding` and `NotLeader` mean nothing was proposed ([F40](../features/replication.md), [C5](../distributed/replication.md)) |
| Fixture | A test's setup | The cluster fixture under `shoal/tests/cluster/` ([F36](../features/cluster-harness.md)): real servers and mock peers as child processes on port zero, with directed links, pause, kill and cleanup ~~— no peer protocol yet~~; since [F38](../features/inter-node-transport.md) it places a cluster - a marker, a data port and a control port per child ~~and one placement they all read~~ - proxies each node's lanes so a test can cut, delay or heal one, and drives a child over stdin; since [F39](../features/membership.md) node zero bootstraps and every other child joins through it, the proxies are per direction, and a cluster can be restarted, cloned, isolated, initialized and administered |
| Schedule | A timetable | An explicit list of events the protocol model in `shoal-model` applies in order: generated from a seed, written by a builder, saved as JSON, replayed and minimized. Every subsequence of one is one ([F36](../features/cluster-harness.md)) |
| Oracle | Prophecy | The sequential state machine `shoal-model` judges a history against, one tablet and one key at a time, with successful, rejected and unknown outcomes held to three different contracts ([C11](../distributed/testing.md#the-write-ledger)) |
| Control plane | A separate service | ~~Unbuilt.~~ A thread on the control core of every cluster node - cpu 0 by default, validated against the process's affinity - running an embedded `openraft` group on a glommio executor; the failure detector and the rebalancer will hang off it; never on a query path ([C1](../distributed/node-identity.md), [F37](../features/node-identity-control-plane.md)). At M1 the group has one member and a standalone node runs none of it; at M2 ([F38](../features/inter-node-transport.md)) its RPCs cross the control lane to a placed peer, over a listener and links the thread owns itself, ~~and every node is still a group of one~~; at M3 ([F39](../features/membership.md)) the group is the cluster's membership - joiners admitted as learners, voters promoted under the policy, the map built and pushed, admin operations proposed, and the leader's phi-accrual detector committing health - and the thread is one fan-in loop of events |
| Peer link | A TCP connection | One outbound lane to one peer, owned by one task on one executor - a shard's for the data and bulk lanes, the control thread's for the control lane - dialled lazily on its first frame, bounded in bytes, shedding at the bound before anything is recorded, and backing off with jitter when it drops ([F38](../features/inter-node-transport.md), `shoal-core/src/server/peer/link.rs`) |
| Lane | A channel | Which of ~~three~~ four sockets a frame travels on: **data** for forwarded bundles and their answers, **bulk** for snapshot streams, **control** for the control group's RPCs and pings, and since [F40](../features/replication.md) **replication**, on the data port, for the tablet groups' appends, votes and proposals. Separate sockets because bytes already written to one stream cannot be preempted by a more urgent frame behind them ([C2](../distributed/transport.md), [F38](../features/inter-node-transport.md)) |
| Placement | Where a thing is put | ~~The static map of a cluster's nodes - identity, data and control address, shard count - that every node reads and none elects~~ The ordered list of nodes the cluster's `Initialize` named, in the tablet map: tablet `t` belongs to `nodes[t % N]`, then to shard `(t / N) % shards`. ~~Test-shaped by design, written by the fixture or the benchmark harness, and replaced when M3's membership commits a real one ([F38](../features/inter-node-transport.md))~~ Committed by the control group and pushed whole to every shard since [F39](../features/membership.md); before `Initialize` the bootstrapper is placed on itself and a joiner is unplaced |
| Tablet map | A routing table | `TabletMap`: the version, the cluster, the leader, the members with their endpoints, roles, health and incarnations, the placement order, the tables with their ids, the desired factor, the two consistencies and the admins - built by the control thread from the applied state on every version and installed whole by every shard, which rebuilds its ring from it ([F39](../features/membership.md), [C4](../distributed/tablet-map.md)) |
| Topology frame | A cluster directory | What a subscribed client is pushed on every map version: the map's members with their client, data and control endpoints, the placement, the factors, the consistencies and the tables, as JSON under the nil query id; a run of them queued to one connection is folded to the newest ([F39](../features/membership.md)) |
| Joiner | A new node | A node with `seeds` and no `bootstrap`: its marker says `joining` with a node id and no cluster, it dials a seed's control lane with a hello that names no cluster, and the leader admits it as a learner and commits it; it adopts the cluster id once and never initializes anything ([F39](../features/membership.md)) |
| Learner | A non-voting member | A member of the control group that receives the log and votes in no election; every joiner starts as one and the leader promotes learners only up to `control_voters`, so a fourth node under a three-voter policy stays one ([F39](../features/membership.md), [C3](../distributed/membership.md)) |
| Table id | A table's name | `TableId`: the gxhash of the table's name under a frozen seed, emitted by the derive and never the enum's position, committed by `Initialize` and pinned by literal in a test; what a persisted stream is named by ([F39](../features/membership.md)) |
| Forward | To send on | One frame per node carrying the whole client bundle once, as the client's own bytes, and one entry per query the node owns; re-validated on arrival as if it had come from a client. A *forwarded* answer names the bundle and the query index and comes back sealed for a whole query or as a share for a gathered one ([F38](../features/inter-node-transport.md)) |
| Hello | A greeting | The 68 byte pre-schema handshake two peers exchange before any data frame - cluster, node, incarnation, lane, wire versions, capabilities, schema identity, shard count, largest frame - judged in one fixed order on both ends, with the refusal reason written into the ack ([F38](../features/inter-node-transport.md)) |
| Schema id | The schema fingerprint | The structural fingerprint *without* the protocol version folded in, so that schema identity, wire version and capabilities are three separately compared things in the hello; the fingerprint a client checks still folds the version ([F38](../features/inter-node-transport.md), Q10) |
| Incarnation | A restart counter | ~~Nanoseconds since the epoch at process start~~ A counter in the storage marker, bumped by every claim of an established directory, carried in the committed member record, the hello, the pong, every status report and every proposal. The fencing rule is written against it: a lower run is refused, an equal one from another address is a duplicate, a higher one supersedes and the superseded run stops `Fenced` ([F39](../features/membership.md)); ~~Provisional: a persisted, monotonic one is part of Q11's cloned-directory fencing ([F38](../features/inter-node-transport.md))~~ |
| Hop | A network hop | Where a query ran relative to the shard that accepted its connection: `same` (that shard), `local` (another shard of the node, over the mesh) or `remote` (another node, over a peer link). A property of the query and its connection, since the kernel picks the accepting shard, which is why the hop arms state an expected mix and the stage report splits by it ([F38](../features/inter-node-transport.md)) |
| `sync` | Force to stable storage | On `StreamWriter`, issues a background write and returns. `sync_blocking` is the real one — but on glommio's `DmaStreamWriter`, `sync` *does* fsync |

**Workload** — One purpose-built benchmark in `shoal-bench`, isolating one path through the
engine: it generates its own rows from a seed, drives a server it owns, and writes one block of
the macro artifact. Its identifier — `macro/get_archived`, `macro/fanout/evicted/64` — is the key
every comparison joins on, so **renaming one orphans every capture taken before the rename**. Not
to be confused with the *macro layer*, which is the set of all of them. See
[F8](../features/purpose-built-workloads.md).
