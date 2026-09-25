# 15. No backpressure anywhere - the remainder

This is the second half of item 15, and it closes the item. [The first
half](shard-mesh-admission.md) bounded what a client may put on a shard's mesh queue. This half
bounds the three places work goes after it leaves that queue: writes waiting on their fdatasync,
queries parked on a partition read, and answers owed to a client that is not reading them.

## Symptom

The mesh's admission bound (`networking.max_queued_queries`) counts the messages waiting on a
shard's queue and nothing else. Work that has been dequeued is invisible to it, and three kinds
of dequeued work grew with nothing counting them:

- **`PendingResponse`**, a table's queue of writes waiting to be made durable. It grew with
  arrival rate × fsync latency. A slow device made it grow without limit while the shard loop
  itself stayed fast, so the mesh queue stayed short and nothing was shed. A rotation's
  `drain_all` then released the whole queue into the client channels at once.
- **`blocked` and `pending_data`**, the queries parked on partition reads. They grew with
  arrival rate × read latency, in the same way and invisibly to the mesh for the same reason.
- **A client connection's reply channel**, which held every answer until the connection's
  write relay wrote it. A client that pipelined queries and never read the answers was read
  forever, and every answer it would never collect was held for it.

The item also said that the mesh bound counts releases and loads as well as client queries, so a
shard busy with its own work sheds sooner. That is kept deliberately. See *Alternatives
rejected*.

## Cause

Each of these was bounded only by "the work the admission bound admits". That is not a bound,
because the admission bound stops counting a query as soon as its shard dequeues it. A shard
that dequeues quickly and then waits on its device admits without limit. The cluster path
already had the missing bounds: a replicated write is shed at `cluster.replication.pending_bytes`
before it is proposed (F40), and a peer lane stops reading forwards once its unanswered bytes
reach `cluster.transport.inflight_bytes` (F38). The standalone write path and the client connection had
neither.

## Evidence

**Reproduced.** Each test below was run against the fixed tree with its bound switched off:
`>= usize::MAX` in place of the pending and parked comparisons, and an always-open gate in
`ReplyRoom::poll`. Every one of them failed.

`shoal/tests/table_backlog.rs`, whose tables are held so that nothing sweeps the pending queue
and no loader serves a read:

```text
test an_unsorted_read_past_the_parked_bound_is_shed_before_it_parks ... FAILED
test a_sorted_read_past_the_parked_bound_is_shed_before_it_parks ... FAILED
test a_sorted_write_past_the_pending_bound_is_shed_and_never_written ... FAILED
test a_get_that_has_parked_is_never_shed ... FAILED
test an_unsorted_write_past_the_pending_bound_is_shed_and_never_written ... FAILED
a get past the parked bound was not shed
a get past the parked bound was not shed
the write past the pending bound was not shed
a fresh get past the parked bound was not shed
the write past the pending bound was not shed: None
```

`shoal/tests/backpressure.rs`, with a raw connection that asks for sixty-four answers of 256 KiB
each, reads none of them, and then writes 64 MiB of inserts:

```text
the client's writes stalled at None of about 67112960 bytes, owing 64 answers
thread 'a_client_that_stops_reading_stops_being_read' panicked at shoal/tests/backpressure.rs:289:9:
the server read all 67112960 bytes from a client that read none of its answers
```

With the fix, the same client's writes stall at about 530 KB, which is what the socket buffers
hold. Once it reads, all 320 queries are answered.

The opposite mistake was checked too. With the rule that no part of a query may already be
parked removed (`may_shed = true`), `a_get_that_has_parked_is_never_shed` fails with *a get that
had parked on one partition was answered rather than parked*.

## The fix

Three new `networking` fields sit beside `max_queued_queries`. Each has a serde default, so the
committed `shoal.yml` and the benchmark baseline are unchanged:

| Field | Default | Bounds | When it is reached |
| --- | --- | --- | --- |
| `max_pending_writes` | 65,536 | writes one table on one shard holds waiting to be made durable | the write is answered `Shedding` before it is committed |
| `max_parked_queries` | 65,536 | queries one table on one shard holds parked on partition reads | a query that would park is answered `Shedding` before it parks |
| `max_queued_replies` | 8,192 | answers one client connection owes, on its channel and taken but not yet written | the connection's read relay stops reading until the owed answers drop below it |

**Pending writes.** Each persistent table's `handle` checks, before an insert, delete or update
runs, whether `pending.len()` has reached the bound. If it has, the write is answered with a
`Shedding` response in its own index and `end` place, and nothing is committed. This is one
comparison per write, made once per table rather than at each of the eight `storage.commit`
sites. A write released from a parked read passes through `handle` again and is judged again.
That is sound because it has not committed anything yet. The rotation burst is now bounded by
the same number, since `drain_all` can release no more than the queue holds.

**Parked reads.** `blocked` is now a `ParkedQueries` (`tables/persistent.rs`). It is the same
map, plus a count that every `join`, `park` and `take` keeps. `block_on_load` answers with a
`Parking` enum (`Parked`, `Absent` or `Shed`) instead of a `bool`. It sheds only when the caller
passes `may_shed` and the count is at the bound, and it decides this before anything is pushed
or a read is asked for.

- A single-partition query always passes `may_shed`.
- A get passes it only while the query is fresh (`pending_data` did not hold it) and this
  execution has parked nothing yet.
- A sorted exists passes it only while `pending_exists` did not hold it and nothing is in its
  blocked list yet.

A query that is partly parked is therefore never shed, because its first share would stay
parked with nobody left to answer. A resident read never reaches `block_on_load`, so a slow disk
never sheds it.

**Replies.** The accepting shard builds a `ReplyBacklog` for each client connection: cells
shared by the two relays, which run on the same executor. The write relay records how many
answers it has taken off the channel and not yet started writing, wakes the read relay each
time it starts one, and marks the backlog closed when it ends. Before reading each frame, the
read relay awaits a `ReplyRoom`, which resolves once `unwritten + client_rx.len()` is under the
bound, or reports that the connection is over. It reads the channel only for its length, through
a cloned receiver. When the gate is shut, the socket is not read, TCP pushes back, and the
client's writes block. Nothing is refused and nothing is dropped. This is the peer lanes'
`Inflight`/`Room` model applied to clients.

A shed query is answered with the `ErrorCode::Shedding` that F11 reserved and the mesh already
produces. The client's retry already treats that code as retriable.

## Alternatives rejected

**Closing a connection whose reader has stopped.** This is what the item's fix direction
suggested. It drops answers the client may still want, and it needs a hangup signal sent across
threads and raced against every write that blocks. The write relay can block forever on a
client with a zero window, so the race cannot be skipped. Not reading the client bounds the same
memory, loses nothing, and needs only two cells on one executor.

**Bytes rather than counts.** The cluster's `pending_bytes` is a byte bound, but here the memory
held per pending write or parked query is its metadata, not its row. A byte count of the log
would let millions of tiny deletes through. For replies, a count times `max_frame_bytes` is
already a byte bound, which is the argument the first half made for the mesh.

**Judging at each `storage.commit`.** There are eight sites across two tables, and at several of
them a partition is already borrowed mutably. One check in `handle`, before the query runs,
covers all of them and costs the same.

**Shedding a parked query when a later partition would pass the bound.** A get that has parked
on its first partition and is shed at its second would leave the first share parked, and its
replay would answer a query the client was already told had failed. `may_shed` exists to make
that impossible.

**Counting only client queries on the mesh.** The item suggested that releases and loads should
not count against the mesh bound. A message on a shard's queue delays the query behind it
whatever kind of message it is, so a shard busy with its own work really is behind. Telling the
kinds apart would take an atomic increment when a share is sent and a decrement when it is
received, on every share, to make a distinction that does not matter to the query waiting.

**Bounding the loader and compactor channels.** The loader receives at most one request per
partition, and the parked bound now limits how many partitions a table waits on. The compactor
receives one job per rotation. Neither has anything left to bound.

## Invariants to uphold

- **A shed query ran nowhere on this shard.** A shed write committed nothing. A shed read parked
  nothing and asked for no read. This is what makes `Shedding` safe to try again. A split get
  whose share on one shard is shed fails as a whole, and its other shares ran only as reads.
- **A query that has parked any part of itself is never shed.** `may_shed` is false from the
  first `Parking::Parked` of an execution, and false for any query `pending_data` or
  `pending_exists` already holds. Read it before `resume`, which takes that state out.
- **Every change to `blocked` goes through `ParkedQueries`.** The count is only correct if
  nothing reaches the map directly.
- **The write relay never waits on the read relay.** The gate only ever stops reading. The
  write relay drains unconditionally and wakes the gate as it goes, so the gate cannot deadlock
  the connection. When the write relay ends it closes the backlog, so a read relay parked at the
  gate ends too instead of holding a dead socket.
- **The defaults are the documented ones.** `a_config_without_an_admission_bound_gets_the_default`
  pins all four bounds.

## Still open

- **A query past the parked bound can be shed for a partition that has nothing on disk.**
  `block_on_load` judges the bound before it asks storage whether there is anything to read, so
  a fresh query for an absent partition is shed while the table is at its bound. Checking the
  archive map first would move the check behind the storage call on every park. When a table
  is at its bound it is already turning reads away, and this is one more.
- **A connection that is not being read stays open.** Its cost is now bounded, but a client
  that never reads again holds its socket and its `max_queued_replies` answers until it
  disconnects. There is no idle timeout, which was true before too.
- **Nothing counts what was shed or held.** The mesh bound's refusals are counted on the
  transport view (`ShardTransportView::shed`). The two table bounds and the reply gate are not
  counted anywhere yet.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `an_unsorted_write_past_the_pending_bound_is_shed_and_never_written` | `shoal/tests/table_backlog.rs` | A write past the pending bound is committed, or a shed write's row is in the table, or the queue does not take writes again once drained |
| `a_sorted_write_past_the_pending_bound_is_shed_and_never_written` | `shoal/tests/table_backlog.rs` | The same for the sorted table |
| `an_unsorted_read_past_the_parked_bound_is_shed_before_it_parks` | `shoal/tests/table_backlog.rs` | A get past the parked bound parks, or asks for a read, or the count does not drop when a read lands |
| `a_sorted_read_past_the_parked_bound_is_shed_before_it_parks` | `shoal/tests/table_backlog.rs` | The same for the sorted table |
| `a_get_that_has_parked_is_never_shed` | `shoal/tests/table_backlog.rs` | A get that has parked on one partition is shed at the next, or a fresh get past the bound is not shed |
| `a_client_that_stops_reading_stops_being_read` | `shoal/tests/backpressure.rs` | A client that reads none of its answers is read to the end, or an answer is lost once it reads |
| `a_config_without_an_admission_bound_gets_the_default` | `shoal-core/src/server/conf.rs` | A default moves or a key becomes required |

## Related

- [Resolved #15, the shard mesh half](shard-mesh-admission.md), the bound this one completes.
- [F11. The error channel](../../features/error-channel.md), which reserved `Shedding`.
- [F38. Inter-node transport](../../features/inter-node-transport.md), whose peer lane
  in-flight bound is the model for the reply gate.
- [F40. Replication](../../features/replication.md), whose `pending_bytes` is the cluster's
  pending-write bound.
- [D6. Connection pool](../../direction/connection-pool.md), whose bounded-channels section this
  finishes.
- [Resolved #33](gather-expiry.md), the deadline that bounds how long a client waits.
