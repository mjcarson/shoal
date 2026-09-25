# 60, 130, 131. A stream's slot and the connections that owe it answers

Items 130 and 131 came out of the same hang, and item 60 is the older half of 130. They are all
about the client's channel map, the table the read loop of each pooled connection uses to route
a frame to the stream it belongs to, and about what a stream's entry in it records. Item 60 is
only partly closed: its client half is fixed here and its server half, a `Cancel` message, is
still open on [Known Issues](../known-issues.md#60-a-result-stream-that-is-not-drained-to-the-end-leaks-its-slot-in-the-client).

## Symptom

The first [TMDB load](../../features/tmdb-dataset-deployment.md) with retries wrote the whole
dataset and then hung in its verify phase. Four connections to one member held 47–79 KB each in
their receive queues, unread and not growing, and the server logged nothing. The loader's workers
had broken out of their loop once every query was answered and dropped their streams without
reading the end the close sends.

## Cause

Three defects, each of which on its own is survivable:

- **Item 60.** A result stream gave its slot back only in `release`, which runs when `next()`
  returns the stream's end or an error. A stream dropped before that left its slot behind for
  the life of the client.
- **Item 130.** kanal closes a channel when its last receiver drops. So the slot left behind
  pointed at a closed channel, and `TcpProxy::relay` handed the next frame for it to
  `waiter.tx.send(..).await.map_err(send_failed)?`. That send failed, and the `?` ended the read
  loop of the whole connection. The comment under the `None` arm just below it said a frame
  nobody is waiting for must never do that. A closed channel is the same case, and nothing
  caught it.
- **Item 131.** When a read loop ended, `fail_waiting` failed each waiter whose `conn` was this
  connection. A waiter recorded one connection, and a query stream wrote over it on every
  bundle with whichever connection the pool handed out. A stream that sent bundle A on
  connection 1 and bundle B on connection 2 was recorded against connection 2 only. When
  connection 1 died owing A's answers, the stream was never told, and its `next()` waited for
  answers that could not come. That is what turned 130's dead read loop into a hang rather than
  a `ConnectionLost` the loader would have retried.

## Evidence

**Reproduced against the unfixed tree.** The two unit tests below were run in a worktree of
`8afdeac`, written in the old `Waiter { conn: Some(..) }` shape. The old waiter can only name the
connection a stream's last bundle went to, so the multi-connection stream is expressed as
`conn: Some(2)`, which is exactly what the old code recorded:

```text
test client::tests::a_stream_is_failed_by_every_connection_that_owes_it ... FAILED
test client::tests::a_frame_for_a_dropped_stream_does_not_end_the_read_loop ... FAILED

thread 'client::tests::a_stream_is_failed_by_every_connection_that_owes_it' panicked at shoal-client/src/client.rs:4248:14:
a stream owed answers on a dead connection was not told

thread 'client::tests::a_frame_for_a_dropped_stream_does_not_end_the_read_loop' panicked at shoal-client/src/client.rs:4194:22:
the live stream was answered with ServerError(ConnectionLost, "the connection to the server failed: Channel(ReceiveClosed)", ClientStamps), not its response
```

The second message is item 130 exactly: a frame for a dropped stream ended the loop, and the live
stream on the same connection got `ConnectionLost` instead of its answer.

## The fix

- **A waiter records what each connection still owes it** (`Owed`, in `shoal-client/src/client.rs`).
  It is a small list of `(connection, answers)` pairs behind a mutex. It is shared by every copy of
  the waiter through an `Arc`, and a query stream holds the same `Arc`. Each send adds the bundle's
  query count to the connection it wrote to (`Shoal::owe`, `ShoalQueryStream::send`), and the
  relay subtracts one for every response frame it reads for that id on that connection.
  `fail_waiting` fails a waiter when the dying connection's count is above zero
  (`Owed::owes`). Counts are signed because a response can be read before the send that owes it
  has recorded its write. The two always meet at the same total, whichever lands first.
- **A failed send drops the frame and removes the slot.** The relay no longer ends on it. It logs
  at `WARN` and goes on, the same as a frame for an id nobody tracks.
- **A result stream removes its slot when it is dropped** (`Drop` for `ShoalResultStream` and
  `ShoalUnorderedResultStream`), unless it was already released. Only the slot goes. Returning
  the channel pair to the reuse queue is an async send that `drop` cannot await, and a lost pair
  is a missed reuse, not a leak.
- **`Shoal::tracked()`** reports how many ids a client is tracking, so a leak is something a test
  or an operator can see.

## Alternatives rejected

- **Failing every stream with any bundle ever written to the dying connection.** That is simpler,
  a set of connections instead of counts, but it fails a stream whose answers on that connection
  had all arrived. The pool reaps idle connections as a matter of course, so a long-lived stream
  would be failed with `ConnectionLost` by ordinary housekeeping.
- **Tracking outstanding bundles by index range per connection.** That is exact to the query, but
  the relay would have to decode every response's index before routing it, which is a parse on the
  hot path to maintain a figure that only a death reads. One response frame per query is the
  invariant the counts rest on, and it is cheaper.
- **An atomic counter per connection in a concurrent map.** A stream writes to a handful of
  connections, a linear scan of a short vector under an uncontended mutex costs tens of
  nanoseconds, and it keeps the per-stream state one allocation.
- **Only the `Drop`.** That fixes the leak at its source for result streams, but a query stream
  that keeps sending after its reader dropped registers its slot again, and the late frames would
  still reach a closed channel. The relay has to survive that on its own.

## Invariants to uphold

- **A server answers each query of a bundle with exactly one response frame, on the connection the
  bundle arrived on.** `Owed` counts frames per connection against queries per bundle, and a server
  that answered a query on another connection, or in two frames, would leave counts that never
  reach zero. The cost of that is over-failing on a later death, not a hang.
- **Every path that writes a bundle records it in `Owed` before it checks `dead_conns`.** The read
  loop marks itself dead before it sweeps. So one of the two always sees the other, which is the
  ordering the old single-connection code relied on too.
- **The relay never ends on a frame it cannot deliver.** Only a failure of the connection itself
  ends a read loop, because the loop is shared by every stream multiplexed on it.

## Still open

- The server half of item 60: nothing tells the server to stop producing answers for a stream the
  client dropped. `MessageType::Cancel` has a discriminant and no wiring
  ([D2](../../direction/framing.md#message-types)).
- A dropped result stream's channel pair is not returned to the reuse queue.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_frame_for_a_dropped_stream_does_not_end_the_read_loop` (`shoal-client/src/client.rs`) | A frame for a stream whose reader is gone ends the connection's read loop and fails a live stream on it, or the slot is left behind |
| `a_stream_is_failed_by_every_connection_that_owes_it` (`shoal-client/src/client.rs`) | A stream owed answers on a connection other than its last is not told when that connection dies, or a stream owed nothing on it is failed |
| `a_stream_dropped_early_is_not_tracked` (`shoal/tests/ephemeral_unsorted_table.rs`) | A result stream dropped before its end, ordered or unordered, leaves its id tracked by the client |

## Related

- [F54](../../features/tmdb-dataset-deployment.md), whose loader found items 130 and 131 and reads
  its streams to the end.
- [F11](../../features/error-channel.md), which introduced `fail_waiting` and the rule that a frame
  nobody waits for is dropped rather than fatal.
- [Distributed cluster testing](../../cluster-testing/findings.md), where the lab runs that
  exercise these paths are recorded.
