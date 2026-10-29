# 309. One put whose task took none of its bytes stopped its whole connection being read

A put past its pool's inline threshold is piped to its task as its frames arrive, through a channel
a stream's window long. Its connection's reader waited on that channel with no bound, so a put whose
task took none of its bytes stopped the reader. And a pooled connection carries other operations: a
connection goes back to the pool once an operation's frames are written. Every operation queued on
it waited behind the one put for as long as its task did.

Found while reading the server half of
[item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
whose stall it fits, and filed and fixed in one change, the third of the silent waits wave three set
out to bound.

## Symptom

- **Operations that wait on a connection nobody reads.** A get, a stat or another put behind a
  stalled put on the same connection is neither read nor answered until the put's task takes its
  bytes.
- **No progress at all if the task never does.** Before item 308, an operation's attempt had no bound
  either, so its caller waited for good.
- **What fits item 305:** its check read 236 of 246 objects back, and the other ten failed
  `ConnectionLost` when the cluster was torn down. Reads queued on pooled connections behind stalled
  puts would have failed exactly so, though which connections they were is not known.

## Cause

`client_rx_relay` reads a connection's frames (`server/shard.rs`). For a piped put's data frame,
`read_data_frame` sent the bytes into the put's feed and waited:

```rust
if feed.send(bytes).await.is_err() {
    sink.assembly = Body::Drained;
}
```

The feed is bounded by the stream's window (`put_window_frames`, four frames). That is deliberate:
holders that take bytes slowly push back on the client through TCP, and the server never buffers a
put's bytes past its window. But the wait had no bound. A task that took nothing kept the reader
waiting with it, and the connection's other operations sat unread in the socket behind the put's
frames.

## Evidence

**Reproduced by `a_put_whose_task_takes_nothing_leaves_its_connection_read`**
(`shoal/tests/objects.rs`), written before the fix:
- a standalone node whose query deadline is two seconds;
- a client of one connection set apart for objects;
- a fault (`faults::hold_feed`) holds the bytes of puts under one path unread for twenty seconds
  before their pipelines start, as a put whose holders take none would;
- the client puts six MiB there, which fills the window and leaves the rest in the connection;
- it then stats an object on the same connection.

On the unfixed tree:

```
a stat behind a put whose task took none of its bytes waited past fifteen seconds
test result: FAILED. 0 passed; 1 failed; ... finished in 16.56s
```

With the fix, the stat is answered in about two seconds, and the put is answered by name once its
task resumes and finds its stream cut short.

## The fix

- **A put's feed send is bounded by the node's query deadline** (`ObjectGate::feed_stall`, handed to
  `read_data_frame`).
- **A put whose task takes none of its bytes within it is given up:**
  - its feed is dropped and its later frames are read and dropped (`Body::Drained`);
  - the reader goes on to the connection's other operations;
  - a throttled WARN says so.
- **The put fails by name.** Its task finds its stream ended before its declared length, fails as
  `PutFailure::Source`, gives the put up and answers it, so its client puts it again under a new
  identity.
- **Why the race is safe.** A frame being handed over as the bound passes may reach the task or not.
  The put ends either way, since its feed is dropped. That is why this kanal send may be raced, where
  a receive never may.

## Alternatives rejected

- **No window, every frame buffered.** It would hold a stalled put's whole object in the server's
  memory, which is what the window exists to prevent.
- **A connection of its own for every put.** A client sets connections apart for long streams
  already, and a put shares one with other operations by design (Resolved #230). Giving each put its
  own would trade this stall for a connection an operation.
- **Failing the connection, not the put.** Every other operation on it would fail with it, which is
  the cost this avoids.
- **A bound of its own.** The query deadline is already the node's word for how long a step may take
  before it is given up. A task that takes none of a put's bytes for that long is stalled, whatever
  its holders are doing.

## Invariants to uphold

- **No reader of a client's connection waits on one operation without a bound.** A wait that holds
  the reader holds every operation on the connection.
- **A put is never made current from bytes cut short.** Its task reads to its declared length or
  fails.
- **The window still bounds a put's bytes in memory.** Giving a put up drops them; it never buffers
  them.

## Still open

- **The put's task itself may stay blocked** after its feed is dropped, until whatever holds it ends.
  The lanes' stall bound and Resolved #307 bound most of that, but not a task blocked outside them.
- **Keepalive on connections whose peer vanished** is filed beside this one from the same reading of
  item 305.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_put_whose_task_takes_nothing_leaves_its_connection_read` (`shoal/tests/objects.rs`) | A stat behind a put whose task takes none of its bytes, on the same connection, waits past fifteen seconds instead of being answered within the query deadline's bound; or the put whose bytes were dropped is made current |

## Related

- [Item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
  the stall this was found reading for.
- [Resolved #307](a-lane-whose-writer-stops-ends.md) and [Resolved #308](an-object-attempt-gives-up.md),
  the holder's and the client's halves.
- [Resolved #230](object-connection-held.md): an operation's connection goes back to the pool once its
  frames are written.
- [F86](../../features/replicated-pools.md): a put's bytes piped to its task through its window.
