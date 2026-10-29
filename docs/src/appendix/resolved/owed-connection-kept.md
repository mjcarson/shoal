# 217, 317. A client's pooled connection past its lifetime was closed with answers owed on it

Since [Resolved #230](object-connection-held.md) an operation gives its pooled connection back once
its frames are written, and its answer is routed to it by its id. The connection still owes that
answer, but the pool does not know it. bb8 checks a connection's age as it is given back, and drops
one past the pool's `max_lifetime`, 30 minutes by default. Dropping the pool's handle dropped the
connection's write half, its only other holder being a weak entry for cancels. tokio shuts a write
half down when it is dropped, and a node reads the end of a client's stream as the client gone: its
read relay ends, and its write relay is cancelled with every answer it owes.

Found reading the client and bb8 for how [item 316](../known-issues.md#316-a-gets-request-on-an-object-lane-went-unanswered-and-its-coordinator-waited-with-no-bound)'s
first get failed, and reproduced by a test.

**One defect, filed twice.** [X8](../../object-storage/small-writes.md)'s driver met it first, as
item 217 (2026-10-08): its writes failed thirty minutes into a leg, and it was established by
reading the source. The store comparison's second run found it again as item 317, filed without a
reference to the first, which was still open.

## Symptom

```text
Error: Client(Server { query_id: Some(01a178f5-…), index: None, code: ConnectionLost, msg: "the connection to the server failed: IO(Custom { kind: UnexpectedEof, error: \"early eof\" })" })
```

An operation fails `ConnectionLost`, at once rather than after its answer's time, and is tried again
within its retry budget. A client living past its pool's lifetime loses an answer on a connection
each time an old one is given back owing one. An operation that has already spent its retry budget
fails.

## Cause

bb8 0.8.6 drops an expired connection in `put_back` (`inner.rs`), and only there and in its reaper:
a connection handed out is never checked for its age, so an old idle one is handed out, written on,
and dropped as it comes back.

```rust
let is_expired = match self.inner.statics.max_lifetime {
    Some(lt) => conn.is_expired(Instant::now(), lt),
    None => false,
};
```

The write half lived in the connection's `ConnShared`, held by the pooled `ShoalConnection` alone.
Nothing that waited for the connection's answers held it: the client's `Owed` counted what each
connection owed by its id only.

## Evidence

**Reproduced by `a_connection_past_its_lifetime_keeps_the_answers_it_owes`** (`shoal/tests/objects.rs`),
written before the fix. A client whose pool keeps connections a second gets a striped object once,
which opens the connection set apart for objects and gives it back young. It waits a second and a
half, so the idle connection is past its lifetime, then gets the object again with every striped read
held three seconds on its node (`faults::hold_read`) and no retry.

On the tree before the fix the second get failed half a second after it was sent, without waiting out
the hold:

```text
Error: Client(Server { query_id: Some(01a178f5-f35c-7ee2-aae3-d9649da24340), index: None, code: ConnectionLost, msg: "the connection to the server failed: IO(Custom { kind: UnexpectedEof, error: \"early eof\" })" })
test result: FAILED. 0 passed; 1 failed
```

**X8's evidence, as item 217.** X8's driver held one client a member for a whole leg. In the first
attempt at its rounds every failure fell about thirty minutes after the driver connected, on three
legs of three: 7 writes of one cell thirty minutes into round 1's lab leg, 33 and 2 of two cells
thirty minutes into its loopback leg, and 38 of a cell 31 minutes into round 2's loopback leg, which
ended that leg when a filler row it did not retry failed the same way. Every one was
`ConnectionLost`, "early eof", and the nodes logged no warning or error over the whole leg. One node
authenticated 27 of the driver's connections as the leg began, none for thirty minutes, and 13 more
as the failures came: the pool replacing what it had retired. With the driver's pools given no
lifetime and no idle timeout, the four rounds ran with no failure at all.

A first version of the test used a pool lifetime of a millisecond and passed on the tree before the
fix. The connection set apart for objects is opened on demand, so it was younger than a millisecond
when it was given back, and bb8 kept it. Only a connection already old when it is handed out is
dropped under an answer.

## The fix

**A connection that owes answers keeps its write half.** `Owed` holds each connection's `ConnShared`
from the write that is owed answers until its last answer is read, beside the count it already kept.
Every send records what it is owed with its connection (`Shoal::owe` takes the connection, and the
routed send's write loop and the stream's bundles hand it theirs). The pool may drop its handle as it
likes: the write half is shut down only when the pool has let the connection go and it owes nothing.
A hold is let go outside the lock, since the last one shuts the write half down. The same hold covers
a connection bb8's reaper drops, idle past its `idle_timeout` (five minutes) or past its lifetime,
which is how X8's legs lost theirs.

## Alternatives rejected

- **No lifetime for the pool.** It would hide this and keep every connection forever, where the
  lifetime is how a client moves off an endpoint that came back or a node that moved.
- **Keeping a connection that owes answers out of the pool**, as before Resolved #230. Every other
  operation queued for it.
- **A node writing what it owes on a connection whose client only stopped writing.** A half-closed
  connection is a client gone to every node today, and a client that crashed half-closes it the same
  way: the node cannot tell them apart.
- **Checking a connection's age as it is handed out** (`is_valid`). bb8 calls it, but an old
  connection refused there is only replaced: nothing at the moment of `put_back` would know the new
  one owes answers either.

## Invariants to uphold

- **A connection's write half lives as long as anything is owed on it.** A new send records what it
  is owed with its connection's shared half, never by its id alone.
- **The last hold on a write half is let go outside every lock**, since dropping it shuts the
  connection down.
- **A connection the pool let go is still found for a cancel while it owes answers**, through
  `Writers`, since its `ConnShared` lives.

## Still open

- **Item 316's first get** (the SSD leg's) fits this, but which way it ended is not established: its
  capture kept no record of the pool's connections.
- **The reaper's path has no test of its own.** The test reaches the drop at `put_back`; the reaper
  drops a connection the same way, which the hold covers alike.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_connection_past_its_lifetime_keeps_the_answers_it_owes` (`shoal/tests/objects.rs`) | A get on a connection past its pool's lifetime, held three seconds on its node, fails `ConnectionLost` half a second after it is sent |
| `a_stream_is_failed_by_every_connection_that_owes_it` (`shoal-client`, `client.rs`) | Unchanged: a connection that dies fails only the streams it owes answers |
| `a_dropped_stream_cancels_on_every_connection_that_owes_it` (`shoal-client`, `client.rs`) | Unchanged: a cancel goes to every connection that owes the stream answers |

## Related

- [Resolved #230](object-connection-held.md): an operation gives its connection back once its
  frames are written.
- [Resolved #131](stream-connection-accounting.md): what each connection owes a stream.
- [F75](../../features/client-cancel.md): the cancels a connection's shared half carries.
- [X8](../../object-storage/small-writes.md): the spike whose rounds met it first, as item 217.
