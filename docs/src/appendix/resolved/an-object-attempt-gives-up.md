# 308. A client's object attempt waited with no bound

An object operation is retried under its identity while its retry budget lasts. But the budget ran
only between attempts, and an attempt itself had no bound. A frame written to a connection its
server stopped reading waited as long as the socket stayed open. An answer that never came was
waited for for ever. Either way the caller waited with it, and its retries never began.

Found while reading the client half of
[item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
whose stalled puts at one in flight never failed, and filed and fixed in one change, the second of
the silent waits wave three set out to bound.

## Symptom

- **A put, get or stat that never returns.** It happens on a connection whose server took its
  frames and never answered, or stopped reading them. The caller's retry budget never runs out,
  since no attempt ends.
- **Every operation behind it on the caller's side waits too.** In the bench that is a whole stream.
  Item 305's put streams stopped completing for the minutes of their measured window and drain.

## Cause

`Objects::once` sends one attempt (`shoal-client/src/client/objects.rs`):
- it checks out a connection, which the pool's `connection_timeout` bounds;
- it writes the operation's frame and a put's data frames, each with no bound;
- it hands the connection back and waits on the operation's channel for its answer, again with no
  bound.

`Objects::run` retries a failed attempt while `started.elapsed() + pause < retry`. So its budget is
spent only by attempts that fail, never by one that hangs.

## Evidence

**Reproduced by `an_object_attempt_that_makes_no_progress_gives_up`** (`shoal/tests/objects.rs`),
written before the fix:
- a client of a node in process reaches it through a line that can be frozen: it carries every byte
  both ways until frozen, then carries none and holds its connections open;
- the client has two connections set apart for object operations, both opened before the freeze;
- each operation is sent once.

On the unfixed client, with the new setting in place but not yet read:

```
a put on a silent line waited past twenty seconds
test result: FAILED. 0 passed; 1 failed; ... finished in 20.24s
```

An earlier version of the test, which sent a stat first, waited past twenty seconds the same way.
With the fix, the put is refused `OutcomeUnknown` and the stat `Timeout`, each a second after its
bound, and the test takes 3.2 seconds.

## The fix

- **A bound on an attempt's progress, `Deadlines::object_attempt`**, 60 seconds by default:
  - each of its frames must be written within it;
  - its answer must come within it of its last frame.

  A large put on a slow link never runs out of it, since every frame written is progress. Sixty
  seconds is six of a server's default query deadlines, past the longest one operation's queries
  take on a server, which bounds each of them by its own.
- **An attempt past it is given up by name:**
  - a write as `OutcomeUnknown`, since its server may have applied it;
  - a read as `Timeout`.

  Both are failures `run` retries under the operation's identity while its budget lasts, and
  `settle` reports a write that never landed as unknown.
- **Its connection is discarded** (marked in `dead_conns`, which the pool's check reads):
  - a write cut off mid-frame has broken the stream;
  - a server that stopped reading a connection holds every operation queued on it.
- **Its channel pair is not returned to the queue.** A late answer may still arrive on it, and must
  never be read by a later operation. The answer's receive is raced through a `KeptReceiver`, as
  CLAUDE.md requires of every raced kanal receive.

## Alternatives rejected

- **A bound on a whole attempt.** A put of many gigabytes on a slow link would run it out while every
  byte was moving. A bound on progress holds a stalled attempt alone.
- **The server's query deadline, read from the server.** The client has no such field, and the
  server bounds each query it makes, not an operation's whole. A minute of nothing is the client's
  to call a stall.
- **Leaving the connection in the pool.** If its server stopped reading it, the retry would take it
  again and stall again. A healthy connection discarded costs one dial.

## Invariants to uphold

- **No await of an object attempt is unbounded.** A new frame or wait inside `once` takes the
  attempt's bound, or the pool's.
- **A stalled attempt never hands its channel pair to another operation.** Its answer may arrive
  after the attempt gave up.
- **A raced receive of the operation's channel goes through a `KeptReceiver`.**
- **A write given up is never reported as failed**, only as unknown: it may have been applied, and
  its identity is what a retry is judged by.

## Still open

- **Bundles of queries have no client-side bound either.** Their deadline travels to the server
  (`SendOptions::deadline`), which answers past it, but a server that stops answering a connection
  holds a bundle as it held an attempt.
- **The server's side of the same stall:** a put whose feed is full stops its connection's reader, so
  every later operation on that connection waits. It is filed beside this one from the same reading
  of item 305, as is keepalive on connections whose peer vanished.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `an_object_attempt_that_makes_no_progress_gives_up` (`shoal/tests/objects.rs`) | A put sent once on a connection whose line went silent waits past twenty seconds instead of being refused `OutcomeUnknown` within its bound, and a stat `Timeout` |

## Related

- [Item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
  the stall this was found reading for.
- [Resolved #307](a-lane-whose-writer-stops-ends.md), the holder's side of a lane that stopped
  answering.
- [Resolved #125](retry-unknown-outcome.md): a write retried after an unknown outcome is reported as
  unknown.
- [Resolved #230](object-connection-held.md): an attempt's connection goes back to the pool once its
  frames are written.
- [The client](../../api/client.md#construction): its deadlines.
