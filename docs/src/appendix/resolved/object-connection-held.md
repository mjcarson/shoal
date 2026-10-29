# 230. An object operation held its connection until its answer came back

[F80](../../features/object-wire.md)'s client sent every object operation on one of the
connections F73 sets apart for long streams, and kept it until the operation's answer had come
back. A bundle of queries gives its connection back to the pool once its frames are written, and
its connection's reader routes the answers by id. Two connections are set apart by default, so a
client carried two object operations at a time however many it had in flight, and the rest queued
for a connection. It was found by M13's own baseline, on its first arm.

## Symptom

On the lab, `shoaladm bench`'s object arms on one node (titan, driven from europa over 1 GbE)
put 4 KiB objects at 304 a second, eight in flight. The median was 5.9 ms and the p99 609 ms.
Nearly every second of the run had a put that took one to five seconds:

```text
put100/s4k
   3 measure 259 p50 7.563 p99 537.599 max 2033.663
   4 measure 260 p50 7.571 p99 738.303 max 3803.135
   8 measure 313 p50 5.787 p99 1539.071 max 4050.943
put100/s256k
   4 measure 184 p50 10.199 p99 5402.623 max 13377.535
```

Gets showed no tail. At 16 MiB, one run's p99 was 12.8 s. The node logged nothing at `WARN` or
above, and the bench counted no error and no retry: the time was spent in the client.

## Cause

`Objects::once` (`shoal-client/src/client/objects.rs`) took a connection with `connection(op)`
and held that `PooledConnection` to the end of the operation. That covered writing its frames,
and also waiting for its answer on the channel the connection's reader routes it to. The answer
needs nothing from the pooled handle. `ShoalConnection`'s own documentation says a bundle's
connection "goes back to the pool once its frames are written", so a cancel reaches its write
half through `ConnShared` and not through the handle. The object path was written without that
rule.

A put of 4 KiB held a connection for about 6 ms, its round trip, and two connections carry about
330 such round trips a second. The arm drove eight in flight and saturated at 300, so six
operations were always queued for a connection. The pool's waiters are not served in strict order,
so a few waited seconds while most waited none. A get's round trip is 0.3 ms, short enough that
its queue never built a tail.

## Evidence

**Reproduced**, three ways:

1. **The lab capture.** The figures above are from `f80-one`, `shoaladm bench`'s capture of the
   build at `9d7975e` on titan (Ryzen Embedded V1756B, `performance` governor). Every put arm at
   4 KiB and 64 KiB had a p99 of 0.3 to 0.8 s over four runs.
2. **A probe.** Timings printed by the client around each step of `once`, on the same arm, put
   every slow operation's time in the wait for a connection. Only connections 11 and 12, the two
   set apart, ever appeared:

   ```text
   OBJPROBE once put s4k/1c/dc/0000000000000022 conn 3.186406652s written 3.186418565s answered 3.193339978s conn_id 12
   OBJPROBE once put s4k/b8/ca/0000000000000015 conn 1.339320498s written 1.339341078s answered 1.343694153s conn_id 12
   ```
3. **A test written first.** `object_operations_share_a_connection_while_they_wait` puts every
   answer a quarter of a second late through a delay line, on a client with one connection set
   apart. On the unfixed tree six stats sent together took six round trips:

   ```text
   test object_operations_share_a_connection_while_they_wait ... FAILED
   six stats on one connection took 1.515722891s, as if each waited for the answer before it
   ```

## The fix

`once` drops its pooled connection as soon as the operation's frames are written: the head, and a
put's data frames. It keeps the connection's id, and then checks whether that connection's reader
has stopped, as before. The answer arrives on the channel `track_response` registered under the
operation's id, whichever operation holds the connection by then.

On titan, from europa, with the fix:

| Arm | Before | After |
| --- | --- | --- |
| `put100/s4k/c8` | 304/s, p50 5.9 ms, p99 609 ms, worst second's max 5.5 s | 1,043/s, p50 7.2 ms, p99 14.7 ms, worst second's max 31 ms |
| `put100/s64k/c8` | 259/s, p50 7.9 ms, p99 713 ms | 673/s, p50 11.5 ms, p99 24.6 ms, worst second's max 39 ms |

## Alternatives rejected

**More connections set apart.** It would have moved the knee, not removed it: a client with
sixteen operations in flight on two connections would have queued fourteen. X11 found that a
connection carrying a stream hurts a small request's tail, so connections are set apart. That
says nothing about how many operations may wait on one.

**A connection per operation in flight.** That is a pool sized by the caller's concurrency. A
bundle does not need one, because the connection's reader already routes answers by id, and
neither does an object operation.

## Invariants to uphold

- **No operation holds a pooled connection while it waits for its answer.** Its frames are
  written while it holds the connection, and its answer is read from its own channel. A cancel
  of it reaches the write half through `ConnShared`.
- **A put's data frames are written while the connection is held**, so no other operation's
  frames reach that connection in the middle of them. Releasing the connection before the last
  data frame was written would interleave two streams' frames on one connection.

## Still open

- Nothing measures how many operations one connection set apart carries before its writes queue.
  At 16 MiB a put's frames hold a connection for the whole put, about 0.15 s on 1 GbE, so two
  connections still carry two large puts at a time. That is the network's limit on the lab and
  not a defect, but a host with a faster link would meet it.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `object_operations_share_a_connection_while_they_wait` | `shoal/tests/objects.rs` | Six stats on one connection set apart take six delayed round trips (1.5 s) where one is expected (0.25 s) |

## Related

- [F80](../../features/object-wire.md), whose client this was and whose baseline found it.
- [F73](../../features/bodies-across-frames.md), the connections set apart.
- [F75](../../features/client-cancel.md), which gave a connection's write half to whoever cancels on it.
