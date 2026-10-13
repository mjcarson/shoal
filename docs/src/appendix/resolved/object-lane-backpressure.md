# 238. A put that outran 1 GbE was refused by holders that were keeping up

An object executor's link to a remote slice has a queue bounded by `object_queue_bytes`, and a
request that did not fit was refused at once as unavailable ([F86](../../features/replicated-pools.md#the-object-lane)).
A put's pipeline sends every piece to every holder of its stripe. On the lab's 1 GbE, eight puts at
once outran the link and filled its queue, so a holder that was taking bytes as fast as the network
carried them was counted as one that did not answer. Found by F86's three-node bench leg, and fixed
before its figures were taken.

## Symptom

`shoaladm bench` on a copy of the lab's three nodes failed its preload at 16 MiB, eight puts in
flight, four of them refused after the client's 30 seconds of retries:

```text
failed: the object preload at s16m failed 4 puts: {"put": KindSummary { ok: 28, ... p99_ms: 27656.191, ...
errors: {"NotReady": 4}, samples: {"NotReady": "Object { ..., outcome: NotReady, msg: \"stripe 1 is held by 1
holders and needs 2: position 1: Refused { code: Unavailable, message: \\\"the object lane to slice
01a133a8-5c15-7491-96c2-6d47565434f7 on node 9626234e-922a-4df0-9fdf-489364a0ea88 holds its bound of 167...
```

Every node was up and every device healthy. The 4 MiB arm before it finished with a p99 of 5.4 s
against a median of 378 ms, its worst second's p99 8 s: puts refused the same way and tried again
by the client. One node, whose holders are all local, never met it.

## Cause

A coordinating node takes a put's bytes from the client at up to the link's 112 MiB/s and sends
each byte to two remote holders, so its outbound link needs twice what its inbound one carries. The
pipeline cut pieces and called `Lanes::send` for each holder without waiting. `send` was
synchronous and refused a frame that would pass the queue's bound, the module's invariant being
that "a request that does not fit is refused at once". The pipeline took a refused piece as a
holder that did not answer. A stripe needs `k + f` holders, two of three, and both remote ones
refused, so the put was answered `NotReady` and the client tried again into the same full lanes.

The bound was meant to stop a stalled holder's lane from growing without limit. Refusing at once
also refused a holder that was merely behind, and on 1 GbE every holder of a large put is behind.

## Evidence

**Reproduced** by `a_full_lane_holds_its_sender_until_the_holder_takes_its_bytes`, written first
and run on europa against `0728387` with its last assertion, on a counter the fix adds, left out.
Two stores over loopback, the sender's lane bounded at two chunks and a little more, eight whole
chunks sent back to back from the sender's executor as a put's pipeline sends them:

```text
test server::objects::lane::tests::a_full_lane_holds_its_sender_until_the_holder_takes_its_bytes ... FAILED
thread '...' panicked at shoal-core/src/server/objects/lane/tests.rs:392:5:
a live holder's requests were refused at a full lane: [Done, Done, Refused { code: Unavailable, message: "the
object lane to slice 01a133b0-68c8-7f91-bb0b-fbef5aff86e3 on node 461697bc-87dc-46dc-ad97-1a68f2a9388d holds its
bound of 589824 bytes; its holder is not keeping up" }, Refused { ... }, Refused { ... }, Refused { ... },
Refused { ... }, Refused { ... }]
```

Six of eight were refused while the holder was reading. With the fix all eight are written, none
is shed, and the sender waited for room.

## The fix

**A full lane holds its sender while it writes.** `Lanes::send` is async and returns once the
request is on the link's queue. A request that does not fit waits, looking again every
millisecond. While it waits, the link's count of frames written (`Link::written`) is watched: a link
that writes anything is a holder taking bytes, however slowly. One that writes nothing for
`STALL_AFTER`, five seconds, is a stalled holder's, and the request is refused then, as unavailable
and counted as shed. A refusal comes back to the caller at once as `Err`, never through the answer
channel, and `LaneFigures::waited` counts the requests that waited.

**Both callers wait.** A put's pipeline on an object executor awaits `send` directly. A request from
a table shard, a staged write's piece, is handed to an object executor with a channel that says
when it is queued, and the shard waits on it, so a write in place is held to its holders' rate as
a put is. Its executor serves that wait on a task of its own, so a mailbox is never held.

**A put sends a refused holder nothing more.** Once a holder's send is refused, the pipeline
marks it gone for the rest of the put, and every later stripe counts it missed. A stalled holder
then costs a put one `STALL_AFTER`, not one for each of its stripes.

## Alternatives rejected

**A larger bound.** It moves the point at which a live holder is refused without removing it:
eight puts of 16 MiB outrun any bound smaller than what they carry, and a bound that large is the
unbounded queue the bound exists to prevent.

**Retrying a refused piece in the pipeline.** A pipeline that slept and sent again would be the
same wait, written once for each caller, with a refusal counted for every try. The lane can see
whether its link is writing, and a caller cannot.

**Waiting on the link's writer instead of looking every millisecond.** A waker from the writer
would save the wakeups, at the cost of a second waker list in `peer::link`, which every other lane
shares. A put waits a few milliseconds at a time on a link moving a mebibyte every nine, so the
look costs nothing measurable; it is recorded as a choice, not left as a gap.

**Dropping a slow holder as soon as `k + f` keep up.** It would let a put finish at the faster
holders' rate, and leave a stale position on a live holder that M16 must rebuild, for every put the
network is the limit of.

## Invariants to uphold

- **A request waits at a full lane only while the lane writes.** The stall is judged by frames
  written, never by time in the queue alone, so a slow holder holds its senders and a stalled one
  releases them after `STALL_AFTER`. Anything that changes how a link counts what it wrote changes
  what a stall is.
- **A sender waits until its request is queued**, from an object executor and from a table shard
  alike. A path that posts a request and returns before it is queued has no backpressure, and its
  requests pile up behind the bound unseen.
- **No mailbox waits for room.** An executor answers a shard's remote request on a task of its own.
- **A refused holder is told nothing more of the put.** The rows record it missed, and M16
  rebuilds it.
- **Nothing but the senders to a stalled holder waits for it**, which is S13's invariant: other
  lanes, other holders' sends and the table shards go on.

## Still open

- A put still waits on its slowest holder: every piece goes to every holder in turn, so a holder
  that is slow but writing sets the put's rate. A put that went at `k + f`'s rate would leave
  missed positions for M16 to rebuild, which is not done before M16 exists.
- `STALL_AFTER` is a constant, not a setting.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_full_lane_holds_its_sender_until_the_holder_takes_its_bytes` | `shoal-core/src/server/objects/lane/tests.rs` | A live holder's requests are refused at a full lane, and a put that outruns its link loses its holders |
| `stalled_object_lane_leaves_the_others_answering` | `shoal/tests/cluster_fixture.rs` | Now through a 4 MiB lane and a 64 MiB put: a stalled holder holds its sender past `STALL_AFTER`, or a put beside it fails |

## Related

- [F86](../../features/replicated-pools.md), whose three-node bench leg found it.
- [S13](../../object-storage/isolation.md#a-lane-for-object-bytes), the object lane and its bound.
- [Resolved #237](object-store-starts-after-the-shards.md), the other refusal F86's lab run found
  that a client could not recover from.
