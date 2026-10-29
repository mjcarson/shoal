# 321. `a_cancel_cuts_a_stream_being_written` read a cancelled stream faster than its server could cut it

The row asks for a 16 MiB answer as a stream of 64 KiB frames, on a raw connection whose buffers
are small, so the server's writer is held part way through
([F75](../../features/client-cancel.md)). It sends a cancel once it has read the stream's first
data frame, reads until the cancel's acknowledgement, and asserts the stream never reached its last
frame and stopped well short of sixteen mebibytes.

A cancel reaches a stream's writer through the connection's shard. The read relay hands it to the
shard's queue behind the bundles it read. The shard records it on the node's board, counts it
`received`, and hands it to the connection's writer. The writer takes it between two frames and cuts
whatever is left of the stream.

The row read on at memory speed as soon as it had sent its cancel, which let the writer go. Under
load the writer wrote the rest of the stream before the shard reached the cancel. The cut then found
nothing left to cut. This was the row's own race, as the item's fix direction allowed: F75 cuts a
stream once its writer has the cancel, and promises nothing about how soon a busy shard hands it on.

## Symptom

In a run of six suites at their default threads beside item 317's fix (2026-10-26):

```text
the cancelled stream was written to its end
```

It passed in three runs of its binary after. Nothing was answered wrong.

## Cause

The row's premise is that the socket holds the writer back, but it released the writer itself. After
the cancel it read every frame as fast as the frames arrived, so the writer was held only by how
soon its shard handed it the cancel. On a loaded host that was later than a client takes to read
sixteen mebibytes over loopback.

## Evidence

**Established by reproducing it.** Since item 317's change the row's message names the frames it read
and the server's cancel counters. Fifteen runs of the cancel suite beside the five suites it first
failed beside, each at its default threads (2026-10-26), failed five times: four with the stream
written to its end, and one with 254 of its 257 frames written:

```text
the cancelled stream was written to its end, 257 data frames, the server's cancels CancelCounters { received: 1, forwarded: 0, refused: 0, dropped: 0, dropped_bytes: 0, cut: 0, unrecorded: 0 }
254 data frames were written
```

The shard had handled the cancel by the time the row read the counters, after the acknowledgement:
`received` is counted as the shard records it. The writer had found nothing of the stream to cut:
`cut` is counted as it cuts one.

## The fix

**The row reads nothing more after its cancel until the shard has handled it**: it waits for
`received`, as its sibling `a_cancel_is_read_while_answers_are_owed` does, then reads until the
acknowledgement. The full socket holds the writer part way through a frame, and the shard hands it
the cancel in the same step that counts it. Once the row reads, the writer finishes that frame,
takes the cancel and cuts the stream. The row as fixed passed fifteen runs beside the same five
suites.

A first version read a frame at a time with 20 ms between, as a reader slower than memory does. It
passed fifteen runs beside the same five suites, but it still raced the shard, only with seconds to
spare instead of milliseconds. Waiting for the count does not race it.

## Alternatives rejected

- **Waiting for the stream's cut before reading.** The writer takes the cancel only between two
  frames, and a full socket holds it inside a frame's write: it cannot cut until the row reads, so
  the row would wait on a writer waiting on the row.
- **Reading at a pace.** It narrowed the race without closing it.
- **Cutting a stream from the read relay at once.** The server could do that, and it would spare a
  client that drains at memory speed the bytes it no longer wants. It changes what the server does
  under load, not what the row tests, and is filed as
  [O138](../optimizations.md#o138-a-cancel-cuts-a-stream-only-once-its-shard-hands-it-on-which-a-client-reading-at-memory-speed-outruns-under-load).

## Invariants to uphold

- **A row that tests a cancel's cut holds the writer until the cancel has reached it.** It reads
  nothing that would let the writer run past the cut, until the shard has handled the cancel.
- **F75 cuts a stream between two frames once its writer has the cancel.** ~~Nothing bounds how soon a
  busy shard hands it on.~~ Since [F114](../../features/early-cancel-cut.md) the writer has it as soon
  as the read relay reads it, for a stream already begun; the row's own wait for the shard still
  holds every other answer of the bundle, which only the shard's word takes back.

## Still open

- ~~**A client that keeps reading a cancelled stream at memory speed is sent all of it under load**
  ([O138](../optimizations.md#o138-a-cancel-cuts-a-stream-only-once-its-shard-hands-it-on-which-a-client-reading-at-memory-speed-outruns-under-load)).~~
  Built by [F114](../../features/early-cancel-cut.md): a stream already begun is cut as the read relay
  reads the cancel, and `a_cancel_cuts_a_stream_its_shard_has_not_handed_on` reads one at memory speed
  with its shard held.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_cancel_cuts_a_stream_being_written` (`cancel.rs`) | Under load the stream reaches the client whole before its shard hands the writer the cancel: five runs in fifteen beside the five suites |
| `a_cancel_is_read_while_answers_are_owed` (`cancel.rs`) | Unchanged: the sibling that already waited for the cancel before reading |

## Related

- [F75](../../features/client-cancel.md): a client cancelling a bundle it stops reading.
- [Resolved #317](owed-connection-kept.md): the change the row first failed beside.
- [Tests That Fail Under Load](../flaky-tests.md), where the row's failures are recorded.
