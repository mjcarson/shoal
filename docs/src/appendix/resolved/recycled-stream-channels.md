# 141. A stream that failed handed its channel, and its late answers, to the next stream

## Symptom

Rolling upgrades under load on the lab ended with bench workers that never finished. Three of
them waited more than a minute after closing their stream for an end that never came, with
nothing owed to them by their own count. Another run hung for more than eight minutes. With every
response that did not match an outstanding query logged, one run printed thousands of lines like:

```text
    337 an unexpected answer for index 90: Get None
    337 an unexpected answer for index 3: Get None
```

These were answers for indexes the stream had already been answered for, hundreds of times each.

## Cause

Two things, the second only possible because of the first:

- **A result stream that ended on an error returned its channel pair to the client's reuse queue**,
  in `release`, exactly as one that reached its end did. A stream that failed can still have
  answers queued in its channel, and answers on their way to it.
- **Since [#138](stream-bundle-identity.md) a bundle's slot outlives its stream** until the
  bundle's answers are in. When a bench worker's stream failed on a node restart, the worker opened
  a new stream, which was handed the same channel pair. The old stream's bundles' answers kept
  arriving through their slots, into the new stream's channel. Their indexes, counted from zero
  in both streams, collided with the new stream's own.

A duplicate index then stalled the stream. The unordered stream put an index below `next_index`
into `pending`, where it sat ahead of every real gap forever, so `next_index` never reached the
end the stream was closed at. The ordered stream's reorder buffer had the same flaw.

## Evidence

**Established on the lab, then reproduced in a unit test.** On the lab: the hung workers and the
unexpected answers above, in the upgrade-under-load runs of the
[correctness page](../../cluster-testing/correctness.md#rolling-upgrade-under-load).
`a_late_answer_for_a_closed_stream_is_dropped` delivers a late answer for a closed stream's bundle
slot. With the new check in the relay disabled:

```text
test client::tests::a_late_answer_for_a_closed_stream_is_dropped ... FAILED
a late answer for a closed stream was delivered into its channel
```

## The fix

In `shoal-client/src/client.rs`:

- A query stream and its result stream share an `open` flag, and every bundle slot carries it. The
  relay drops a frame for a bundle of a closed stream and removes the slot. `fail_waiting` fails
  no closed stream.
- `release` hands the channel pair back only after a clean end. A stream that failed drops its
  pair.
- Both result streams drop an answer for an index they have already been given, with a `WARN`,
  instead of keeping it where it would block the stream's end.

The next upgrade under load logged no unexpected answer and no stream that did not reach its end,
and every acknowledged insert was read back through every member.

## Alternatives rejected

- **Removing a stream's bundle slots when it ends.** It needs the result stream to know every
  bundle its query stream sent, which is shared state the relay then has to lock on every frame.
  The flag is one atomic load per delivery.
- **Draining the channel before recycling it.** An answer can still be in the relay, past its
  lookup and before its send, when the drain runs, so the drain only narrows the window.

## Invariants to uphold

- **A channel pair is reused only when nothing can be delivered into it any more.** A clean end
  means every index was answered, so every bundle of the stream is settled and its slot gone.
- **A result stream never holds an index twice.** Every index below `next_index` has been handed
  out, and every index in `pending` is waiting its turn. A second copy of either is dropped.

## Still open

- The server was not shown to send a duplicate on its own. Every unexpected answer on the lab
  disappeared with this fix, so they were all the old streams' late answers. The dedupe stays as a
  guard.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_late_answer_for_a_closed_stream_is_dropped` (`shoal-client/src/client.rs`) | A late answer for a closed stream is delivered into the channel the next stream may be using |
| Rolling upgrade under load ([cluster testing](../../cluster-testing/correctness.md#rolling-upgrade-under-load)) | Streams opened after a failure receive another stream's answers, and some never end |

## Related

- [Resolved #138](stream-bundle-identity.md), whose per-bundle slots made this reachable.
- [Resolved #60, 130, 131](stream-connection-accounting.md), the stream accounting it builds on.
