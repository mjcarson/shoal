# 307. A served object lane whose writer stopped went on reading, and its asker waited with no bound

A holder answers the requests on an object lane through one writer task, and that writer stopped at
the first answer it could not frame or write. The lane's reading did not stop with it. Every request
read after was served and its answer dropped, the asker's link stayed up, and each request the asker
had sent waited for an answer that would never come, with no bound and nothing logged. The writer's
own comment said the lane ended.

Found while reading the put path for
[item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
whose stall it fits, and filed and fixed in one change, the first of the silent waits wave three set
out to bound.

## Symptom

- **Requests to a holder that answer nothing, on a link that looks up.** An asker's requests to a
  slice whose lane's writer stopped are neither answered nor refused, and its link never goes down.
  Every one waits on its waiter.
- **What waits on them waits too:** a put's stage on that holder, a read of its chunk, a rebuild's
  source. Nothing in the asker's lane gives up on a request once it is queued.
- **Nothing logged**, on either node.

## Cause

`Lanes::serve` reads requests and hands each to the slice, and `write_answers`, a task of its
own, writes the answers in the order they are ready (`server/objects/lane.rs`):
- On an answer that could not be framed or written, `write_answers` broke out of its loop and
  returned, dropping its write half.
- The read loop kept reading. Each request it served had its answer sent into the writer's channel,
  which nothing read any more, and the answer was dropped there.
- The socket stayed open, since the read half held it, so the asker's link saw no end and every
  request it had queued kept its waiter.

An answer cannot be framed when it is longer than the frame its asker agreed to take. It cannot be
written when the socket or its kernel TLS fails on the write alone. Either way the lane is useless,
and only the asker's link going down tells its requests so.

## Evidence

**Reproduced by `a_lane_whose_answers_cannot_be_written_ends`** (`server/objects/lane/tests.rs`),
written before the fix:
- node A's hello takes frames of 128 KiB at most;
- A asks node B for a unit of its chunk, which fits and is answered;
- A then asks for the whole chunk, 256 KiB and its head, which B cannot frame.

On the unfixed tree:

```
a read whose answer could not be written was left waiting, its lane up
test result: FAILED. 0 passed; 1 failed; ... finished in 10.19s
```

The read waited past the test's ten seconds, its lane up. With the fix, it is refused by the lane's
end in 0.2 seconds.

## The fix

- **An answer that cannot be framed or written ends the lane** (`write_answers`):
  - the writer logs it at WARN;
  - it sets a flag the read loop shares, and closes its write half, which shuts the socket down for
    writing.
- **The asker's link reads the end** and goes down: every request it carried is refused
  `Unavailable`, "the object lane dropped", and the link dials again.
- **The read loop stops** once the flag is set, before it reads another request, and the asker's
  close ends the read it is in.

## Alternatives rejected

- **Writing the next answers after a failed one.** An answer that could not be framed would be
  missing from a stream its asker reads in order by id. A write that failed leaves the stream mid
  frame.
- **Refusing the one request whose answer failed, and going on.** A frame half written cannot be
  taken back, and the stream after it would not parse. The lane cannot carry another answer.
- **A deadline on every request at the asker** in place of this. It would bound this stall too, but
  only after the deadline, and never say why. The lane's end says it at once. A bound on a client's
  attempt is its own item.

## Invariants to uphold

- **A served lane reads only while its writer writes.** Anything that ends the writer ends the lane.
- **A lane that cannot answer ends where its asker sees it**: the socket is shut, never left open
  with requests unanswered on it.
- **Every chunk request is answered once**, ~~by the holder or by the asker's link going down, which
  refuses every request it carried~~ by the holder, by the asker's link going down, which refuses
  every request it carried, or for a reader's read by its link's sweep once its answer has not come
  within its bound ([Resolved #316](unanswered-read-bound.md)).

## Still open

- **A request too large to answer in its asker's frame is refused by ending the lane,** which costs
  its other requests a refusal and a redial. Reads are cut to `max_read`, which keeps every answer
  inside a frame, so it is not expected outside a test that shrinks the frame.
- **The asker's other silent waits** are filed beside this one, from the same reading of item 305:
  a client attempt with no deadline, one put's wait holding its whole connection's reading, and
  connections with no keepalive.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_lane_whose_answers_cannot_be_written_ends` (`shoal-core`, `server/objects/lane/tests.rs`) | A read whose answer the holder cannot frame is left waiting with its lane up, past the test's ten seconds, instead of being refused by the lane's end |

## Related

- [Item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
  the stall this was found reading for.
- [F106](../../features/lane-copies.md): a served answer holds its buffers, their draw and its room
  until it is written.
- [Resolved #238](object-lane-backpressure.md): a full lane holds its sender while it writes.
- [F108](../../features/object-work-in-flight.md): a node's figures count the answers its lanes owe.
