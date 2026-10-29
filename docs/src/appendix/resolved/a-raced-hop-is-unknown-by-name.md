# 289. The dead primary row took a hop that raced its leader's death for a defect

A write hopped to a leader whose link goes down is refused `NotLeader` if the link never wrote its
frame. It stays unknown, `OutcomeUnknown`, if the link had written it: the leader may have acted on
it before it died ([F42](../../features/primary-failover.md)).
`a_dead_primary_fails_writes_only_until_its_election` kills a group's leader and writes to the group
every quarter second until it elects again. It asserted every failure was refused by name.

Filed from F96's workspace run (2026-10-20); fixed in the flaky-test plan's first wave
(2026-10-27).

## Symptom

In F96's workspace run, and in runs before the record, the row failed with *a write to a dead
leader's group failed OutcomeUnknown rather than NotLeader*. The row kept the code and not the
message. It passed at three threads after.

## Cause

The row's first write after the kill can race node zero's link to the dead node. A kill closes
node one's sockets, and node zero's link learns of it only when its reader next runs. A hop written
in between reaches a peer that is gone, and the link's drop answers it as written. That is unknown
by F42's rule, and the row did not allow it. The answer did not make the case plain either: a
written request's drop said only the socket's error.

## Evidence

**Established by reading the source, and the case shown by a row.** `ReplicationLink::down`
answered a written request with the link's reason alone. `a_hop_written_before_its_leader_died_is_unknown_by_name`
stalls node one's shard, so a write through node zero hops to it, is written and is not answered,
and then kills node one. On the tree with the old wording, the row's check of the message failed:

```text
the unknown hop did not say it was written: Err(IO(Os { code: 104, kind: ConnectionReset, message: "Connection reset by peer" })) (the member last sent 471.942538ms ago, its node 471.946957ms ago)
```

The run that failed the dead primary row kept no message, so its path is not shown. This is the one
path by which a hop to a killed leader is answered unknown inside the silence bound.

## The fix

- **A written request's drop says so**: *the replication link went down after the request was
  written, so it may have been acted on*, then the link's reason. An unwritten one's was already
  plain: *went down before the request was written*.
- **The dead primary row allows exactly that unknown**, by its message, and judges every other
  answer as before. Each failure prints its message.

## Alternatives rejected

- **Refuse a hop written to a socket the peer has closed.** The kernel holds a peer's close for a
  reader to see, and a check before each write would cost every hop a call. It would also narrow the
  window without closing it: a peer can die after the write.
- **Allow any unknown in the row.** A hop given up for silence, or a write that waited out its
  deadline, is what the row is there to catch.

## Invariants to uphold

- **A written request's drop is unknown, and says it was written.** Only a frame the link never
  wrote is refused.
- **The row allows the race and nothing else**, so a hop given up for silence or a timeout still
  fails it.

## Still open

- Nothing of this item.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_hop_written_before_its_leader_died_is_unknown_by_name` (`cluster_fixture`) | A written hop's drop says only *Connection reset by peer* |
| `a_dead_primary_fails_writes_only_until_its_election` (`cluster_fixture`) | Under load, a hop that raced the kill fails the row |

## Related

- [F42](../../features/primary-failover.md), written and unwritten requests.
- [Resolved #284](a-burst-starves-its-replication.md), whose messages say what the member last sent.
- [Tests That Fail Under Load](../flaky-tests.md).
