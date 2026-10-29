# 302. The lease reads row counted the groups' heartbeats against its bound on rounds

A strong read on a group's leader confirms the lead before it is served. Without lease reads it
sends a round: an empty append to every other voter, answered by a quorum. With them
([F100](../../features/lease-reads.md)) a leader a quorum acknowledged within half its lease serves
the read with no round. `lease_reads_send_no_round_and_see_every_acknowledged_write` holds the
second claim. Its bound counted something else.

Filed from F102's and F107's workspace runs (2026-10-22 and 2026-10-23); fixed in the flaky-test
plan's first wave (2026-10-27).

## Symptom

In three loaded runs the row failed with *600 lease reads came with 304 appends sent*, 340 in
F107's run and 368 in W4's. It passed at three threads each time.

## Cause

The row reads 600 strong reads at `Quorum` with lease reads on. It bounded the empty appends the
nodes sent meanwhile at half the reads, 300, since a round for each read would be two appends, one
to each of the group's other voters. Its count was `RoundStats::sent`: every empty append a shard
sent and had answered. That counts heartbeats as well as rounds. openraft sends a read's round and a
heartbeat alike, as an empty append through a heartbeat client, so the network cannot tell them
apart ([F99](../../features/strong-read-rounds.md#design-choices)).

The groups' heartbeats go every half second whatever the reads do, so their count grows with the
time the reads take. A loaded host stretched the 600 reads past the time that left the bound room.
Every read was served inside its lease: the bound was measuring the clock.

## Evidence

**Reproduced.** The row was run with its reads paced 50 ms apart, as a loaded host paces them. The
old bound was kept, and the counts this fix adds were printed beside it:

```text
REPRO302 33.049888372s: 600 barriers, 600 by lease, 0 by round, 1186 appends sent
600 lease reads came with 1186 appends sent
```

Every barrier was obtained inside its leader's lease and none with a round. The 1,186 appends were
33 seconds of heartbeats. The fixed bound holds on the same figures.

## The fix

- **A leader counts each strong read's barrier by how it obtained it**, in two fields:
  - `RoundStats::by_lease`, a barrier inside its lease with no round;
  - `RoundStats::by_round`, a barrier with a round of its own.

  The count is taken in `reads::linearizer_of`, once the barrier is obtained, so it covers the
  leader's own reads and the hops it answers. The two fields ride the node's `Replication` report
  beside `sent`, and the fixture's `ROUNDS` reads them.
- **The row judges the rounds.** The leaders must have obtained a barrier for every read, and fewer
  than a quarter of the reads may have taken a round. That is the tolerance the appends' bound gave:
  150 rounds are the 300 appends it allowed. The appends sent are printed beside the count for a
  reader.
- **F99's row, which reads with lease reads off**, also asserts now that each get's barrier was
  obtained with a round and none by a lease.

## Alternatives rejected

- **Bound the appends by the reads' time and the heartbeat interval**, the item's second direction.
  It needs the number of groups each node leads, each group's followers, and heartbeats sent on
  time, and a loaded host is the one that will not keep to that. A count of rounds needs none of it.
- **Tell rounds from heartbeats by their buckets**, the item's first direction. A bucket says how
  long an append took, not why it was sent, and a round and a heartbeat on one link take the same
  time.
- **Mark a read's appends at the network.** openraft builds a read's probe itself, an empty append
  through a heartbeat client, so the network has no mark to read. The linearizer is the one place
  that knows which way a barrier went.
- **Lengthen the bound.** Any bound on heartbeats is a bound on time, and time is what a loaded host
  stretches.

## Invariants to uphold

- **A count of rounds is taken where the barrier is chosen**, never inferred from empty appends,
  which count every heartbeat.
- **A barrier is counted once, when obtained.** A refusal that sent no round is not a round, and a
  read that hops to its leader is counted on the leader alone.
- **A row bounds what the claim names.** This row claims no round was sent, so it counts rounds.

## Still open

- **A capture does not keep the two counts.** `shoaladm bench` reads the members' `barriers`, waits
  and lists into `MemberRounds` and leaves the new fields out. So a lab A/B of lease reads still
  infers its rounds from appends less heartbeats, as F100's page did. Recorded in
  [TODOs](../todos.md#a-capture-keeps-the-replication-reports-newer-counts).

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `lease_reads_send_no_round_and_see_every_acknowledged_write` (`cluster_fixture`) | Under load, the bound counts heartbeats: 1,186 appends with the reads paced 50 ms apart, every barrier inside a lease |
| `a_strong_reads_round_is_counted_by_its_parts` (`cluster_fixture`) | Its gets' leaders count no barrier with a round, with lease reads off |
| `a_round_is_counted_by_bucket_and_folded` (`shoal-core`, `replication::report`) | A node's report does not fold its shards' barriers by lease and by round |

## Related

- [F100](../../features/lease-reads.md), the lease reads the row holds.
- [F99](../../features/strong-read-rounds.md), the round counts the bound read.
- [Tests That Fail Under Load](../flaky-tests.md).
