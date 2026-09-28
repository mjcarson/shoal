# 187. A WAL segment recovered at a restart was never sealed when a rotation came first

Found in round 13 of the lab testing, and the cause of [item 142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s
longest-standing shape: `lost_response_retry_returns_original_result` stopping at *"group …'s
checkpoint never reached 5: [(0, Some(0))]"*.

## Symptom

The lost-response test kills node zero, restarts it, waits for its digests to match, and then
waits up to a minute for every node's checkpoint of one group to pass an index, sending `ROTATE`
and `COMPACT` to every node every 200 ms. Now and then node zero's checkpoint stayed at 0 for the
whole minute. It had failed that way since the item was filed: 4 runs in 17 alone at first, and
most suite runs since, at six threads. [Resolved #152](kanal-receive-races.md), a compactor job
lost to a timer, was a candidate and did not end it.

## Cause

A shard's WAL writer seals a segment when a batch for a newer generation arrives while it holds
that segment's file open. `ShardWal::rotate` depends on that: it moves the generation on and queues
the markers carried into the new one as a batch, *"which is what makes the writer seal the old
one"*.

At a restart, the WAL is replayed, and the last segment on disk comes back as the active one,
unsealed, with the writer holding no file. If the first thing after the restart is a rotation, not
an append, the writer's first batch is for the new generation. It opens that file and never touches
the recovered one, so the recovered segment stays unsealed for as long as the process lives.

The segment sweep (`sweep_segments`) hands sealed segments to the compactors in generation order,
and stops at the first one that is not sealed. So a shard whose recovered segment was never sealed
handed nothing to a compactor again: every later segment was sealed and waited behind it, and no
group on the shard ever moved its checkpoint. Its WAL grew until the process restarted. In the
test, a `ROTATE` that reached node zero before any entry did was enough.

## Evidence

**Reproduced**, first in the fixture and then in a unit test.

The fixture: the loaded loop (`target/lab/r13/142/loop.sh`, six heavy tests at six threads, the
failing round's child logs kept) caught it twice. In the kept logs node zero's restarted process
logged *"the wal sealed a segment"* for generations 2, 3, 4 and on, 126 in all, and never for
generation 1, the one it recovered. It handed no segment to a compactor. It sealed generation 2 at
04:58:52.19, before the first entry it appended, at 04:58:52.78. A diagnostic added for this round
(below) never fired, which ruled out a group that had not applied past a segment.

The unit test `a_recovered_segment_is_sealed_by_a_rotation_before_any_append` writes a frame,
closes, reopens, rotates before appending, then appends and rotates again. On the unfixed tree:

```text
the recovered segment was never sealed: [SegmentView { generation: 1, sealed: false, … },
  SegmentView { generation: 2, sealed: true, … }, SegmentView { generation: 3, sealed: false, … }]
```

On the fix the unit test passes. The same loop, with child logs on every child as when it caught
the stall, ran five rounds with no checkpoint stall, where the two loops before the fix had one each
in ten rounds between them. Its failures were deadlines under the logs' load, the lost-response
test's among them (a one-second delete not committed in time, answered `OutcomeUnknown` where the
test expects `Timeout`). Without child logs, eight rounds passed all 48 runs.

## The fix

**Before the writer opens a file for a batch while it holds none, it seals every older segment
still unsealed** (`seal_unheld` in `shoal-core/src/server/wal/mod.rs`). Each is synced first and
then marked sealed, and the seal hook fires as it does for any seal, so the shard sweeps. Only a
segment the writer never held can be in that state, and after a restart that is the recovered one.

A diagnostic came with the search: a sealed segment that has waited 30 s on one group that has not
applied past its last frame there is now said, and said again every 30 s while it holds (*"a sealed
wal segment waits on a group that has not applied past it"*, with the group, its applied and
committed index and whether it is installing). It did not fire in this defect, since nothing was
waiting on a group. It is kept because the other way a segment is held, a group that stopped
applying, was silent too.

## Alternatives rejected

- **Sealing the recovered segment at open.** The writer's position is at its end, and after a
  restart the first appends go into it, so it cannot be sealed until the generation moves. Sealing
  it when the writer first moves past it is the same rule the writer already has.
- **Letting the sweep skip an unsealed segment it can see is not the active one.** The sweep's
  order is what makes compaction apply a later write after an earlier one, and a segment the sweep
  skipped would have to be handed later, out of order. Sealing it keeps the one rule.
- **Making `rotate` seal the old segment itself.** The seal has to follow the sync of what was
  written to the file, which is the writer's, on the writer's task.

## Invariants to uphold

- **Every segment older than the active one is sealed once the writer has written past it.**
  The sweep relies on it to hand segments on in order, and a segment left unsealed holds every
  checkpoint on its shard.
- **A segment is synced before it is marked sealed**, whichever path seals it.

## Still open

Nothing of this item. Item 142's remaining shapes are deadlines under the suite's load, and the
restore stall ([#183](restore-driver-uncommitted-done.md)).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-core` `server::wal::tests::a_recovered_segment_is_sealed_by_a_rotation_before_any_append` | A recovered segment a rotation moved past is never sealed |
| `shoal` `cluster_fixture::lost_response_retry_returns_original_result` | A restarted node's checkpoint stays at zero for good when a rotation reaches it before an entry does, a run in a few under load |

## Related

- [F40](../../features/replication.md), the shared WAL and the sweep.
- [Resolved #152](kanal-receive-races.md), the earlier candidate.
- [Known Issues #142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host).
