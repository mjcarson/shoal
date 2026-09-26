# 163. A repair's snapshot stream could be replaced by the group's own replication

## Symptom

In the third lab run of [#160](unreadable-partition-stalls-one-copy.md)'s fix, eight of titan's
copies stalled when it started. Five were repaired within five minutes. Two of the others, both
on titan's shard 1, had every repair fail the same way, for as long as the run lasted:

```text
a group's repair failed op=5e5ee04e… group=2c0307ebca0fd179 error="Unreachable node: … the
  replication link failed: 86cf66e8…/1 refused the snapshot at its end: group 2c0307ebca0fd179
  is still starting"
```

Over the same minutes the group's leader logged a snapshot the group's own replication had
tried to send to the same copy, and failed, about once a second:

```text
ERROR openraft::replication::snapshot_transmitter: ReplicationError while sending snapshot: …
  refused the snapshot at its end: group 2c0307ebca0fd179
```

## Cause

A shard assembles at most one received snapshot per group (`installs.partials`). A begin for a
new stream replaces the partial held for the group. A stalled copy whose start failed has no
handle, but the leader's openraft still thought it needed a snapshot (it was behind the leader's
purge point), so its transmitter kept starting streams. Each begin replaced the repair's partial,
and when the repair's end arrived, `end_snapshot` found openraft's partial and refused it: a
stream that is not a repair's, to a group with no handle, is "still starting".

The race needs a leader whose log is purged past the stalled copy, so the fixture, whose leaders
never streamed to a stalled copy, did not show it. The lab under load did, for two groups out of
eight.

## Evidence

**Found on the lab, established from its journals and by reading the source.** No test reproduces
it. `a_stalled_copy_survives_a_restart_and_an_operator_repairs_it` was extended to write past the
group's retained log while the copy was stalled, 1,400 writes at the most. With child logs, the
leader's transmitter never streamed to the stalled copy, and the test passed with the fix
disabled. It still passes, and it covers the path the fix touches, but it would not fail if the
fix were reverted.

## The fix

At the begin, a stream that is not a repair's is refused (`begin_snapshot`):

- by a copy with no handle, "still starting", before a byte is sent;
- while a repair's stream of the same group is still being assembled.

A repair's stream can still replace any partial, as before.

## Alternatives rejected

- **Hold a partial per stream, not per group.** It would let both streams assemble, and then both
  would install, one after the other. The copy should take the repair's.
- **Stop the leader's replication to a quarantined member.** The quarantine is committed through
  the node's report, a moment after the stall, and a member that is only quarantined for a checksum
  still has a live core that openraft may rightly need to feed.

## Invariants to uphold

- **A copy waiting for its repair takes only the repair's stream.** A copy with no handle refuses
  every other at the begin.
- **A repair's stream being assembled is never replaced by one that is not a repair's.**
  `is_assembling` bounds it: a repair stream abandoned half way no longer blocks the group's own,
  once it has failed or completed.

## Still open

- **The leader's transmitter keeps trying.** Refused at the begin, a try costs one message and no
  cut ([O71](../optimizations.md#o71-a-held-snapshot-is-cut-again-whenever-the-checkpoint-moves)),
  but openraft logs it at `ERROR`, once a second per stalled group, until the copy is repaired.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_stalled_copy_survives_a_restart_and_an_operator_repairs_it` (`shoal/tests/cluster_fixture.rs`) | Runs the path, a stalled copy with no handle repaired while its leader writes past its log. It does not reproduce the race, as above |

## Related

- [Resolved #160](unreadable-partition-stalls-one-copy.md), the stall.
- [F43](../../features/node-recovery.md), the snapshot stream and its partials.
