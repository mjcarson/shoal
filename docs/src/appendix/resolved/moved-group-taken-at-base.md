# 246. A shard that never read a moved placement group placed by its pool's pin

A write, a put and a read place a stripe by its placement group's state as the coordinating shard
last read it, and a group the shard never read is taken at its pool's base, the pin
([F88](../../features/recovery-and-moves.md#how-a-writer-learns-its-groups-state)). Before any group
moved that was the truth. Once a failed device's groups moved, every shard's first operation on
each moved group placed on the slices the group had left. Found by F88's lab run and filed and
fixed in one change.

## Symptom

`shoaladm bench`'s `device_fail` arm on a copy of the lab's three nodes, `rw50` at 1 MiB, two in
flight, at `13b649d`: every placement group switched within five and a half minutes of the failure,
and then the pool's stale positions climbed while the rebuilds ran:

```text
01:15:15 target 6  moves 0  behind 0  stale 210  rebuilt 2884  switched 2715  retired 1
01:17:16 target 6  moves 0  behind 0  stale 512  rebuilt 3670  switched 2715  retired 1
01:19:17 target 6  moves 0  behind 0  stale 923  rebuilt 4326  switched 2715  retired 1
```

About half the puts after the moves left a position missed, so the arm never settled.

## Cause

A put stages its pieces where `place_stripes` names them, from `MetaClient::placement` (gone with
the fix), which was the shard's cache or the group's base. A shard's cache holds a group only once that shard has read
it: after a refusal, or in a put's check once its pieces are durable. The check found the group had
moved and marked the arrival missed, which kept the put correct, and cached the state for that
shard alone. With 18 shards and 2,715 moved groups, nearly every put after the moves was some
shard's first to its group.

A write in place met the same, as a commit refused for the epoch it claimed and tried again, and a
read as a holder that does not have the row's label.

## Evidence

**Reproduced** by an addition to `an_operator_fails_a_device_and_its_groups_move_off_it`, written
first and run against `13b649d`'s placement: once the moves settle, eight puts through each node,
and stale + rebuilt, which a rebuild leaves as it was, read before and after.

```text
assertion `left == right` failed: 24 puts after the moves left 11 positions missed
```

None with the fix.

## The fix

**A group a shard never read is read, linearizably, once its pool's target has moved past its pin**
(`Meta::placement_for`, `Placement::may_have_moved`). Before any target moves a group is still taken
at its base, so a cluster that never moved anything places as it did. The read is the one a refusal
already makes, and its answer is cached for the shard's next operation on the group. Puts, writes
in place, a small write's fold and clear, and reads all place through it.

## Alternatives rejected

**Taking every group at its pool's target.** A group may still be moving, or may never move if its
holders there are the same; a commit claiming the target's epoch would be refused for it.

**Pushing every group's state to every shard.** A tablet map entry for each of thousands of groups,
changed by every switch: the cost [Resolved #243](move-slots-on-the-map.md) took out of the slots.

## Invariants to uphold

- **A shard places a group by state it read, or by its base only while nothing in its pool has
  moved.** Safety never rested on the cache, since a stale claim is refused and a put checks its
  group after its pieces are durable; staging on the slices a group left is the cost this
  prevents.

## Still open

Nothing of this defect.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `an_operator_fails_a_device_and_its_groups_move_off_it` | `shoal/tests/cluster_fixture.rs` | Now putting through every node after the moves: a shard that never read a moved group stages on its old holders and leaves its arrival stale |

## Related

- [F88](../../features/recovery-and-moves.md), whose lab run found it, with
  [Resolved #243](move-slots-on-the-map.md) and [Resolved #245](control-log-rewrite-borrow.md).
