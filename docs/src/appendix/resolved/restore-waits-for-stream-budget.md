# 257. A restore's install refused by its receiver's stream budget failed its group

A member's shard installs `cluster.migration.concurrent_streams` snapshots at a time and refuses
another, a refusal that passes in seconds. A restore's driver took it for the group's failure, so a
restore of a cluster of many groups failed some of them and finished only once an operator retried
it. [F93](../../features/operations-and-the-real-cluster.md) fixed it at M21, since the gate's exit
restores a bucket into a fresh cluster.

## Symptom

On the lab, in [Q31](../../object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)'s
run (E2), one restore of 450 groups into a fresh tmdb cluster recorded a group

```text
Failed { reason: "Unreachable node: … refused the snapshot: stream budget: 2 streams installing on
this shard, which is as many as it takes at once" }
```

and ended `finished with 1 failed`; `restore-retry` ([Resolved #155](restore-retry.md)) finished it
at once.

## Cause

The begin of a snapshot install answered the budget as `SnapshotAnswer::Refused` with its reason,
the same answer as a refusal that will never pass, and `GroupPeer::repair_snapshot` returned it as
an error. A move's step waits for its stream; a restore's driver, which installs every group's file
on every member at once, did not, and every leader sends at the same moment.

## Evidence

**Reproduced on the lab** in Q31's run (E2), above, once. **Reproduced in the fixture** by
`a_restore_waits_out_a_members_stream_budget` run with the driver's wait taken out, as the driver
was before the fix: three nodes whose shards install one stream at a time, each stream paced at
16 KiB/s so two leaders' installs on one member overlap.

```text
restored 3 groups
thread 'a_restore_waits_out_a_members_stream_budget' panicked at shoal/tests/cluster_fixture.rs:30755:9:
group 14582352021589088661 came to {"Failed":{"reason":"b266f152-…/0 stayed busy for the whole
transfer deadline: b266f152-…/0 is busy: stream budget: 1 streams installing on this shard, which is
as many as it takes at once"}} under a stream budget of one
```

With the wait the row restores every group and every note reads back.

## The fix

- **The budget's refusal is named**: `SnapshotAnswer::Busy`, carried to the sender as
  `SendError::Busy` and `RepairSend::Busy`, apart from every refusal that will not pass.
- **A restore's driver waits and tries that member again** within the transfer's deadline,
  `snapshot_timeout`, from 250 ms doubling to 4 s; only a member still busy at the deadline fails the
  group, by name.
- **A repair's driver** asks a busy target again after a second.

## Alternatives rejected

**A larger budget for a restore.** The budget bounds what a shard assembles at once, which a restore
loads hardest; raising it for restores trades the failure for memory.

**Serializing a restore's installs per member.** It would need a coordinator across every group's
leader, where waiting needs nothing.

## Invariants to uphold

- **A refusal that passes is never a failure.** Every driver that sends a stream treats `Busy` as a
  wait, within its own deadline.

## Still open

Nothing.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `cluster_fixture::a_restore_waits_out_a_members_stream_budget` | A restore under a stream budget of one fails a group |

## Related

[F93](../../features/operations-and-the-real-cluster.md), [F49](../../features/backup-and-recovery.md),
[F46](../../features/capacity-rebalancing.md)'s stream budget, [Resolved #155](restore-retry.md).
