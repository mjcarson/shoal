# 297. A restore failed a group on what a retry would ride out

A restore drives each group of a backup through loading, installing and verifying, each committed
before the next ([F49](../../features/backup-and-recovery.md)). A group that fails is never
restored by running the restore again, which is refused; it waits for an operator's
`restore-retry`. So a failure must mean something a retry by the driver would not mend.
[Resolved #155](restore-retries-unreachable.md) made a member out of reach before the install such a
moment, `TRANSIENT`: the driver waits five seconds and drives the group again, up to
`RESTORE_ATTEMPTS` (12) times.

Filed from F98's workspace run (2026-10-22); fixed in the flaky-test plan's first wave
(2026-10-27), with two more of the same kind found while reproducing it.

## Symptom

Under load a restore failed groups for good on moments that pass:
- `backup_restore_verifies_history_in_new_cluster`: a group *came to Failed: … did not report after
  the restore: no report within 20s: the replication rpc timed out*, in F98's run, and in F91's
  with its message not kept.
- `a_restore_rides_out_an_unreachable_member`: a group *came to Failed: proposing the scrub of group
  …: raft stopped*, in F98's run, where a member was only out of reach for a while.
- Found while reproducing them: a group failed at its install, *refused the snapshot: group … is
  installing a snapshot already*. Its fresh copy was being caught up by a snapshot of its own when
  the restore's file arrived. One run of three of the new row met it.

## Cause

- **After the install, nothing was transient.** #155 marked a member's missing report `TRANSIENT`
  in the loading scrub. The verifying scrub's missing report, and a scrub that could not be
  proposed because the group's handle stopped under a restart, fell through to a failure.
- **An install meeting an install under way was refused.** A copy installing a snapshot answered a
  repair stream's begin `Refused` (`begin_snapshot`), and the restore's own copy's install through
  its loop failed the same way. Both failed the group, where a full stream budget is answered
  `Busy` and waited out ([Resolved #257](restore-waits-for-stream-budget.md)).
- **Found on the way: a group's attempts went back.** A hand-back committed `attempts` one higher,
  and the loop drove the group again at once, from its map. The map did not carry the commit yet,
  so the next drive counted from the old value, and its final commit wrote that back over the
  higher one. A group's record named fewer retries than it took, and the bound of twelve counted
  slower than it says.

## Evidence

**Reproduced, each with a fault on the tree before its fix.** `a_restore_rides_out_what_passes_in_a_moment`
was written first. Every node of the restoring cluster puts one fault on its restore, a run with
each: a verifying scrub's report dropped (`FAIL_VERIFYING_SCRUBS 1 report`), its proposal failed as
a stopped handle's (`… handle`), and its first install finding the group installing already
(`FIND_INSTALLS_BUSY 1`). Before the fix:

```text
with a report fault on its verifying scrub, group 12649576434620370261 came to {"Failed":{"reason":"5a8428f8-6e9b-4237-8e6b-6a1786e1c172/0 did not report after the restore: no report within 20s: a test dropped it"}}
```

and with the verifying faults fixed, the install's answer put back to `Refused` and the restore's
wait taken out:

```text
with a install fault on its verifying scrub, group 11926530430908170880 came to {"Failed":{"reason":"Unreachable node: shoal_core::server::replication::network::LinkFailed: the replication link failed: 8ea018fd-43d6-4d0e-9d36-80191b5a1a4c/0 refused the snapshot: group a5838c3cc92b0280 is installing a snapshot already"}}
```

With the verifying faults made transient, every group came to `Restored`, and every record said
`"attempts":0`. The child logs showed the hand-back (*trying again … attempt=1*), then a drive from
`Verifying`: the count was lost between them. The install race itself met a real run once, before
its fix: *group 788867a95b59f5a5 is installing a snapshot already*, `failed_in: Installing`, its
copy caught up by a snapshot when the restore's file arrived.

## The fix

- **A verifying scrub that misses a member's report, or cannot be proposed, is `TRANSIENT`.** The
  group is driven again from `Verifying`, with the handle its shard holds then, within
  `RESTORE_ATTEMPTS`. A copy that does not verify or that stalled on a partition, and copies that
  disagree, still fail the group by name.
- **A repair's or a restore's stream meeting an install under way is answered `Busy`**, which its
  sender waits out within the transfer's deadline as it waits out a full stream budget. The
  restore's own copy's install through its loop waits the same way (`INSTALLING_ALREADY`).
  Openraft's own streams are refused as before; their sender takes `Busy` and `Refused` alike.
- **A drive's attempts go to the loop with its phase** (`RestoreDone::attempts`, kept in
  `DrivenRestore`), and the next drive counts from the larger of those and the map's.

## Alternatives rejected

- **Retry the verifying scrub inside the drive.** The drive's handle may be the stopped one. Handing
  the group back takes the handle its shard holds then, and leaves the pause and the bound in one
  place.
- **Take every failure after the install as transient.** A copy that answers and does not verify
  will not verify on the next try; retrying it only delays what an operator has to see.
- **Answer openraft's own streams `Busy` too.** Their sender maps both answers to unreachable, so
  nothing would change.

## Invariants to uphold

- **After the install, only a failure to reach or to ask is transient.** A copy's answer that it
  does not hold the file is a failure.
- **A repair's or a restore's install never fails for an install under way**; it waits within its
  transfer's deadline.
- **A group's attempts never go back.** The loop drives from the larger of its own count and the
  map's, as it does with the phase.
- **The retries stay bounded** by `RESTORE_ATTEMPTS`.

## Still open

- [Item 296](../known-issues.md#296-a-restores-table-group-stays-at-installing-under-load-and-its-bucket-archives-wait-behind-it),
  restores whose table group stays at `Installing` with no driver, is another defect and still open.
- The install race is reproduced through a fault. No row arranges a real catch-up by snapshot under
  a restore.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_restore_rides_out_what_passes_in_a_moment` (`cluster_fixture`) | With a report fault a group fails *did not report after the restore*, with a handle fault *proposing the scrub*, and with an install found busy *is installing a snapshot already*; with the attempts' fix reverted, no group's record counts its retry |
| `a_restore_rides_out_an_unreachable_member` and `backup_restore_verifies_history_in_new_cluster` (`cluster_fixture`) | Under load, their groups fail as F98's run recorded |
| `a_restore_waits_out_a_members_stream_budget` (`cluster_fixture`) | Unchanged: the stream budget's `Busy`, waited out |

## Related

- [Resolved #155](restore-retries-unreachable.md), the rule this extends past the install.
- [Resolved #257](restore-waits-for-stream-budget.md), the `Busy` an install under way now answers
  too.
- [Resolved #327](restore-figures-kept.md), which keeps the installed file's figures for the drive
  that finishes a group handed back.
- [Item 296](../known-issues.md#296-a-restores-table-group-stays-at-installing-under-load-and-its-bucket-archives-wait-behind-it).
- [F49](../../features/backup-and-recovery.md).
