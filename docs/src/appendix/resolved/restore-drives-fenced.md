# 336. A restore's group failed for good when a second drive judged it non-empty by the first drive's install

A restore drives each group of a backup through three phases, each committed to the group's record
in the control log before the next: Loading proves every copy empty, Installing builds the group's
file and installs it on every member, and Verifying checks every copy's digest
([F49](../../features/backup-and-recovery.md)). A shard drives a group while it leads it. A group
that fails is never restored by running the restore again, which is refused; it waits for an
operator's `restore-retry`.

Found in the flaky-test plan's second workspace run after the confirmation loops (2026-10-28), and
fixed under wire 17 with the user's word on the design (2026-10-29).

## Symptom

`a_restore_rides_out_an_unreachable_member`, in workspace run two at `1e7c38a4`:

```text
group 1519707103153692445 came to {"Failed":{"reason":"1800a61c-…/0 holds 7 rows of the group's tablets; a restore is into an empty table, and this one is not"}}, where a member was only out of reach for a while
… "attempts":0,"driver":"24f53fbc-…","failed_in":"Loading","generation":0,"installed":null …
```

The cluster was new, and nothing but the restore wrote to it. The member's 7 rows were the 7 records
of that group's own restore file.

## Cause

**Two drives of one group overlapped, and the second started behind the record.**

- A shard drives a group from its own memory of the phase or its map's record, whichever is
  further, and either can be behind the control state. An install restarts the group's copies, and
  any member can win the election that follows. The drive that installed then hands the group back
  for its new leader, which drives it from what its map says. A node catching up on the control log
  after a partition, as node two was in the row, has a map behind the record.
- A drive is not stopped when its shard loses the lead. A deposed drive goes on with its quarantines
  and installs, and commits once it reaches the control plane.
- `apply_restore_progress` took any member's progress at the current incarnation and try: a phase
  moving back from Installing to Loading, and a `Failed` outcome from a drive whose lead was taken
  over, alike.

So a drive that started from Pending committed Loading over the record's Installing. Its check found
the rows the install it overwrote had put on every copy, and it failed the group. A retry puts a
group failed in Loading back at Loading, whose check finds the same rows, so the failure could not be
retried either.

**Found on the way, by reading:** a drive that never learned its group was done was answered
`Applied` too. One that started from Installing would have quarantined every copy of a restored table
and installed the backup's file over it again, every write since the restore included. A drive of a
try the operation retried since was answered `Applied` and went on beside the new try's.

## Evidence

**Established by reading the source, then reproduced.** The reproduction is a row written for it,
`a_restore_rides_out_a_drive_behind_its_record`. One node's maps are held once they carry the restore
(`HOLD_MAPS_PAST_RESTORE`, `faults::hold_maps_past_restore`), as a node catching up on the control log
holds them, and the group's leader and third member are held from standing (`ELECT off`), so the held
node alone is elected when the install restarts the group. On the tree before the fix its first run
failed:

```text
group 12059251854110449496 came to {"Failed":{"reason":"8cfd45f2-6278-423b-9d18-f28156a92dbb/0 holds 12 rows of the group's tablets; a restore is into an empty table, and this one is not"}} when node 2 drove it from a map behind its record
… "attempts":0,"driver":"8cfd45f2-…","failed_in":"Loading","generation":0,"installed":null …
```

That record has the same shape as the workspace run's. The child logs tell its order:

```text
09:15:53.974 built a group's restore file group=a75b11ab2d395f58 boundary=3 records=12
09:15:58.297 a group's restore is left for its new leader … the leader is 8cfd45f2-…/0
09:15:58.421 driving a group's restore group=a75b11ab2d395f58 phase=Pending
09:15:58.668 a group's restore failed … holds 12 rows of the group's tablets
```

With the fix the same row passes. The held node's drive starts at Pending and is answered with the
record, and its loop drives the group from Installing. The group is restored at a fresh boundary:

```text
02:30:27.222 driving a group's restore group=722166e5babb98ae phase=Pending
02:30:27.279 a group's restore drive started behind its record; it is driven again from the record … it stands at Installing under term 2
02:30:27.279 driving a group's restore group=722166e5babb98ae phase=Installing
02:30:29.029 restored a group group=722166e5babb98ae boundary=5 records=12 verified=7
```

A deposed drive's late commits, and a drive behind a done group, are established by reading alone.
The output is in `target/lab/i336/`, and the child logs in `target/child-logs/i336-before` and
`i336-after`.

## The fix

**Wire 17 fences a restore's drives one from another** (`RESTORE_FENCE_WIRE_VERSION`). No frame moved,
so a cluster on 16 rolls onto it.

- **Every progress carries the term of the group's lead its drive drives under**
  (`GroupRestore::term`). The term is read when the drive starts, and again after the install's
  restart elects the drive's shard.
- **Once 17 is activated, `apply_restore_progress` keeps one drive at a time:**
  - A drive under an older term than the record's, or of a try the operation retried since, is
    refused `Superseded`. It stops where it is and commits nothing more.
  - **A progress never moves a group's phase back.** A drive behind the record takes it over, its
    term and its driver, and is answered `ControlResponse::RestoreAhead` with the record. The drive
    ends at once. Its loop drives the group again from the record's phase, with the record's
    attempts and installed file (`RestoreDone::installed`, `DrivenRestore::installed`), so no drive
    repeats a phase the record passed.
  - A drive behind a group that is done is answered with the record too, and goes no further.
- **Below 17 every replica applies a progress as a build before the fence does**, the term set
  aside, so a mixed cluster's records and topology versions stay alike.
- A drive whose failure commit is refused or answered so logs that its failure was not the group's.
- **A shard whose drive was taken over drives the group again only under a newer lead**
  (`DrivenRestore::taken_over`, `taken_over_under`). A deposed shard that still believes it leads
  would otherwise start a drive at every tick of its loop, 50 ms at the least, and each would be a
  refused entry in the control log.
- **The fixture's restoring clusters activate the newest wire, as shoaladm's bootstrap does**, so
  their restores are fenced. The exception is `a_restore_with_failed_groups_is_finished_by_a_retry`,
  which checks the refusal of a retry below wire 6 first and activates after it.

## Alternatives rejected

- **The driver alone: start from the control thread's record, and stop once the shard no longer
  leads.** No wire change, but it narrows both windows and closes neither. The control thread's
  replica can be behind the leader too, and a deposed drive that has not heard of its successor
  still commits late.
- **Forward-only phases alone.** That closes the half reproduced. A deposed drive meeting a newer
  drive's install (a member's checkpoint past its boundary) would still fail the group, which is the
  overlap the fence is for.
- **Refuse a drive behind the record without the record.** The drive could only wait for its map,
  and a node catching up on the control log can be behind for seconds. Answered with the record, it
  goes on at once.
- **Let Loading's check accept a copy quarantined under this restore.** That mends a retry after
  this failure, but only the rows. A deposed drive's late word would still reach the record.
- **A fence not gated by the wire.** Replicas built before it would take a progress the others
  refuse, and their records and topology versions would part.

## Invariants to uphold

- **A group's record is the authority on where its restore stands.** Once fenced, no progress moves
  its phase back, and a drive behind it learns where the group stands from the answer to its commit,
  never from its map.
- **Every progress carries the term of the lead its drive drives under**, read again whenever the
  drive's shard is elected again. A group's term only grows: its raft vote is kept across the
  install's restart, the first drive's term 1 and its successor's 2 in the row.
- **A drive refused `Superseded` commits nothing more**, and its shard drives the group again only
  once it leads it under a newer term.
- **Below `RESTORE_FENCE_WIRE_VERSION` a progress is applied as a build before it applies it**, the
  term set aside before the record is compared.
- **A drive ended by the record hands its loop the record's phase, attempts and installed file**, so
  the next drive neither repeats a phase passed nor reports a file it never knew
  ([Resolved #327](restore-figures-kept.md)).

## Still open

- **A group this failed before its cluster activated 17 stays failed.** Its retry loads again and
  finds the same rows, as any group failed in Loading does
  ([Resolved #155's remainder](restore-retry.md#still-open)). Letting a retry's Loading accept copies
  quarantined under the operation would mend it; nothing has met it since.
- **Members are not fenced by the term.** A deposed drive's quarantines and installs in flight still
  reach them. They converge because the newer drive installs again at a later boundary, which every
  member judges an install against, and the deposed drive's next commit is refused.
- [Item 296](../known-issues.md#296-a-restores-table-group-stays-at-installing-under-load-and-its-bucket-archives-wait-behind-it),
  a group left at Installing with no driver, is another defect and still open. The fence changes what
  a drive commits, not which shard drives.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_restore_rides_out_a_drive_behind_its_record` (`cluster_fixture`) | The group's next leader, its maps held behind the record, drives from Pending, commits Loading over Installing and fails the group on its install's 12 rows |
| `a_restores_drives_are_fenced_once_its_wire_is_activated` (`shoal-core`, `control::types`) | A drive behind the record moves it back, an older lead's failure is taken, a drive behind a done group is answered `Applied`, and a word of a try retried since changes nothing without saying so |
| `a_taken_over_drive_waits_for_a_newer_lead` (`shoal-core`, `shard::restore`) | A shard whose drive was taken over under term 3 drives the group again under term 3, or under the term 2 it still believes in |
| `a_restore_rides_out_an_unreachable_member` and the other restore rows (`cluster_fixture`) | Under load a group fails as the workspace run's did; their restoring clusters speak the newest wire since |

## Related

- [Resolved #297](restore-rides-out-a-moment.md), whose hand-back carries a drive's attempts to its
  loop as this carries its file.
- [Resolved #327](restore-figures-kept.md), the installed file's figures in the record.
- [Resolved #155](restore-retries-unreachable.md) and [#183](restore-driver-uncommitted-done.md), the
  drive's other ways of ending.
- [Item 296](../known-issues.md#296-a-restores-table-group-stays-at-installing-under-load-and-its-bucket-archives-wait-behind-it).
- [F48](../../features/rolling-compatibility.md), wire 17's place among the versions, and
  [F49](../../features/backup-and-recovery.md).
- [Tests That Fail Under Load](../flaky-tests.md#left-the-page).
