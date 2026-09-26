# 155, the remainder. A restore whose group failed could not be finished

The first half of item 155 is [its own page](restore-retries-unreachable.md): a group whose
driver could not reach a member is now driven again. This page is the rest, a group that failed
for any other reason.

## Symptom

On the lab ([Back up, destroy and restore](../../cluster-testing/correctness.md#back-up-destroy-and-restore)),
one group of a restore failed, and the cluster was left with that group's tablets empty. Nothing
could fill them. `Restore` is refused on *"a cluster that already restored"*, and the failed
group's copies may no longer be empty, which is the other thing a restore requires. The only way
on was to delete the cluster, bootstrap another, and restore everything again. On the lab that was
three minutes. For a large backup it would be hours, with every other group's verified rows
thrown away and restored again.

## Cause

A restore is once per cluster by design (`apply_restore` refuses when `restored_from` is set), so
that a cluster never holds two histories under one identity. Within one operation a group's
record could only move forward: `apply_restore_progress` ignores any progress for a group that is
`Done`, and a failed group is `Done` with `RestoreOutcome::Failed`. The driver dropped the one
fact a retry would need. Its failure commit wrote `phase: Done`, and the phase the group had
reached (`RestoreContext::reached`) went nowhere. So nothing could say where to start again, and
nothing was allowed to.

## Evidence

**Established by reading the source**, with the lab run as the symptom. On the unfixed tree the
refusals are `apply_restore`'s `restored_from` check and `apply_restore_progress`'s `is_done`
early return, and no command moves a `Done` group. The test below reproduces the state the lab was
left in: a finished restore with some groups `Restored` and some `Failed`. It could not be run
against the unfixed tree, since the step it then takes did not exist.

## The fix

- **A group's record keeps the phase it failed in** (`GroupRestore::failed_in`) and a
  `generation`. The driver's failure commit records `reached`, the phase it committed last.
- **`ControlCommand::RetryRestore { op, principal, expected_version, restore }`** puts every
  group of a finished restore that failed back to be driven (`GroupRestore::retry`). A group
  that failed loading goes back to `Loading`, which checks its copies are still empty. One that
  failed installing or verifying goes back to `Installing`: a new file at a new boundary on every
  member replaces whatever a partial install left, and is then verified. The driver, the outcome
  and the attempts are cleared, and the generation is incremented. A restored or skipped group is
  not touched. The operation and the files are the same ones, so nothing about what is restored is
  judged again.
- **It is refused** below wire version 6 (`RESTORE_RETRY_FROM_WIRE`), for an unknown operation,
  while any of the restore's groups is still running, and when none failed.
- **A driver of an earlier try is fenced.** A progress whose generation is not the group's is
  applied as nothing, and the shard's cache of what it last drove (`driven_restores`) is kept per
  generation. So a stale `Done` from the first try neither ends the retry nor stops the shard
  from driving it.
- **The wire version went to 6** ([F48](../../features/rolling-compatibility.md)). No frame's
  encoding moved. But the control log gained a command, and a record two fields, which a replica
  built before them would refuse to decode or drop.
- **The operator's side** is `AdminKind::RetryRestore { restore }` and shoalctl's
  `cluster admin "restore-retry <op>"`, which follows the restore's own record, since the retry
  has none, and fails the command if a group fails again (#154).

**Found on the way, by the suite.** `mixed_versions_exchange_real_cluster_operations` failed once
in a six-thread run of the fixture: its `ACTIVATE 6`, which has to be refused naming the two
members pinned at 4, was refused `StaleVersion` instead, because a member's report had moved the
topology version between the read and the proposal. Alone it passed three runs of three. The
version check is right to refuse. The test now asks again on `StaleVersion`, which says nothing
about the members, up to twenty times.

## Alternatives rejected

- **Let `Restore` run again on a cluster that restored.** The once-per-cluster rule is what keeps
  two histories from one identity. A second restore of a different backup, or the same one after
  writes, would break it. A retry names its restore and can only repeat that restore's files.
- **Retry automatically, like an unreachable member.** The failures left are not ones that go
  away on their own: an unreadable file, a full disk, copies that are not empty. A retry is an
  operator's statement that the cause was dealt with, and the operator should make it.
- **Restart a failed group from `Pending`.** The loading phase refuses copies that hold rows, and
  a group that failed after its install began may hold some. Starting from the phase it failed
  in is what makes the retry possible for that group at all.
- **Keep the shard's driven cache as it was and clear it on a retry.** The cache lives on every
  shard that ever led the group, and a retry is one committed command. Keying the cache by
  generation needs no message to reach any of them.
- **No wire version.** The command is decoded by every control replica. A build before it refuses
  to decode the entry, and a build after it that drops the fields drives the wrong phase.

## Invariants to uphold

- **A retry never touches a group that did not fail.** `retry` is applied only to groups for which
  `failed()` holds, and a `Restored` group's record is left equal to what it was.
- **A progress counts only for its own generation.** Every commit a driver makes carries the
  generation its context was built with. A driver that outlives a retry is answered `Applied`
  and changes nothing.
- **The phase a group is retried from is one it can be run again from.** Installing and Verifying
  go back to Installing, which quarantines every copy under the same operation and installs a
  new file. Loading is Loading, which needs the copies empty.
- **A restore is still once per cluster.** `restored_from` is not cleared, and the retry reads the
  record's files, never a directory.

## Still open

- Nothing retries a group automatically; the operator does. That is deliberate, above.
- A group that fails loading because its copies hold rows fails again on a retry, until they are
  empty. Emptying one group's copies has no command of its own.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_restore_with_failed_groups_is_finished_by_a_retry` (`shoal/tests/cluster_fixture.rs`) | A restore with failed groups cannot be finished, a retry below 6 is not refused, a restored group is driven again, or a retry with nothing failed is accepted |
| `a_restore_retry_drives_only_its_failed_groups` (`shoal-core`, `control/types.rs`) | The refusals, the phase a failed group goes back to, the untouched restored group, or the fence against a first try's driver |
| `parse_takes_every_verb` (`shoalctl`, `cluster/actions.rs`) | `restore-retry` is not parsed, sent, or followed by the restore's record |

## Related

- [Resolved #155](restore-retries-unreachable.md), the first half.
- [Resolved #154](admin-hides-failed-groups.md), which made a failed group fail the command.
- [F49](../../features/backup-and-recovery.md), backup and restore.
- [F48](../../features/rolling-compatibility.md), the wire version rules.
