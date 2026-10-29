# 322. A table or a bucket every group of which failed was restored empty

A backup writes one file for each group of its tables, on the node that leads the group
([F49](../../features/backup-and-recovery.md)), and one archive of objects for each group of a
bucket's `ObjectMeta` table ([F93](../../features/operations-and-the-real-cluster.md)). An operator
gathers every node's files into one directory, which `shoaladm ship-backup` does
([F59](../../features/backup-shipping.md)), and a restore of a fresh cluster reads that directory.

This page is the rest of item 322. Its first half, an archive driven again after a refusal for now,
is [Resolved #322, in part](archive-refusals-retried.md).

## Symptom

A backup restored less than it held, or failed where a later try would have written it:
- **A table every group of which failed came back empty.** A failed group writes no file. With
  every group of a table failed, no file named the table. The restore restored the tables and
  buckets it found files of, and left that table empty, as one the backup never held is.
- **A backup gathered without a table's files came back the same.** A table whose files were all
  left out of the gathering restored empty in the same way, with nothing said.
- **A table group's backup failed for good at its first refusal for now.** The first half retried a
  bucket's archives. A table group whose nudge, cut, copy or commit did not finish in time still
  failed, and the backup had to be taken again.
- **`BackupOutcome::Written::retries` meant two things.** For a table it is the remembered requests
  its file's trailer holds. Since the first half, an archive put its tries in it.

## Cause

A restore judged only the files it found. `scan_backup` (`control/backup.rs`) read every manifest
under the directory, and `judge_coverage` judged every table some file named:

```rust
for file in files {
    let covered = coverage.entry(file.table).or_default();
    ...
}
// every tablet of every covered table, in exactly one file
for (table, covered) in &coverage { ... }
```

A table no file named was never covered, so never judged. Nothing beside the files said which
groups the backup had: its record of them lived only in the control state of the cluster that took
it. A group's identity is a hash of its table and its replica set, so even a list of groups could
not have said which table a group without a file belonged to.

A table group's driver, `drive_group_backup` (`shard/backup.rs`), committed `Failed` on every error
but a lost lead, where the archive's driver had learned to try a refusal for now again.

## Evidence

**Established by reading the source and reproduced by four tests**, written before the fix with a
fault hook they add: `faults::refuse_backup_cuts` refuses a node's next cuts of a table group, for
now or for good, armed through the fixture's `REFUSE_BACKUP_CUTS <n>` and `FAIL_BACKUP_CUTS <n>`.
Each runs three nodes serving `ObjectDb`, a document table and the bucket `Reels`. On the tree
before the fix:

```text
thread 'a_restore_refuses_a_table_every_group_of_which_failed' (903221) panicked at shoal/tests/cluster_fixture.rs:32473:13:
a restore of a backup whose every Doc group failed was accepted and finished, and 0 of 16 documents read back; the restore: {"archives":{"Reels/239eae1e3dfe7bae.objects": …
thread 'a_restore_refuses_a_backup_not_gathered_whole' (903219) panicked at shoal/tests/cluster_fixture.rs:32520:13:
a restore of a backup without Doc's 6 files was accepted and finished, and 0 of 16 documents read back; the restore: {"archives":{"Reels/521cc64c75bf84bb.objects": …
thread 'a_restore_refuses_a_backup_without_its_record' (903220) panicked at shoal/tests/cluster_fixture.rs:32564:13:
a restore of a backup without its record was accepted: {"archives":{"Reels/9062d3b88272084e.objects": …
thread 'a_table_groups_backup_rides_out_a_refusal_for_now' (903222) panicked at shoal/tests/cluster_fixture.rs:18287:9:
backup 42d1c485-7a3e-41f8-83ea-224ed4045031's group 12616194234269232139 came to {"Failed":{"reason":"refused for now: the group's cut was refused by a test"}}: …
```

The third had nothing to refuse it by before the fix: no backup wrote a record.

## The fix

### A record of every group beside the files

Once a backup ends, `<path>/<op>/backup.json` (`BackupRecordFile`) records:
- its kind, `backup`, and its format, 1;
- the operation, the cluster, the table it was asked for or none, and who asked;
- every group, with its table and the tablets it served, the bucket it archives if any, the node
  that drove it, and what it came to.

**Each group's table and tablets are kept when the backup is asked for** (`BackupRecord.placed`,
committed, `#[serde(default)]` in the control state's JSON), since a group's identity cannot name
them.

**Every node writes the record from its committed state** (`ControlPlane::record_backups`):
- It runs at each applied command and reads the state in place, so a command that ends no backup
  costs a pass over at most 64 records.
- Every node writes the same bytes, so wherever the files are gathered from, the record is among
  them, and `ship-backup`'s `--skip-old-files` keeps any one copy.
- It is staged under the writing node's own name, synced, and renamed over `backup.json`, then the
  directory is synced. Nodes sharing a directory, as the fixture's children do, never write one
  staged file between them.
- After a restart, a backup that ended before it is written only into a directory this node holds,
  and only where its record is missing. A backup an operator removed is not made again.
- A write that fails is tried again on a command applied 30 s or more later, and warned of once.

An export writes a record too, every group `Written`, so `RESTORE` of an export reads one.

### A restore reads it first

`scan_backup` reads the record before any manifest and judges the files by it (`judge_record`),
before anything is proposed. It refuses, by name:
- **a backup without a record.** The user's choice (2026-10-27): every backup must carry its record.
  One taken before this change is restored with a build before it, and one gathered without its
  record needs it copied beside the files;
- **a failed group**, naming its table or bucket, how many of its groups failed, and the first
  reason;
- **a written group whose file is not there**: "not gathered whole", naming the file and the node
  that wrote it;
- **a file the record does not name**, a file of another operation or cluster, and two files of
  one group.

It judges at the scan, where item 315's judge of a bucket's archives runs, so a member of an older
build is never given a restore to accept that a newer one would refuse.

### A table group tried again

A table group's driver now meets a refusal for now as an archive's does: it is driven again after
5 s, up to twelve tries (`BACKUP_ATTEMPTS`, `BACKUP_RETRY_PAUSE`, renamed from the archive's
constants). These are marked `RETRIABLE`:
- a nudge not committed in time;
- a checkpoint not past its index in time;
- a cut that did not land in time;
- a copy that did not finish in time;
- a progress the control plane did not commit within 30 s, or a control thread not taking
  proposals.

An I/O error and a refused progress still fail the group.

A table group's file is named by its boundary, so a try that wrote its file and failed after would
leave a second file of its group. `write_backup` removes an earlier try's files of its group before
it writes its own, and a restore refuses two files of one group.

### Tries counted apart

`BackupOutcome::Written` gains `attempts`, the tries a group's backup took, this one among them,
which a build before this records as nothing (0). `retries` is the remembered requests a file's
trailer holds again, and none for an archive.

### `ship-backup`

`shoaladm ship-backup` refuses a backup that no host holds the record of, and says to wait until
every group is done.

## Alternatives rejected

- **The control leader writing the record and committing that it had.** That is a new control
  command, which a replica built before it cannot apply, so a wire version and an activation. It
  would also put the record on one node, where a copy on every node costs nothing.
- **Writing the record from the shards.** A shard never sees a finished backup: the map carries
  only backups not done.
- **Judging an older backup by its files, with a mark in new manifests that a record exists.**
  Offered. The user chose to refuse every backup without a record (2026-10-27).
- **Restoring what a backup holds, with a failed group's table left empty and said so.** A restore
  that finishes is read as whole. A partial restore is an operator's to ask for, and nothing asks
  for one yet.
- **Judging at apply.** A member of an older build would accept a restore a newer one refuses,
  which is item 315's reason for judging at the scan.
- **Naming every tablet a failed group served.** A group of a three-node cluster serves well over a
  thousand, so the refusal counts groups and gives the first reason.

## Invariants to uphold

- **Every backup that ends has its record beside its files on every node that applied its end**,
  the same bytes on each, built from committed state alone (`BackupRecordFile::of`), never from
  what a node saw.
- **A restore reads the record before any file and refuses a backup without one.**
- **The files and the record agree exactly**: every written group's file there, no group failed, no
  file the record does not name, and one file a group.
- **A group's placement is kept when the backup is asked for** (`placed`), since its identity cannot
  name its table.
- **A table group's try leaves no file of its group beside its own.**
- **`retries` is a file's remembered requests and `attempts` its tries.**

## Still open

- ~~**A repair or a backup can be forgotten unfinished** once 64 newer ones are kept, and what queued
  behind a forgotten repair is never released.~~ Fixed as [Resolved #324](unfinished-records-kept.md).
- **A backup taken before this change is refused by this build**, as the user chose.
- **Nothing says on which nodes the record was written.** A backup reads as done a moment before its
  record is there, so `ship-backup` and the fixture wait for the file.
- **A table group failed by an I/O error is not tried again**, nor an archive failed by one.
- **There is no partial restore** of what a backup with a failed group holds.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_restore_refuses_a_table_every_group_of_which_failed` (`cluster_fixture`) | Every document group's cut refused for good: the restore is accepted and finishes, 0 of 16 documents back |
| `a_restore_refuses_a_backup_not_gathered_whole` (`cluster_fixture`) | The document table's files taken away: the restore is accepted and finishes, 0 of 16 documents back |
| `a_restore_refuses_a_backup_without_its_record` (`cluster_fixture`) | A backup without `backup.json` is accepted |
| `a_table_groups_backup_rides_out_a_refusal_for_now` (`cluster_fixture`) | Every node's next two table cuts refused for now: a group fails for good |
| `a_record_refuses_a_failed_group_a_missing_file_and_a_file_it_does_not_name` (`shoal-core`, `control::backup`) | The judge: a failed table, a missing archive and a stray file refused by name, a skipped group needing no file, files as recorded accepted |
| `a_record_is_made_once_its_backup_ends` (`shoal-core`, `control::backup`) | A record made while a group is not done, or one that does not name a group's table and tablets or does not read back |
| `a_backup_without_its_record_is_refused_by_name` (`shoal-core`, `control::backup`) | A scan of a directory with no record, a record of a later format, or a file of another kind under its name accepted |
| `a_restore_refuses_a_bucket_missing_an_archive` (`cluster_fixture`) | Changed: refused by the record first, as not gathered whole, before item 315's judge of the bucket's tablets |
| `rows_older_than_chunks_are_refused_by_name` (`cluster_fixture`) | Changed: its hand-made file of a bucket's rows has a group of its own and is in the record, so it is still refused by its table's judge |
| `a_backup_rides_out_a_refused_archive_read` (`cluster_fixture`) | Changed: counts the archives that took more than one try by `attempts` |
| The fixture's other backup and restore rows, and `single_node_data_has_a_verified_cluster_migration_path` | Unchanged: a backup and an export restored, now through their records |

## Related

- [Resolved #322, in part](archive-refusals-retried.md): the archive's retry this extends to a
  table group.
- [Resolved #315](archive-coverage.md): a bucket's archives judged whole at the scan, which the
  record now judges before.
- [Resolved #323](queued-backups-released.md): a backup queued behind a move or a repair, which
  never ended and so never had a record.
- [Resolved #324](unfinished-records-kept.md): a repair or a backup kept while it is not done.
- [F49](../../features/backup-and-recovery.md), [F93](../../features/operations-and-the-real-cluster.md)
  and [F59](../../features/backup-shipping.md).
