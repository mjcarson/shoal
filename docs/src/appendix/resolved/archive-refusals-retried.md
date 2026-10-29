# 322. A group's archive failed for good at its first refusal for now

A backup of a bucket writes one archive for each group of the bucket's `ObjectMeta` table, on the
group's leader ([F93](../../features/operations-and-the-real-cluster.md)). The leader walks the
tablets it leads and reads every live object strongly: its entry and its stripe rows at `Quorum`,
and its bytes at the labels those rows name.

A strong read is refused while its group cannot confirm its leader in time (`QuorumUnavailable`),
while a member is away, or while a shard sheds. A client tries such a refusal again under the same
identity. The archive's driver did not: every error but the loss of its lead committed the group
`Failed`, and the backup had to be taken again. A restore's driver rides out the same kind of
refusal ([Resolved #155](restore-retries-unreachable.md)).

This page is the first half of item 322. The other half ~~stays open on Known Issues~~ is fixed on
[its own page](backup-outcome-record.md): a table or a bucket every group of which failed left no
file for a restore to judge, and now the backup's record of every group's outcome says so.

## Symptom

In the loop that traced item 315, under a load far past a workspace run's, two of a bucket's three
archives failed within five seconds of the backup's start:

```text
ERROR … msg="a group's archive failed" op=de8a9005-… group=d7b0b23460fd36a4 error="QuorumUnavailable: group d7b0b23460fd36a4 could not confirm its leader: not enough for a quorum, …"
```

The backup's record said `Failed` for each. A restore of that backup finished without their objects
until [Resolved #315](archive-coverage.md) made it refuse the gap. In another round every archive of
the bucket failed.

## Cause

`drive_group_archive` (`shard/backup.rs`) had two endings for an error:

```rust
Err(error) if error.starts_with(NOT_LEADER) => { /* left Pending for the next leader */ }
Err(error) => { /* committed Failed with the reason */ }
```

and the archive's reads reported their refusals as `"{code}: {why}"`, which nothing told apart.

## Evidence

**Established by running it and reproduced by a test.** The loop's logs are above.

`a_backup_rides_out_a_refused_archive_read` (`cluster_fixture`) was written before the fix, with a
fault hook it adds: `faults::refuse_archive_reads` refuses a node's next archive reads as a group
that cannot confirm its leader does, armed through the fixture's `REFUSE_ARCHIVE_READS <n>`. Every
node refuses its next two, and a bucket of twelve objects is backed up. On the tree before the fix,
the hook in place:

```text
thread 'a_backup_rides_out_a_refused_archive_read' panicked at shoal/tests/cluster_fixture.rs:18276:9:
backup b38317f5-…'s group 16013556788407048674 came to {"Failed":{"reason":"QuorumUnavailable: group of refused-7.bin could not confirm its leader: a test refused the archive's read"}}: …
```

## The fix

**An archive that meets a refusal for now is driven again after a pause, up to twelve tries.**
- **A refusal for now is marked as one** (`archive::refusal`). A read refused with a code a client
  tries again under the same identity gets the prefix `RETRIABLE`: a leader not one, a member away,
  a quorum not there, a lost connection, an unknown outcome, a deadline, a shed, and a route by an
  older map. So does the walk's refusal of the same codes, and an object that kept moving past the
  archive's own tries. Any other code fails the group as before.
- **The driver leaves the group writing** and tells its loop to try again (`BackupDone::retry`).
  The loop counts the try (`backup_retries`) and holds the group for `ARCHIVE_RETRY_PAUSE`, five
  seconds. Its next tick past the pause drives the archive again from the start. The archive is
  written under a temporary name, so a try again overwrites what the last one left.
- **Past `ARCHIVE_ATTEMPTS`, twelve, the group fails** as before, with the last refusal's reason.
- **A written archive says how many tries it took** ~~(`BackupOutcome::Written::retries`)~~ in
  `BackupOutcome::Written::attempts` since [the rest of item 322](backup-outcome-record.md): this
  half put an archive's tries in `retries`, which for a table's file is the remembered requests its
  trailer holds, so one field meant two things.

The test's backup finishes with every group written and its retries counted, and the backup
restores every object.

## Alternatives rejected

- **Trying the read again inside the archive.** The archive already reads an object that moved
  again, up to a bound. A group that cannot confirm its leader for seconds needs a pause measured in
  seconds, and a try of the whole archive checks again that its node still leads the group.
- **Leaving the group `Pending`**, as a lost lead does. Its next leader drives it at once, and the
  leader that met the refusal is that same leader: the try would follow without a pause, as often as
  the loop ticks.
- **Retrying every error.** A corrupt archive record or an object behind its chunks fails the same
  way every time, and twelve tries of it only delay the failure the backup reports.

## Invariants to uphold

- **A group's archive is tried again only on a refusal a later try may not meet**, marked by
  `RETRIABLE` where the read was refused, and never more than `ARCHIVE_ATTEMPTS` times.
- **A try again starts the archive over**, under its temporary name, never appended to a try before.
- **A held group is held by the loop's own count**, forgotten with its record.

## Still open

- ~~**A table or a bucket every group of which failed leaves no file**, and is restored empty, as a
  table the backup never held is.~~ Fixed by [the rest of item 322](backup-outcome-record.md): every
  backup writes a record of every group's outcome beside its files, and a restore refuses a backup
  with a failed group by name.
- ~~**A table group's backup is not tried again.** It reads nothing but its own copy, and nothing has
  been seen to fail it under load; its commits to the control plane wait up to thirty seconds each.~~
  Tried again since [the rest](backup-outcome-record.md), on a nudge, a cut, a copy or a commit that
  did not finish in time.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_backup_rides_out_a_refused_archive_read` (`cluster_fixture`) | A group's archive met by a refusal for now fails for good, and its backup is not written whole |
| `a_refusal_for_now_is_marked_so` (`shoal-core`, `shard::objects::archive`) | A read refused with a code a later try may not meet is not marked, or one a try would meet again is |
| `objects_survive_a_backup_and_restore_by_archive`, `a_restore_never_puts_over_a_clients_write` and the fixture's other backup rows | Unchanged: a backup with nothing refused is written in one try |

## Related

- [Resolved #315](archive-coverage.md): a restore refuses a bucket whose archives leave a tablet
  uncovered, which is what a group whose archive failed left.
- [Resolved #155](restore-retries-unreachable.md): a restore's group driven again for want of a
  member.
- [F93](../../features/operations-and-the-real-cluster.md): a bucket backed up as its objects.
