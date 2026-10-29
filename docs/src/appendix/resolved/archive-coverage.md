# 315. A restore finished without the objects of a bucket's group whose archive had failed

A backup copies a bucket as its objects, one archive for each group of the bucket's `ObjectMeta`
table, `<path>/<op>/<Bucket>/<group>.objects`, with a manifest beside it
([F93](../../features/operations-and-the-real-cluster.md)). A restore reads the manifests it finds
under the backup's directory and puts every object of every archive again.

A group whose archive fails writes no manifest. Its group's backup ends `Failed`, and the backup's
record says so, but nothing beside the files does. A restore of that directory found the other
archives, put their objects again, and finished with every archive it knew of done and none failed.
The failed group's objects were in no archive, and the restore never said so.

A table's backup files have been judged since [F49](../../features/backup-and-recovery.md): each
manifest names the tablets its file covers, and a restore refuses a backup in which a tablet of a
table is in no file. A bucket's archives named no tablets and were judged for nothing.

Found as item 315, two fixture rows that read a restored object as not found, and traced to the
backup by looping the rows under load with their nodes' logs kept.

## Symptom

`a_restore_never_puts_over_a_clients_write`, in F113's workspace run (2026-10-26). Its restore
finished with every archive `Done` and none failed, and then a strong get of an object only the
backup held:

```text
restored-inline.bin: Object { … outcome: NotFound, msg: "no object is at restored-inline.bin" }
```

`a_restore_judges_each_table_by_its_own_identity` met the same in window three's run: every group
`Restored` or `Skipped`, every document read back, and `kept-0.bin` not found. Both passed when run
again.

## Cause

The backup lost the objects, and the restore did not notice.
- **The backup.** Under the run's load, two of the bucket's three groups failed their archives. A
  strong read inside each archive could not confirm its group's leader in time (`QuorumUnavailable`),
  and a group's backup ends `Failed` on any error but a lost lead, so neither was tried again. That
  failure is [item 322](../known-issues.md).
- **The fixture's wait.** `wait_operation_done` takes a group as done once its phase is `Done`,
  which a failed group's is, so the rows restored a backup that had failed.
- **The restore.** `scan_backup` (`control/backup.rs`) reads every manifest it finds, and judged the
  table files' coverage alone:

```rust
// every tablet of every restored table in exactly one file
let coverage = match judge_coverage(files, &self.tables, TABLET_COUNT as u16) { … };
```

The archives were checked for their bucket, its binding, their format and their rows' layout, never
for which groups they stood for.

## Evidence

**Established by reproducing it, twice.**

**Under load.** `a_restore_never_puts_over_a_clients_write` and `a_restore_judges_each_table_by_its_own_identity`
were run in a loop beside four of the heaviest fixture rows and the object and device store suites,
each child logging at DEBUG (`SHOAL_CHILD_LOG`), a load far past a workspace run's. The first row
failed in all eight rounds. It now reads a missing object again for ten seconds, so its failure says
the object was gone, not read as missing for a while:

```text
Error: NotReady("restored-inline.bin: Object { … outcome: NotFound, msg: "no object is at restored-inline.bin" }, missing in every read for 10.144466398s: […]; the restore: {"archives":{"Reels/2814744679c24238.objects":{"bytes":0,"failed":null,"file":{…"records":0…},"phase":"Done","put":0}},…}
```

The restore knew one archive, of no objects. The source nodes' logs held the rest:

```text
16:44:54.251 ERROR … msg="a group's archive failed" op=de8a9005-… group=d7b0b23460fd36a4 error="QuorumUnavailable: group d7b0b23460fd36a4 could not confirm its leader: not enough for a quorum, …"
16:44:55.720 INFO  … msg="wrote a group's archive of objects" op=de8a9005-… group=2814744679c24238 objects=0 bytes=0 …
16:44:58.523 ERROR … msg="a group's archive failed" op=de8a9005-… group=ca73d431cd3d9f4c error="QuorumUnavailable: group 40d59c49c2b67ea9 could not confirm its leader: not enough for a quorum, …"
```

The same round's backup of `objects_survive_a_backup_and_restore_by_archive` archived two of its six
objects. The second row failed earlier in most rounds, its cluster's leases lapsing under the load.
In one round its restore's record named no archive of the bucket at all: every group of the bucket
had failed its archive, so there was nothing to judge, and the restore put back the documents alone.

**Unloaded, by a test written before the fix.** `a_restore_refuses_a_bucket_missing_an_archive` backs
up twelve objects, takes away one archive that holds some and its manifest, as a failed group leaves
the backup, and restores. On the tree before the fix:

```text
thread 'a_restore_refuses_a_bucket_missing_an_archive' panicked at shoal/tests/cluster_fixture.rs:32284:13:
a restore of a backup missing 245abdd72b4d350b.objects, which held 2 objects, was accepted and finished, and 2 of 12 objects read as not found: ["spread-2.bin", "spread-4.bin"]; the restore: {"archives":{"Reels/489728b54606b9cc.objects":{…"failed":null,…"phase":"Done","put":4},"Reels/d6038ca8c372b009.objects":{…"failed":null,…"phase":"Done","put":6}},…}
```

## The fix

**A bucket's archives are judged as a table's files are.**
- **Each archive's manifest names the tablets its group served** (`ObjectsManifest::tablets`, from
  the tablets the driver walked).
- **`scan_backup` judges every bucket's archives** before the restore is proposed
  (`judge_archive_coverage`). Every tablet must be in exactly one archive. A gap is refused by name,
  "tablet 1 of bucket Reels is in no archive of the backup: the archive of the group that served it
  failed or is missing", and so is a tablet in two archives.
- **An archive written before archives named their tablets** cannot be judged. A bucket all of
  whose archives are of that kind restores as it did before, with a warning at the scan. One that
  mixes them is refused, since it cannot be shown whole.
- **The judgment is made where the backup is scanned, on the node that takes the restore**, so a
  refusal never reaches the control log, and members of different builds cannot apply a restore
  differently.

**The fixture's restore rows require a backup that wrote every group.** `wait_backup_written` waits
for the backup and asserts every group `Written` or `Skipped`, naming a failed one with its reason.
A backup that fails under load now fails its row at the backup, and says why.

Diagnostics added on the way:
- **A restore's put logs each try at DEBUG**: a record found done by an earlier try, a put made,
  and a try made again, each with the object it names.
- **The two rows that met the item read a missing object again for ten seconds**, and say whether
  it came back.

## Alternatives rejected

- **Judging the coverage in the state machine**, beside the tables' judgment. A refusal added to an
  apply makes members of an older build accept what newer ones refuse. A restore is judged once, at
  the scan, from the files themselves.
- **Refusing every archive that names no tablets.** Every bucket backup taken before the fix, the
  lab's among them, would become unrestorable. They restore as they always did, and the warning says
  they cannot be judged.
- **A record of the backup's outcome written beside its files**, read by the restore. It would also
  catch a table or a bucket every group of which failed, which leaves no file to judge, but the
  backup's record lives in the control state of the cluster backed up, and the directory is shipped
  between hosts by hand. Filed with item 322.
- **Making a backup retry a group's transient failure.** It is a change to what a backup does under
  load, not to what a restore accepts, and is item 322.

## Invariants to uphold

- **Every restore judges every bucket's archives whole before it is proposed**: each tablet of the
  bucket's `ObjectMeta` table in exactly one archive.
- **An archive's manifest names the tablets its group served**, and a new kind of archive does too.
- **A restore never finishes having skipped a group it knew of.** It refuses instead.
- **A fixture row that restores a backup checks that the backup wrote every group.**

## Still open

- **A group's backup or archive fails for good on a transient refusal**, so a backup taken under
  load can fail ([item 322](../known-issues.md)). The restore now refuses its gap, and the backup has
  to be taken again.
- **A bucket or a table every group of which failed leaves no file to judge.** It restores as empty,
  as a table the backup never held does ([item 322](../known-issues.md)).
- **A bucket backup written before this fix is not judged.**

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_restore_refuses_a_bucket_missing_an_archive` (`cluster_fixture`) | A restore of a backup missing one archive of a bucket is accepted and finished, and that archive's objects read as not found |
| `a_buckets_archives_are_judged_whole` (`shoal-core`, `control::backup`) | A bucket's archives with a gap or an overlap are accepted, or a mix of named and unnamed ones |
| `a_restore_never_puts_over_a_clients_write`, `a_restore_judges_each_table_by_its_own_identity`, `objects_survive_a_backup_and_restore_by_archive`, and the other rows that restore a backup (`cluster_fixture`) | Unchanged in what they hold; each now fails at a backup that failed, with its reason, rather than at a restored object found missing |

## Related

- [Item 322](../known-issues.md): a group's backup failing for good under load, which is how the
  lab's backups lost their archives.
- [Item 296](../known-issues.md#296-a-restores-table-group-stays-at-installing-under-load-and-its-bucket-archives-wait-behind-it):
  the same rows' other failure under load, a restore never finished.
- [Resolved #261](archive-driven-once.md): a restore's put never goes over a client's write.
- [F93](../../features/operations-and-the-real-cluster.md): a bucket backed up as its objects.
- [F49](../../features/backup-and-recovery.md): a table's files and their coverage.
