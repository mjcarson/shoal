# 343. A get failed on a partition an archive pass only moved under it

## Symptom

A get of an evicted partition, a write's read of one, or a direct read of one could fail
`ArchiveMissing`, and the client was told the partition could not be read, when an archive pass had
only moved it to another archive while the read was in flight. The loader gives up on
`ArchiveMissing` at once, so every query parked on the partition was released with the error. A
partition a pass pruned under a read failed the same way, where it should have answered as pruned,
with nothing.

## Cause

`ArchiveMap::read_partition` (`shoal-core/src/server/tables/storage/fs/map.rs`) looked the
partition's chain up (`chain_of`, which may read a page of the index from disk and awaits) and then
read the chain's records, opening their archive if the map did not hold it open. An archive pass
copies an archive's live records into the active archive, repoints the map, removes the archive and
unlinks its file (`FileSystemCompactor::compact_archives`). A read whose lookup came before the
repoint and whose open came after the unlink found the file gone and failed `ArchiveMissing`, which
`classify` takes as `Fatal` (`.../fs/loader.rs`), as it must for an archive the map still names
([Resolved #57](missing-archive.md)). The direct read (`FileSystem::load_partition_direct`, which a
table's digest and a start's recovery take) had the same shape. A walk's page did not: it looked a
partition whose archive was missing up again, up to three times (`ArchivedWalk::read`,
`shoal-core/src/server/tables/walk.rs`). The rule was kept by one reader and not by the other two.

## Evidence

Established by tests written first, found while [F120](../../features/one-operation-a-record-read.md)
was planned (2026-10-30), and reproduced and fixed once it was built. A read of partition 7 is held once it has found the
partition's chain (`faults::hold_resolved_reads`); the record is copied into a new active archive and
the map repointed over it, or the partition pruned; the old archive is removed and unlinked, as a pass
does; and the read is let go. Against the read path before the fix:

```text
thread 'server::tables::storage::fs::tests::a_read_follows_a_partition_a_pass_moved_under_it'
panicked at shoal-core/src/server/tables/storage/fs/tests.rs:911:14:
a partition a pass only moved could not be read: Shoal(ArchiveMissing { archive: aa75dad2-..., path: ".." })

thread 'server::tables::storage::fs::tests::a_read_of_a_partition_pruned_under_it_finds_nothing'
panicked at shoal-core/src/server/tables/storage/fs/tests.rs:938:9:
assertion `left == right` failed: Shoal(ArchiveMissing { archive: 12fb1ed1-..., path: ".." })
  left: Fatal
 right: Absent
```

The first runs passed for a reason of their own: the three rows held one table's reads, and one row's
release and count of waiting reads reached another's, whose read then looked its partition up after
the move. The rows take turns since. Not observed on the lab, where a pass runs at most once a minute,
over archives below half live, and a table loaded once leaves its archives alone.

## The fix

Every read of a partition by its key goes through one rule, `ArchiveMap::read_current` and
`read_current_from`, which the loader's read, the direct read and a walk's page all take. A read
whose archive is missing looks the partition up again: one the map no longer names is nothing to
read (`PartitionNotFound` for the loader, which answers as pruned); one the map has moved is read
where it is now, at most three moves (`MOVES_FOLLOWED`, the walk's bound); and one the map still
names where the read missed it is refused `ArchiveMissing` at once, since that archive is lost, not
moved. A walk whose partition moved past the bound fails by the read's own error rather than a
message of the walk's.

## Alternatives rejected

- **Class `ArchiveMissing` as `Retryable` in the loader.** Its retry looks the partition up again, so
  it would follow a move, but it would also try a lost archive three times with a wait between before
  it said so, which is why it is `Fatal`, and a direct read and a walk would gain nothing.
- **Hold an archive's unlink until no read holds a view of it.** The pass cannot see the views in
  flight, and a share a read holds ([F120](../../features/one-operation-a-record-read.md)) keeps open
  only a file the read opened.
- **Make the lookup and the open one step.** The lookup reads a page of the index from disk
  ([F76](../../features/paged-archive-map.md)), which awaits.
- **Look again on every failure.** A corrupt record or an I/O error is not a move, and is classed as
  it always was.

## Invariants to uphold

- **Every read of a partition by its key goes through `ArchiveMap::read_current`**, or
  `read_current_from` for a view a walk captured, and never opens a chain's archive with no look
  again.
- **An archive pass repoints every record of an archive before it removes the archive**, so an
  archive missing under a view is one the pass moved or pruned.
- **An archive missing under a chain the map still names is refused `ArchiveMissing`**, `Fatal`,
  never followed (Resolved #57).

## Still open

Nothing of this item. The cuts read through handles they hold rather than by key, so a removal never
fails them ([F120](../../features/one-operation-a-record-read.md)).

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_read_follows_a_partition_a_pass_moved_under_it` | `shoal-core/src/server/tables/storage/fs/tests.rs` | A read by key stops following its partition to where a pass moved it |
| `a_read_of_a_partition_pruned_under_it_finds_nothing` | same | A partition pruned under a read is refused rather than answered as pruned |
| `an_archive_missing_under_a_standing_entry_is_refused` | same | Looking again hides a lost archive, or follows it |
| `a_moved_record_is_read_where_it_moved` | same | A walk's page stops following a record whose archive was removed under it |

## Related

[F120](../../features/one-operation-a-record-read.md), whose design review found it;
[Resolved #57](missing-archive.md), which made a missing archive `Fatal`;
[Resolved #16, 51](partition-load-failure.md) for the loader's classes; [F87](../../features/tablet-walk.md)
for the walk; [Compaction](../../storage/compaction.md) for the archive pass.
