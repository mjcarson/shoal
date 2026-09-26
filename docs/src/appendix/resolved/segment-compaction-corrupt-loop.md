# 166. A segment compaction that had to merge onto a corrupt record was retried forever

## Symptom

With [#165](corrupt-record-compaction-loop.md)'s fix on titan, the archive compaction loop
stopped, but another one took its place, every five seconds:

```text
WARN msg="A compaction failed before writing and will be tried again" table="Movie" attempts=25
  retry_in_ms=5000 error=Shoal(CorruptArchive { archive: a8493989-…, partition_id: 3688995164151160054, … })
```

## Cause

A segment compaction merges a sealed WAL segment's frames into the archives, so it loads the
archived copy of every partition its frames name (`load_partitions_for_intents`). A write that
applied without reading its partition (an insert replaces the row whole) still needs the archived
copy at the merge. A record that fails its checksum fails the load, and the job is retried as one
that failed before writing. That is true, and it leaks nothing, but it never succeeds:

- the segment is never merged for the table, so the checkpoint of every group of the table on the
  shard stops moving, and the WAL behind it grows;
- nothing quarantined the copy. The apply had not read the partition, and only a read quarantines,
  so nothing would ever repair it.

A repair alone would not have been enough. The job was built before the repair's install, so its
retry would merge the group's frames at or below the install's boundary over the installed rows.
Those frames are older than the rows, so newer values would be put back to older ones.

## Evidence

**Found on the lab, then reproduced.** `a_segment_merge_onto_a_corrupt_record_has_its_copy_repaired`
(`shoal/tests/cluster_fixture.rs`) corrupts a follower's record of one note, inserts over it through
the leader, and compacts. With the new report taken out:

```text
the merge's corrupt record never quarantined its copy
```

The other half, skipping frames an install replaced, is **established by reading the source**. The
test's writes are inserts, and merging an insert again is harmless, so the test passes with that
half taken out. It covers the path, not the defect.

## The fix

- **A failed load reports the partition** (`report_corrupt(partition, unreadable: true)`), once. The
  shard quarantines the copy as `Unreadable`, and its leader repairs it without an operator
  ([#160](unreadable-partition-stalls-one-copy.md)). The job stays a retry and succeeds once the
  install has replaced the record.
- **A segment job's frames carry their group and index** (`CompactionJob::Segment::frames` is
  `Vec<FrameRef>`). The compactor keeps the boundary of the last snapshot it installed for each group
  (`installed`), and `compact_segment` skips any frame at or below it. A job built before the install
  merges only what is newer than the install.

## Alternatives rejected

- **Leave the partition out of the merge.** Its frames would never reach the archives, and the
  checkpoint would move past them.
- **Quarantine it for its checksum.** Then the copy waits for an operator while the WAL grows behind
  it. Unlike a record a read met, this one blocks the node's compaction of the whole table.
- **Drop the retried job at the install, and rebuild it.** The shard builds jobs from its checkpoint
  and hands each segment once. A job dropped here would have to be handed again, which is what
  `rehand_segments` does for frames above the boundary, and skipping below it gives the same result
  without a second path.

## Invariants to uphold

- **No compaction merges a frame at or below the boundary of an install it came after.** `installed`
  is set when an install lands and never lowered. It is in memory, which is enough: after a restart
  every job is built from a checkpoint the install already moved.
- **A corrupt record met by a merge quarantines its copy `Unreadable`.** One met by an archive pass or
  a read quarantines it for its checksum.

## Still open

Nothing of this item.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_segment_merge_onto_a_corrupt_record_has_its_copy_repaired` (`shoal/tests/cluster_fixture.rs`) | A merge onto a corrupt record fails forever without quarantining its copy. The skip of replaced frames is not caught, as above |

## Related

- [Resolved #165](corrupt-record-compaction-loop.md), the archive pass's loop.
- [Resolved #160](unreadable-partition-stalls-one-copy.md), the unreadable quarantine and its repair.
- [Resolved #104](segments-recompacted-after-restart.md), the frames a job is built from.
