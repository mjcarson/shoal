# 165. An archive compaction that met a corrupt record retried forever, leaking what it wrote

## Symptom

On the lab, after titan's archives were corrupted in 29 places to test [#160](unreadable-partition-stalls-one-copy.md),
its `Movie/archives/` grew from about 3 GB, what each other node holds, to **38 GB** in under an
hour. The largest file was 6 GB. Titan's journal held 6,261 lines like:

```text
ERROR FileSystemCompactor::compact_archives: error=Shoal(CorruptArchive { archive: a8493989-…,
  partition_id: 17469737611060103667, … })
WARN  msg="A compaction failed before writing and will be tried again" table="Movie" attempts=141 retry_in_ms=5000
```

## Cause

An archive compaction rewrites what is still live in its least used archives into the active
one, then deletes them (`compact_archives`). Every record is read through `read_record`, which
checks its checksum. A record that failed ended the pass with the error, and the run loop filed
`Archives` jobs as retriable (`JobFailure::Retry`), so the pass was tried again five seconds
later, for as long as the node ran.

The log line said "failed before writing", and that was false. By the time a pass met the corrupt
record it had written every record before it into the active archive. The map intents naming them
were staged and dropped with the failed job (`reset_job`), so the records were unnamed dead bytes,
and the next try wrote them again. Nothing corrupt was ever reported to the shard, because only a
read on the query or apply path quarantines. So nothing was repaired, and the loop never ended.

## Evidence

**Found on the lab, then reproduced.** `an_archive_compaction_leaves_a_corrupt_record_and_quarantines_it`
(`shoal/tests/cluster_fixture.rs`) seeds sixty notes and restarts a follower, so the archive
holding them is no longer its active one. It corrupts one record and writes every other note
again, which leaves the old archive nearly all dead, and then compacts. Against the old behaviour:

```text
the corrupt record never quarantined its copy: {"groups":6,…,"integrity":{"checksum_failures":10,…,"quarantined":0,…}
```

Ten checksum failures in 30 s of retries, and no quarantine.

## The fix

A corrupt record found by an archive compaction is **left where it is**:

- the pass rewrites the rest of that archive and carries on;
- the archive is kept rather than deleted, since the map still names the record;
- the compactor sends `ServerMsg::CorruptRecord { table, partition }` once per partition, and the
  shard quarantines the copy for its checksum (`quarantine_for_checksum`), exactly as a read that
  met the record would have. Reads are then routed around it, and a `Repair` replaces it.

A kept archive stays a candidate for later passes. It has little left to rewrite, and the
partition is not reported twice.

## Alternatives rejected

- **Classify the pass as fatal.** It stops the loop, but it also stops the table's compactor on the
  node, which stops the shard, which is [#160](unreadable-partition-stalls-one-copy.md)'s mistake
  made again.
- **Skip the record and drop it from the map.** A partition that disappears from a replica answers
  reads wrongly and diverges at the next apply. The corrupt record is evidence, and the repair
  is what replaces it.
- **Quarantine it `Unreadable`, so it is repaired without an operator.** That reason means the copy
  stopped applying, and this copy has not. A checksum quarantine is F44's for a corrupt record, and
  F44 wants an operator or a verified majority to replace one.

## Invariants to uphold

- **An archive is deleted only when every record the map names in it was rewritten.** A pass that
  keeps one keeps the archive.
- **A corrupt record met anywhere is reported, never rewritten.** `read_record` is still the only
  reader, and a new caller that meets `CorruptArchive` reports it.

## Still open

- **A segment compaction that has to load a corrupt partition still fails and is retried
  forever** (filed as [item 166](../known-issues.md#166-a-segment-compaction-that-needs-a-corrupt-partition-is-retried-forever)).
  Its merge needs the partition, so it cannot simply leave it. Unlike the archive pass, it writes
  nothing before the load fails, so it leaks nothing. But the table's checkpoint on the shard
  stops moving until the copy is repaired.
- The 38 GB titan held was reclaimed by the lab's re-bootstrap, not by the fix: bytes already
  written are dead space that later archive compactions reclaim.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `an_archive_compaction_leaves_a_corrupt_record_and_quarantines_it` (`shoal/tests/cluster_fixture.rs`) | The pass fails and retries without end, the copy is never quarantined, and every try rewrites records for nothing |

## Related

- [F44](../../features/repair.md), the checksums and the quarantine.
- [Resolved #91](compaction-retry.md), the retry of a job that fails before writing, which this
  job did not.
