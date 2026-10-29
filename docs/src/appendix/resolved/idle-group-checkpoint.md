# 258. An idle table's checkpoint stood still while another table on its shard was written

A shard hands each sealed WAL segment to the compactors of the tables with frames in it, and moves a
group's checkpoint to its last entry in the segment once its table's compactor has merged it. A
group whose only frames in a segment are commands no compactor sees, a scrub's nudge or a placement
change, had nothing for any compactor to merge, and its checkpoint moved only when the segment held
no table's frames at all. So on a shard where another table was being written, an idle group's
checkpoint never moved, and a backup or a repair of that table nudged it forever. Found by
[F93](../../features/operations-and-the-real-cluster.md)'s archive row and filed and fixed in one
change.

## Symptom

A backup of `ObjectDb` while an object was written in place left every group of its document table,
which nothing wrote, in `Cutting`, with a driver and no outcome, until the test gave up. The shard's
log said the WAL sealed a segment every five seconds and handed it to `tables=2`, the bucket's two,
and nothing more.

## Cause

`ShardWal` sealed each segment the backup's `advance_past` rotated, and the hand-off grouped the
segment's groups by table. For the document table, `frames_in` found only the nudges, which a
compactor is never given, so no job was sent and the table was "done with the segment now" in a
comment and nowhere else: the hand-off called `advance_checkpoints(generation, None)` only when no
table at all had frames to merge, and `handle_segment_compacted` moved only the compacted table's
groups. An idle group's checkpoint stayed where it was, and `advance_past` nudged, rotated and
waited five seconds, round after round, for as long as anything else wrote.

## Evidence

**Reproduced** by `an_idle_tables_backup_cuts_while_another_is_written`, written first and run on
the unfixed tree: three nodes serving `ObjectDb`, an object written in place throughout, and a
backup of the document table alone.

```text
Error: NotReady("BACKUP_STATUS 21e844bf-… never finished: {"groups":{"15486681503143336831":
{"boundary":null,"driver":"43b48f10-…","outcome":null,"phase":"Cutting"}, … "phase":"Cutting"}, …
"phase":"Cutting"}}, …}")
test an_idle_tables_backup_cuts_while_another_is_written ... FAILED
```

With the fix every group is written within seconds.

## The fix

**A table with frames in a segment and nothing to merge moves its own groups' checkpoints at the
hand-off**: the hand-off records each such table, and once the segment is marked handed calls
`advance_checkpoints(generation, Some(table))` for each, beside the whole-segment case it already
had.

## Alternatives rejected

**Handing the nudges to a compactor.** A scrub command is never merged by design; a compactor that
took it would merge nothing and only delay the checkpoint further.

**A checkpoint moved by the backup's driver itself.** The checkpoint is the compactor's to move, as
the archive's state; a driver that set it would claim a merge nobody made.

## Invariants to uphold

- **A group's checkpoint moves for every segment it is done with**, whether its table merged
  something there or nothing, once its table is done with every segment before it
  ([Resolved #260, 273](checkpoint-in-segment-order.md)).

## Still open

~~Nothing.~~ ~~The advance this fix added, like the `compacted_now` one before it, moves a
table's checkpoint past a later segment without waiting for the merges of its earlier ones, so a
crash or a cut in that window can lose or skip rows (item 260).~~
Nothing: the advance this fix added moved a table's checkpoint past a segment its compactor still
held, as the whole-segment one before it did, and both now wait for every earlier merge of the
table ([Resolved #260, 273](checkpoint-in-segment-order.md)).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `cluster_fixture::an_idle_tables_backup_cuts_while_another_is_written` | An idle table's backup never cuts while another table is written |
| `cluster_fixture::objects_survive_a_backup_and_restore_by_archive` | A bucket's backup under writes never finishes its document groups |

## Related

[F93](../../features/operations-and-the-real-cluster.md), [F43](../../features/node-recovery.md) and
[F44](../../features/repair.md) for checkpoints and nudges, [F49](../../features/backup-and-recovery.md).
