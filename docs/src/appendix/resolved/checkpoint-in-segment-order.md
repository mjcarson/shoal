# 260, 273. A table's checkpoint and its merges ran ahead of a segment it had not merged

A shard hands each sealed WAL segment to the compactors of the tables with frames in it, and moves a
group's checkpoint to its last entry in a segment once its table is done with the segment. A table
was done three ways: its compactor answered `SegmentCompacted`; it had nothing to merge in the
segment, which the hand-off judged at once ([Resolved #258](idle-group-checkpoint.md)); or no table
had anything to merge in it. The last two came at the hand-off, while the table's compactor could
still hold an earlier segment's merge, queued behind a backlog or set aside to be tried again, and
the checkpoint moved past rows its archives did not hold yet (item 260). And a merge set aside to be
tried again ran after its table's later segments had merged, and wrote its older rows over theirs
(item 273). Item 260 was filed by a code review of F87 to F93, reading item 258's fix; item 273 was
found reproducing it, and filed and fixed with it.

## Symptom

- **Item 260.** On a node whose compactor was behind its merges, a crash after the hand-off of a
  segment in which a table had nothing to merge lost every row of the earlier segment still queued,
  from that copy. Without a crash a snapshot cut in that window named a boundary past those rows and
  held none of them, so a copy installed from it caught up by log from the boundary and never
  received them; a backup takes that path on purpose, since its nudge lands alone in the segment it
  rotates.
- **Item 273.** A row written twice, each version in a segment of its own, read back after a
  restart as the older version when the first segment's merge had failed before writing and been
  tried again.

## Cause

`advance_checkpoints` moved each group of a table with frames in a segment to its last entry there,
and asked nothing of the table's earlier segments, which `compacting` listed. A start skips every
frame at or below a checkpoint as merged ([Resolved #104](segments-recompacted-after-restart.md)),
so a checkpoint past an unmerged segment drops its frames at the next start. A cut takes the higher
of what its compactor merged and the shard's checkpoint (`at_least`) for its boundary, and jumps the
compactor's queue ([Resolved #174](snapshot-cut-queue.md)), so it was cut at the checkpoint from
archives that lacked the queued merge. Item 258 widened the window: before it only a segment no
table on the shard had frames in moved the checkpoints at the hand-off; after it, any one table quiet
in a segment did.

The compactor sets a merge that fails before it writes aside, tries it again after a backoff, and
takes new jobs meanwhile ([Resolved #91](compaction-retry.md)). New jobs included the table's later
segments, which merged first; the retried merge then applied its older intents over them, and the
later segment's `SegmentCompacted` had already moved the checkpoint past the retried one.

## Evidence

**Reproduced** by three fixture tests written first and run on the unfixed tree, with two hooks they
need: `HOLD_MERGES`, which keeps every segment merge in its compactor's backlog as a compactor far
behind keeps it, and `FAIL_MERGES <n>`, which fails the next merges before they read anything. One
node at a factor of one; the notes are in a segment handed and held, and the group's only entry in
the next segment is a scrub's nudge.

```text
---- a_crash_beside_an_unmerged_segment_loses_no_row stdout ----
20 of 20 notes were lost by a crash once the checkpoint stood at 23, past 2 where they began:
[(26001, None), (26002, None), …, (26020, None)]

---- a_cut_never_names_rows_its_archives_lack stdout ----
a cut at 23, past 2 where the notes began, holds 0 of 20 of them

---- a_merge_tried_again_keeps_its_tables_later_segments_behind_it stdout ----
assertion `left == right` failed: the archives hold an older version, merged after the newer one
  left: Some("v1")
 right: Some("v2")
```

With the fix all three pass, as do Resolved #258's two tests, Resolved #166's repair of a merge onto
a corrupt record, and the cut boundary test.

## The fix

- **A table's checkpoints move in segment order** (`Shard::advance_in_order`). A segment a table is
  done with joins the table's `done_with` queue, and its groups' checkpoints move only while no
  earlier segment of the table is in `compacting`; the queued segments go in generation order once
  the earlier one's `SegmentCompacted` arrives. All three ways a table is done with a segment go
  through it, and the whole-segment advance is gone: a segment no table merges puts every table of
  it in `merged_now`, so it was the same advance made twice.
- **A merge waiting to be tried again holds its table's later merges and drops**
  (`merges_held`, `take_next`). A cut, an install, an archive pass, a fixture's fault and a shutdown
  run ahead of it.
- **A job that arrives while the compactor waits joins its backlog** and is taken through
  `take_next` like any, where before it ran at once and passed whatever the hold held.
- **The hooks**: `faults::hold_merges` and `faults::fail_merges`, which the fixture's `HOLD_MERGES`,
  `RELEASE_MERGES` and `FAIL_MERGES <n>` arm; idle, each costs a compactor one relaxed load a job.

## Alternatives rejected

- **A cut that takes what its compactor merged and never the checkpoint.** It closes the cut and not
  the crash, and a compactor that merged nothing of a group since its start would have no boundary
  for it, which is why the cut takes the checkpoint at all. With the advances in order the checkpoint
  is a boundary the archives hold.
- **Ordering the advances by group rather than by table.** A group with no frames in the queued
  segment could move at once. The compactor's queue is the table's, and a group's wait is bounded by
  the merge ahead of it, so the table's order is the rule the queue already keeps.
- **Holding the compactor while a merge waits for its retry.** A merge onto a corrupt record waits
  for its copy's install ([Resolved #166](segment-compaction-corrupt-loop.md)), which would never
  run behind it.
- **Letting a drop pass a held merge.** A drop run ahead of a merge of its group's frames has the
  merge write the dropped rows back.
- **Taking item 258's advance back out.** An idle table's backup would never cut again while another
  table on its shard was written.

## Invariants to uphold

- **A group's checkpoint never passes a segment its table has not merged.** Every advance goes
  through `advance_in_order`, and a new way of learning a table is done with a segment has to as
  well.
- **A table's merges run in segment order.** A merge waiting for its retry holds the merges and drops
  behind it; only a cut, an install, an archive pass, a fault and a shutdown pass it.
- **An install may pass a held merge**, since a merge tried again may be waiting for it, and a merge
  skips what an install replaced.
- **Every job goes through `take_next`**, the ones that arrive while the compactor waits included.

## Still open

- **A merge tried forever holds its table's merges behind it**, where before it held only itself.
  The one case known is a merge onto a corrupt record whose copy's install never comes: a group moved
  off its node before its repair. The table's WAL then grows until its retention budget forces its
  groups past it, and a restart, which no longer hosts the group, ends it. Before this fix the later
  segments merged meanwhile and the retried one wrote its older rows over them.
- `compacting` holds a table once a segment, so a segment `rehand_segments` hands a table again
  while its first job is still out is taken for done when either answers. Not examined further.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `cluster_fixture::a_crash_beside_an_unmerged_segment_loses_no_row` | A crash after an idle segment's hand-off loses every row of the queued segment |
| `cluster_fixture::a_cut_never_names_rows_its_archives_lack` | A cut in that window names a boundary past rows it does not hold |
| `cluster_fixture::a_merge_tried_again_keeps_its_tables_later_segments_behind_it` | A merge tried again writes an older version over a later segment's |
| `compactor::order_tests::a_held_merge_holds_the_merges_and_drops_behind_it` | A held merge lets a merge or a drop pass it, or holds an install back |

## Related

[Resolved #258](idle-group-checkpoint.md), whose advance this orders;
[Resolved #91](compaction-retry.md) and [Resolved #166](segment-compaction-corrupt-loop.md) for the
retry; [Resolved #104](segments-recompacted-after-restart.md) and
[Resolved #174](snapshot-cut-queue.md); [F43](../../features/node-recovery.md) for checkpoints and
cuts, [F49](../../features/backup-and-recovery.md) for a backup's nudge.
