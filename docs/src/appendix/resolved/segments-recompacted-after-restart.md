# 104. A restart merged every sealed segment below the checkpoint into the archives again

## Symptom

A cluster node restarted with sealed WAL segments still on disk - every segment between the
purge point and the checkpoint, which at the default `retained_entries` of ten thousand is most
of what the WAL holds - handed all of them to the compactors again on its first sweep. Each was
merged in generation order over archives that already held the effect of every one of them, so
for the length of the merges the archives ran *backwards* through the table's history: a row
written at `v1` in the first segment and `v2` in the second was `v1` on disk again until the
second segment's merge caught up. A read through the node in that window loaded `v1` from the
archive and kept it resident, and a digest read `v1`. At M7 the window would have been
permanent: a snapshot cut from the archives inside it carries the older generation under a
boundary that names the newer one, and the replica that installs it never hears of `v2`.

## Cause

`SegmentView::handed` lives in memory only. `ShardWal::open` rebuilds every segment's view from
the files with `handed: false`, and `sweep_segments` in `shoal-core/src/server/shard/groups.rs`
judges an unhanded sealed segment by one rule: every group with frames in it has applied past
its last entry there. After a restart every group's applied position *is* its checkpoint, so
every sealed segment below the checkpoint is resolved at once and handed at once, and
`ShardWal::frames_in` answered every command frame of the group in the segment whether or not
the archives already held it. The purge point does not help: a frame is dropped from the index
only once openraft purges it, and openraft purges `retained_entries` behind the snapshot.

## Evidence

**Reproduced.** `restart_does_not_recompact_segments_below_the_checkpoint` in
`shoal/tests/cluster_fixture.rs` writes five hundred notes at `v1`, seals and compacts them past
on every node, writes the same five hundred at `v2` and compacts past those too, restarts node
two, forces a sweep, waits until the first handed segment's merge has finished while a later one
is still running, and compares node two's digest - which reads the archives, since nothing is
resident after a restart - with node zero's. Against the tree before the fix, with the sweep
handing every frame:

```text
thread 'restart_does_not_recompact_segments_below_the_checkpoint' panicked at shoal/tests/cluster_fixture.rs:3560:5:
assertion `left == right` failed: node two's archives no longer hold the state its checkpoint names:
{"groups":{...},"hash":4980398009914967013,"rows":501} vs {"groups":{...},"hash":12031719376766914753,"rows":501}
```

Five hundred and one rows on both, and a different hash: the same keys at the older text. The
unit test `frames_at_or_below_the_checkpoint_are_not_handed_again` beside the WAL is the same
rule in isolation, on a live index and on one rebuilt from the files.

## The fix

`frames_in` takes each group with the index its archives are complete to, and answers only the
frames above it; the sweep passes every group's checkpoint. A sealed segment below every
checkpoint is still marked handed - it has to be, so its deletion can be judged once the groups
purge past it - but it hands no frames, is never listed as compacting, and moves no checkpoint.
The digest reads archived partitions beside resident ones (`StorageSupport::archived_keys`),
which is what let the test see the archives at all and is what every M7 test that compares a
freshly installed replica relies on.

## Alternatives rejected

**Persist `handed`.** A marker per segment, or a field in the checkpoint file, saying which
segments were handed. It records the wrong fact: a segment handed and not yet merged when the
process died *should* be handed again, and a handed marker would skip it. The checkpoint is the
fact - the archives are complete to it - and the frames above it are exactly what needs merging.

**Purge more aggressively so the segments are gone.** The retained log is what a slow follower
catches up from without a snapshot; shrinking it to fix a restart would trade one cost for
another, and the segments between the purge point and the checkpoint would still be re-merged.

**Skip a segment whose every group is checkpointed past it, and hand the rest whole.** Right for
the segments wholly below the checkpoint and wrong for the one straddling it, whose frames
below the checkpoint would still be merged again. Filtering by index covers both.

## Invariants to uphold

- **A frame at or below a group's checkpoint is never handed to a compactor.** The archives hold
  its effect; merging it again moves the archives backwards for as long as the merge of every
  later frame takes. `frames_in` enforces it and the sweep has no other path to the compactors.
- **A segment below every checkpoint is still marked handed.** Deletion is judged on handed
  segments, and a segment that is never handed is never deleted.
- **The checkpoint moves only on `SegmentCompacted`.** A segment that hands no frames advances
  no checkpoint, since there is nothing for it to advance past.
- **The digest reads the archives for what is not resident.** A resident copy shadows the
  archived one; a replica that holds nothing in memory hashes the same state as one that
  applied everything itself.

## Still open

- A segment handed and not yet merged when the process died is merged again at open, which is
  right, and the merge of a frame twice is idempotent for an insert and a delete and is not
  promised to be for an update whose intent is not a plain set. Nothing in the schema today has
  one.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `frames_at_or_below_the_checkpoint_are_not_handed_again` | `shoal-core/src/server/wal/tests.rs` | A segment below the checkpoint hands its frames, on the live index and after a reopen |
| `restart_does_not_recompact_segments_below_the_checkpoint` | `shoal/tests/cluster_fixture.rs` | Node two's digest disagrees with node zero's in the window between two re-merges, and its sweep lists a segment as compacting |

## Related

[F40. Replication](../../features/replication.md), whose sweep this is;
[F43. Node recovery](../../features/node-recovery.md), whose snapshot boundary depends on it;
[C5. Replication](../../distributed/replication.md).
