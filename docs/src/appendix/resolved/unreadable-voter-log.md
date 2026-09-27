# 176. A voter whose log had a hole stopped its node, or diverged in silence

## Symptom

Filed from [#175](stopped-group-log.md)'s fix, which rebuilt a *learner* whose log could not be
read and left a voter alone: "a voter whose log cannot be read stops its node at every start".
`handle_group_up` turned a group that failed to build into a shard error, the process exited, and
systemd started it into the same failure. On the lab the error was openraft's:

```text
Failed to get log entries, expected index: [51747, 51752), got [None, None)
```

Reproducing it found a worse form. A voter whose log lost a sealed segment from its middle does
not always fail to build. When the hole falls inside one of the 64-entry chunks openraft reads to
re-apply the log, the copy builds and serves. It never applies the entries in the hole, and nothing
says so. In the fixture, node two came back with 94 notes where its peers held 110, at the same
applied index on every group:

```text
the digests of Note never agreed: [
  {groups: {ac122b15f33211ff: 27, e4a21a1c97069dc9: 43, fc538f09cbc41a36: 44}, hash: 15873626370682114709, rows: 110},
  {groups: {ac122b15f33211ff: 27, e4a21a1c97069dc9: 43, fc538f09cbc41a36: 44}, hash: 15873626370682114709, rows: 110},
  {groups: {ac122b15f33211ff: 27, e4a21a1c97069dc9: 43, fc538f09cbc41a36: 44}, hash: 13970837995607797951, rows: 94}]
```

A read at `One` through that node answered "not found" for sixteen rows that every other member
held.

## Cause

**A read of the log skipped the entries it did not have.** `GroupStore::try_get_log_entries`
(`shoal-core/src/server/wal/mod.rs`) collected the indexes the WAL's index held inside the range it
was asked for, and read those. An index missing from the middle of the range was not an error: the
read returned fewer entries. openraft's `reapply_committed` checks only a chunk's first and last
index. So a replay over a hole applied the entries on either side of it and moved the copy's applied
index past it, as if the entries in the hole had been applied.

When the hole covered a whole chunk, or a chunk's first or last index, openraft's check failed.
`Raft::new` then returned the error above, and **one group that could not be built stopped its
node**. #175 turned that error into a rebuild for a learner only. A learner holds no vote, so
forgetting its log loses nothing a quorum counted. A voter is different: an emptied voter grants its
vote to a candidate with any log, which is how [#109](volatile-majority-loss.md) lost committed
entries on a volatile group. #109's guard covers volatile copies only.

The same gap already existed with no hole at all. [#99](durable-log-reversion.md) accepted that a
durable voter whose *whole* WAL is lost starts empty and is fed by its leader. That copy voted with
no guard: it could elect a candidate lacking entries that it had acknowledged and that no other copy
held yet.

A hole comes from a sealed segment that is gone while the copy still needs what is in it. That can
be a file lost or deleted from under the node, or #175's index into a reclaimed segment. On a
Zen1 lab node whose compactor is hundreds of jobs behind
([O74](../optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench)),
the copy's checkpoint stays behind many sealed segments, so what it needs from them reaches far back.

## Evidence

**Reproduced.** `a_voter_whose_log_has_a_hole_is_fed_not_fatal` (`shoal/tests/cluster_fixture.rs`)
writes a hundred 4 KiB notes through three voters with 64 KiB segments. Node two's segment handoffs
are held with the new `HOLD_COMPACTION` verb, so its checkpoint stays behind its sealed segments as
it does on a busy lab node. The test then kills node two, deletes the middle one of its seven
segments, and restarts it.

- **Without the hold**, the test passed on the unfixed tree. The fixture's compactor keeps the
  checkpoint within an entry of the log, so every sealed segment lies below it, and openraft purges
  to the checkpoint without reading the hole. The hold is what makes the hole matter.
- **With the hold, on the unfixed tree**, node two came back up with the digest above: 16 notes
  missing at the same applied indexes (`target/lab/r176/unfixed-held.log`).
- **On the fixed tree**, the test passes: node two reports the lost log, is fed past its floor, and
  serves every note (`target/lab/r176/fixed.log`).

The lab reproduction, with a hyperion segment deleted under the bench, is
[section 9](../../cluster-testing/correctness.md#9-a-voter-whose-log-has-a-hole) of the cluster
testing chapter.

## The fix

- **A read across a hole is refused.** `try_get_log_entries` fails with `the log of group … has no
  entries from … to …` when the indexes it collected are not consecutive. So no replay, append or
  snapshot read can skip a lost entry again.
- **A hole is looked for before a durable group is built.** `ShardWal::hole_of` returns the first
  run of missing indexes between the log's purge point, or its first entry, and its last entry.
  `rebuild_groups` then does one of two things:
  - **A hole wholly at or below the checkpoint** holds nothing the archives lack. The log is purged
    through the checkpoint (`ShardWal::stage_purge`, which `GroupStore::purge` now calls too), and
    the copy keeps its log and its vote.
  - **A hole past the checkpoint** means the copy cannot trust its log. `forget_durable_log` forgets
    the log under a **floor**.
- **A copy whose log is forgotten keeps a floor and its vote.**
  1. The floor is the last log id the copy knew: its log's last, its checkpoint's, or an earlier
     floor's, whichever is greatest. It is written, synced, to `wal/Shard-N/floor/<group>` *before*
     the log is forgotten, together with the copy's last vote.
  2. The log is forgotten.
  3. The vote is staged again (`ShardWal::restore_vote`), so the copy keeps the term it voted in.
- **A copy below its floor grants only to a candidate at or past it.** `grants_above_floor` is
  judged on the replication lane beside #109's rule, for votes and pre-votes. A candidate's last log
  id is compared with the floor the way openraft compares log ids: by leader id, then index. There
  is no grace, unlike #109's.
- **The floor is cleared once the copy has applied past it.** This is checked on the shard's sweep
  (`probe_cores`), which removes the marker. The replication report carries `floor` on each group
  while it holds.
- **A durable voter that fails to build is rebuilt the same way, once a run.** This covers failures
  not found by the hole check, such as an index into a file that is gone. `handle_group_up`'s
  #175 arm now takes any durable copy, and uses `forget_durable_log` for a voter. `reset_learners`
  is now `reset_copies`. A copy that fails again in the same run still stops the shard. The arm also
  puts back what the first build took from the shard's memory: a quarantine read from disk and a
  received snapshot not yet installed. #175's learner path dropped both.
- **#99's path is held to a floor too.** A durable copy that holds nothing in its WAL, but whose
  checkpoint or archives say it held the group, and that is not already floored, is held to its
  checkpoint's log id. That is the most it can know it held.

## Alternatives rejected

- **Stall the copy, as an unreadable partition does since
  [#160](unreadable-partition-stalls-one-copy.md), and have its leader repair it.** A copy stalls
  without a handle, and a repair's install builds the group again over the same log. So the hole
  would be read again by the start that the install triggers. It also needs a leader with a
  verified majority to repair from, and it pins the WAL while it waits. Feeding the copy from its
  leader is what #99 already does for a whole lost log, and it needs nothing new.
- **Rebuild the voter empty with #109's guard as it stands.** That guard refuses only a candidate
  as empty as the copy, and it expires after two election timeouts. A durable copy's lost entries
  may be committed and held on just one other member. So a candidate with *some* log, short of what
  the lost one held, must be refused too, and for as long as it takes.
- **Remove the copy from its group and add it back as a learner.** This is the textbook answer to
  a lost disk, and it is what `cluster rebuild` does to a whole node. For one group it is a
  membership change per copy, driven from the control plane. That is heavier than a vote guard, and
  a copy that has not caught up is exactly what the guard makes harmless.
- **Keep skipping gaps in reads and check only at open.** A gap made at runtime would still be read
  as fewer entries. The read refusing is what makes a hole visible wherever it comes from.

## Invariants to uphold

- **A durable log is read as a run with no gap, or not at all.** Any path that can leave a gap
  inside a group's index — a lost segment, a reclaimed one, a partial replay — is either purged
  below the checkpoint or forgotten under a floor. Nothing may read across a gap.
- **The floor is on disk before the log is forgotten.** A crash between the two must leave a copy
  that is floored with its log, never one that is unfloored without it.
- **A floor only rises until it is cleared.** A second loss takes the greater of its own floor and
  the one held. The marker's vote is used only when the WAL has none.
- **A floor is cleared by applying past it, never by time.** Entries applied past the floor are
  committed and durable, so the copy again holds what it had acknowledged.
- **Only a hole at or below the checkpoint is purged in place.** Above the checkpoint, the entries
  in the hole may be committed and not yet in the archives.
- **A copy is rebuilt empty at most once a run.** A copy that fails its rebuild stops its shard, so
  a loop of rebuilds cannot hide a defect in the rebuild itself.

## Still open

- **A group whose every voter lost its log waits for ever.** Each refuses the others, as the
  contract says it should. The operator's way out is `cluster rebuild`. A command to clear a floor
  by hand is not built.
- **A running copy whose log read fails is not rebuilt.** Its `RaftCore` stops, the shard reports
  `core_dead`, and the copy serves nothing until the process restarts, when the start's hole check
  takes it. This is the same limit #109 left for a dead core.
- **A floored copy's vote is lost if the marker cannot be read.** An unreadable marker is logged by
  name and the copy votes unguarded. A marker is a synced file the node writes itself, so this needs
  its storage to be damaged a second time.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_voter_whose_log_has_a_hole_is_fed_not_fatal` (`shoal/tests/cluster_fixture.rs`) | Node two's copy comes back without the notes in the hole, and the digests never agree; or the node dies at the start; or its floor never clears |
| `a_lost_segment_is_a_hole_found_at_open` (`shoal-core/src/server/wal/tests.rs`) | A lost middle segment is not reported as a hole, a read across it skips the gap, a purge through it leaves one, or a forgotten group loses its vote across a reopen |
| `a_copy_below_its_floor_grants_only_to_a_candidate_past_it` (`shoal-core/src/server/shard/groups.rs`) | A floored copy grants to an empty candidate or to one behind its floor, or refuses one past it |
| `durable_log_reversion_is_fed_not_fatal` (`shoal/tests/cluster_fixture.rs`) | A voter that lost its whole WAL, now held to its checkpoint, is never fed or never clears |
| `a_move_asked_again_rebuilds_its_learner_from_nothing` (same file) | #175's learner rebuild, now the same arm as a voter's, no longer ends `Moved` |

## Related

- [#175](stopped-group-log.md), the learner half of the same rebuild.
- [#109](volatile-majority-loss.md), the volatile rule the floor sits beside.
- [#99](durable-log-reversion.md), a whole lost WAL, now floored.
- [#160](unreadable-partition-stalls-one-copy.md), the stall rejected here.
- [O74](../optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench),
  the backlog that leaves a lab node's checkpoint far behind its sealed segments.
