# 175. A group stopped on a node kept a log index into segments the node then reclaimed

## Symptom

In rebuild 7 of [section 8](../../cluster-testing/correctness.md#8-an-unplaced-member-coordinates),
hyperion's new identity crash-looped 147 times. The first failure came while it was running, when
a shard rebuilt a learner copy of a keyword group:

```text
20:39:10 WARN a shard died  shard=0 error="tablet group 797d61748e5ca3c4 could not be built:
  building the group: when Read Logs: Opening /optane/shoal/wal/Shard-0/000000000000000003.wal:
  No such file or directory (os error 2)"
20:39:10 panicked at …/scoped-tls-1.0.1/src/lib.rs:168:9
         panic in a destructor during cleanup … aborting.
```

Every start after that failed at the same group:

```text
Failed to get log entries, expected index: [51747, 51752), got [None, None)
```

## Cause

Two defects, one feeding the other.

**A stopped group's log outlived its segments.** When the map stops naming a group on a shard, and
the shard is not a published move's source, the shard stops the group's handle and nothing else.
The WAL keeps the group's index. `sweep_segments` counts a group the shard no longer hosts as purged
past everything (`replication.groups.get(group).is_none_or(…)`), so the segments that index points
into are reclaimed. If the map then names the group on that shard again, the copy is built on an
index into files that are gone.

[#171](failed-group-publishes-its-set.md)'s fix made this path common. A failed move's record now
ends unpublished, so the destination stops its learner copies. The retry names them again, and a
learner that had entries past its checkpoint re-reads them at its build. Before #171, a failed set
was wrongly published and its learner was never stopped.

**One group that could not be built killed its node, at every start.** `handle_group_up` returns a
shard error for any group that fails to build. A process whose shard dies exits, and systemd
starts it again into the same failure.

## Evidence

**Established from the lab's logs and the source.** The error names the reclaimed segment and the
group. `a_move_asked_again_rebuilds_its_learner_from_nothing` (`shoal/tests/cluster_fixture.rs`)
drives the same path in the fixture: a move whose learner took entries past its checkpoint fails,
the destination seals and reclaims its segments, and the move is asked again. It passed on the
unfixed tree as well. The fixture's destination did not re-read a reclaimed segment at the rebuild,
and the conditions for it were not found. It stays as the regression test of the path the fix
changes: the retry ends `Moved`, with the rows on the destination.

## The fix

- **A group stopped because the map no longer names it has its log forgotten.** The shard marks it
  stopping, shuts its handle down, then forgets its log (`GroupStopped`,
  `handle_group_stopped`): a marker frame, after which a replay rebuilds nothing from its entries.
  A copy the map names here again waits until that is done, then starts with no log and is fed by
  its leader. A published move's source already forgot its log when it retired.
- **A learner copy that cannot be built is built again, empty, once a run.** It holds no vote, and
  nothing it acknowledged counts toward a quorum, so forgetting its log loses nothing. Writes that
  waited for it are refused retriably. A voter that cannot be built still stops the shard, as
  before.

## Alternatives rejected

- **Keep a stopped group's segments until the group is gone for good.** The shard cannot know that:
  a failed move may never be asked again, and its segments would be held for ever.
- **Rebuild any copy whose log cannot be read, voters included.** An emptied voter grants its vote
  to a candidate with any log, which is how [#109](volatile-majority-loss.md) lost a survivor's
  committed entries. That needs #109's guard extended to durable copies first. It was, as a floor
  on the vote, by [#176](unreadable-voter-log.md), and a voter is now rebuilt too.

## Invariants to uphold

- **A copy's log index never outlives the copy on the shard.** Every path that stops hosting a group
  forgets its log, whether a retirement or a stop.
- **Nothing is forgotten under a live handle.** The forget waits for the shutdown, and a rebuild
  waits for the forget.
- ~~**Only a learner is ever built empty in place of a log it had.**~~ Since
  [#176](unreadable-voter-log.md), a durable voter is too, but only under a floor on its vote
  written before its log is forgotten.

## Still open

- ~~**A voter whose log cannot be read still takes its node down at every start.** The node needs a
  `rebuild`. It is filed as #176.~~ [Resolved #176](unreadable-voter-log.md): a durable voter is
  built again empty through the same arm, under a floor on its vote until it is fed past what it
  held. Reproducing it found that a hole inside one of openraft's read chunks did not stop the node
  at all but was skipped in silence, which the same fix closes.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_move_asked_again_rebuilds_its_learner_from_nothing` (`shoal/tests/cluster_fixture.rs`) | A move asked again after a failed one does not end `Moved`, or the destination dies or lacks the rows |
| `migration_resumes_after_each_phase_failure` (same file) | A move killed at any phase does not finish |

## Related

- [#171](failed-group-publishes-its-set.md), which made the path common.
- [F45](../../features/replica-migration.md), the retirement path that already forgot a log.
- [#151](purge-ahead-of-its-marker.md), the other reason a segment is deleted too early.
