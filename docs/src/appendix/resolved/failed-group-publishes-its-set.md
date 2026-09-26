# 171. A set whose one group failed its move was published anyway

## Symptom

In the same rebuild as [#170](uncached-log-reads.md), the move of set 4 onto hyperion's new
identity covered two groups. The `MovieByKeyword` group activated. The `Movie` group failed its
catch-up. Two seconds later both groups logged that their members had moved, naming the new copy
as a voter:

```text
16:24:33 titan ERROR a group's move failed  group=c7ec528e6824849d phase="catching_up"
16:24:35 titan INFO  a tablet group's members moved under it  group=c7ec528e6824849d
                     members=[577dcf39/1, 0935612f/1, 218f1b4f/1] learner=false
```

The plan then treated the set as moved. It never replanned it, finished `Completed` with 17 moved
and one failed, removed the old identity, and `cluster rebuild` reported success. Afterwards
`cluster status` said "3 of 3 copies" with no set under-replicated. But the `Movie` group's
committed voters had never taken the new copy: they were still the old, removed identity and the
two survivors.

Every acknowledged insert and every csv row still read back through each member alone. The new
copy was a learner fed by the log, and it held the rows. Nothing was lost, but the group would
have stopped at the next failure of either survivor while every view called it healthy.

## Cause

A group whose move fails is committed `Done` with a `Failed` outcome, whatever phase it reached.
`GroupMove::is_activated` was `phase.rank() >= Activated.rank()`, and `Done` ranks above
`Activated`. So the last other group to activate found every group "activated" and published the
set's configuration. [F45](../../features/replica-migration.md#limitations) says the opposite: a
group activated before another of the set failed keeps its target membership "while the
configuration is not published".

## Evidence

**Established from the lab's logs and the source, then reproduced.**
`a_set_with_a_failed_group_is_not_published` (`shoal-core/src/server/control/types.rs`) records a
move of a two-group set, reports one group `Activated` and the other `Done` with a failure. Against
the unfixed tree:

```text
a set with a failed group was published: {0: DataConfiguration { tablets: [0, 6, 12, …],
members: [...], configs: {GroupId(1939045904159552408): 40}, published_at: 11 }}
```

The published configuration carries the one activated group's index and none for the failed one.

## The fix

- **`is_activated` is false for a failed group** (`GroupMove::is_failed`), so a set with a failed
  group is never published.
- **The record ends once a group has failed and every other is settled**, that is, done or
  activated (`GroupMove::is_settled`). An activated group under a record that can no longer be
  published would otherwise wait for that publish for ever, and the plan on the record with it.
  The record ends `Failed`, the map stops carrying it, and the queue behind it is released.
- **The plan sees a failed step** and replans the set, up to `REPLAN_FAILURES` times. The new move
  reconciles each group from its committed membership, as F45 already did after a crash: the
  activated group is taken as already configured, and the failed one starts again from its learner.

## Alternatives rejected

- **Roll the activated groups back.** Proposing the old membership again is a second membership
  change under load, for a set about to be asked again anyway. F45 decided that nothing rolls a
  group back, and the retry's reconcile makes that sufficient.
- **Publish the set with the failed group's copy as a learner.** Routing would then name a copy
  that its group does not count toward a quorum, which is exactly the disagreement this item was.

## Invariants to uphold

- **A configuration is published only when every group of its set committed the target
  membership.** Routing and the groups' quorums must name the same copies.
- **A move record always ends.** Every path that makes publishing impossible also has to end the
  record, or a plan waits on it for ever.

## Still open

- **A cluster that already published such a set** keeps the disagreement. Asking the move again
  does not repair it, because the set already names the destination. A `rebuild` of the node, or a
  move of the set off it and back, puts the group's membership right. The lab's cluster was
  destroyed instead.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_set_with_a_failed_group_is_not_published` (`shoal-core/src/server/control/types.rs`) | A set with a failed group is published, routes to the destination, or its record never ends |
| `a_move_is_recorded_queued_published_and_released` (same file) | The last group activated no longer publishes a set whose groups all moved |

## Related

- [#170](uncached-log-reads.md), whose slow catch-up made the group fail.
- [F45](../../features/replica-migration.md), the move and its reconcile.
- [F46](../../features/capacity-rebalancing.md), the plan that replans a failed set.
- [F56](../../features/cluster-rebuild.md), the rebuild that reported success.
