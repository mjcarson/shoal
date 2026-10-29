# 232. A former control leader answered capacity it no longer heard

`Members` answers every member's standing from the committed state, which every node holds, and
its capacity from what the control leader heard in members' status reports, which only the
leader holds ([F46](../../features/capacity-rebalancing.md)). A node that had led and stopped
leading kept the table it had heard and went on answering from it. Found by
[F82](../../features/root-capacity.md), whose roots section in `status` makes the table visible.

## Symptom

A node that led the control group and then lost the lead (an isolation, a restart of its peers, a
transfer) answered `Members` with every member's `free_bytes` and `held_bytes` as it last heard
them, for as long as it ran. Nothing marked them as old. A new leader heard a member's capacity
move and the former leader went on showing the figure from before. `shoaladm status` and the
`shoalctl` cluster tab draw whatever node they reach, so through a former leader they showed
figures that had stopped being true, and since F82 each root's figures too.

The planner never read the stale table: a node clears it when it becomes leader, before it plans.
`Stats`, which the same reports carry figures for, already answered by the right rule.

## Cause

`Core::topology` filled each member's capacity from `self.capacity` on every node.
`handle_metrics` cleared `capacity` and `stats` when a node *became* leader, so a new leader
started with no evidence, and nothing cleared them when it *stopped*. Reports go only to the
leader, so a former leader's table stopped changing on the day it lost the lead and stayed in
memory. `cluster_stats` had been written with the rule `topology` lacked: the leader's table on
the leader, a node's own figures anywhere else ([F52](../../features/cluster-stats.md)).

## Evidence

**Reproduced** by `a_former_control_leader_answers_no_capacity_it_no_longer_hears` on europa
against the tree with F82 and without this fix. The control leader is isolated, loses the lead,
is healed and follows. A member's free bytes are then overridden to 7,777, and the new leader
hears it. The former leader still answered the member's figure from before the isolation:

```text
the former leader answered capacity it no longer hears for member 1: {"episode":null,"free_bytes":322182381568,...,"roots":[{"device":"0:31","free_bytes":322182381568,"path":".../target/tmp/.tmptUV8i0","size_bytes":996923146240,"uses":["primary","latency","throughput"]}],...,"state_name":"up","weight":1}
```

With the fix the test passes: the former leader answers nothing for the other two members and
its own measurement for itself.

## The fix

- **`topology` answers by `cluster_stats`'s rule.** On the leader a member's capacity is the
  leader's table. On any other node it is the node's own measurement, for itself only, and nothing
  for another member.
- **A leader stepping down drops its tables.** `handle_metrics` clears `capacity` and `stats` when
  a node stops leading, as it does when one starts. The memory goes, and a node that leads again
  starts from nothing either way.

## Alternatives rejected

**Marking the figures with their age.** `NodeCapacity` carries when it was heard, and `Members`
could answer each figure's age. A reader would then have to judge it, and a former leader's
figures are not old so much as nobody's: no report will ever refresh them while it follows.

**Forwarding `Members` to the leader.** A follower would answer the leader's view, as `status`
wants. But `Members` is also how a node is asked what *it* sees: the fixture and the admin tools
read a node's own view of the membership through it, during elections and partitions. `Stats`
names the leader to ask instead, and `Members` names it too, in `leader`.

**Clearing on step-down alone.** That fixes the stale figures, but a follower then answers no
capacity at all, its own included, though it measures its own every tick.

## Invariants to uphold

- **Only the leader answers another member's capacity.** Any other node answers its own and
  nothing else. Reports go to the leader alone, so any other node's table is stale by construction.
- **A node keeps capacity and figures heard from others only while it leads.** Both are cleared
  at either edge of a lead. The planner runs only on the leader, and it reads a table the lead
  began with.

## Still open

Nothing of this item. `status` through a follower shows one member's roots, which is right and
less than an operator may want. The leader is named in the header, and that is where to ask.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_former_control_leader_answers_no_capacity_it_no_longer_hears` | `shoal/tests/cluster_fixture.rs` | A former leader answers a figure from before it lost the lead, or answers nothing for itself |

## Related

- [F82](../../features/root-capacity.md), which found it.
- [F46](../../features/capacity-rebalancing.md), the capacity table and `Members`' capacity fields.
- [F52](../../features/cluster-stats.md), whose `Stats` read already followed the rule.
