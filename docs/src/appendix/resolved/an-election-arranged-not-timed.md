# 142, in part. The move resume row arranged its election by the timing of a pause

[Resolved #197](retired-driver-holds-slot.md) made a move's source that resumes a dead driver's
drive at `reconfiguring` hand the lead on, never drive its own removal.
`a_source_resuming_a_move_hands_the_lead_on_and_frees_its_driver` holds that. It needs the source
to win the election the driver's death leaves, and it arranged that by timing. Item 142 is the
fixture's long record of rows that fail under load; this is one of its rows, and the item stays
open for the rest ([Known Issues](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)).

Fixed in the flaky-test plan's second wave (2026-10-27).

## Symptom

In three loaded runs, #229's, F91's and #304's, the row failed with *the source did not win the
election the driver's death left*, node two leading the group.

## Cause

The row paused the third member, node two, from before the move until after the driver's death,
so that the source's vote request would be the first thing node two answered on resuming. Under
load, node two's own election timer, run out during the pause, fired before it read the waiting
request, and node two stood and won. The engine did what an election does. The row had counted on
an order.

## Evidence

**Established by reading the row and its message.** The row names the winner, and the three
failures named node two. No fixture verb moved or held a lead, so nothing but timing arranged the
winner.

**The hold, reproduced.** A shard sets the switch of every group it hosts again whenever its own
rule for standing changes: off while every replication link is down
([Resolved #106](isolated-member-term-inflation.md)) or its disk is under the append reserve
([Resolved #156](wal-failure-stops-the-node.md)), on again after, and on when a head start ends. So
`ELECT off` lasted only until the next change. A paused node, as this row's node two is, can come
back to links that went down while it slept. `a_held_copy_stays_held_when_its_links_come_back` was
written first. It holds node two's copy, cuts every data link of node two until its shard stands for
nothing, heals them until it stands again, holds node one too, and kills the leader. On the tree
before the hold:

```text
node 1 names node Some(2) the leader of group d86a431bf009b96d, which no copy may stand for
```

## The fix

- **`ELECT <group> on|off`** sets a node's copy of a group's own campaigns through openraft's
  switch (`runtime_config().elect`, `ReplicationVerb::Elect`). Every shard is asked, and one not
  hosting the group says so.
- **The row keeps node two from standing** before the pause and lets it stand again once node one
  leads. The source is then the one member that can win, whatever the load. The pause stays, so
  the move goes on with the leader and the source as the old configuration's quorum, as before.
- **The row takes node two leading as the source's win** (the confirmation's A/B, 2026-10-27). The
  source resumes the drive at once and hands the lead on, to the one member of the target that is
  neither the destination nor down ([Resolved #197](retired-driver-holds-slot.md)): node two. A
  loaded host's reads saw node two first, and the row failed with *the source did not win the
  election the driver's death left* (`left: 2`). Node three is still a learner, since the driver
  died before proposing the joint configuration, and node two cannot stand by its own timer, so
  only the source can win, and only a leader can hand node two the lead. A transfer is a leader's,
  and `ELECT off` does not stop one: openraft's `HandleTransferLeader` elects the target without
  asking the switch.
- **A copy held from standing stays held** (the confirmation, 2026-10-27). `ELECT` records the hold
  (`faults::hold_elections`), and every place a shard turns the switch on reads it first: its rule
  for standing, a returning leader's lease and the head start ending, and a handle built fresh. Each
  shard's groups view says whether it stands by its own rule (`ShardReplication::stands`).

## Alternatives rejected

- **Lengthen the pause, or resume node two later.** A later resume changes which timer runs out
  first only on a host that keeps time; the load is what reorders them.
- **Transfer the lead to the source after the election.** The row is about the source winning the
  election the driver's death leaves, and resuming the drive from there. A transfer would test
  another path.

## Invariants to uphold

- **A row that needs one member to win an election arranges it with `ELECT`**, never by timing a
  pause or a restart.
- **`ELECT off` is lifted in the row that set it**, so nothing after the row's claim runs with a
  member that cannot stand.
- **Every place that turns a copy's elections on reads the hold first.** A new place that calls
  `runtime_config().elect(true)` without `faults::elections_held` lets a held copy stand again.
- **Node two leading proves the source's win only while node two cannot stand and node three is a
  learner.** A crash point moved past the joint configuration's proposal makes node three a voter,
  and then node two leading proves nothing.

## Still open

- The rest of item 142's rows, each on [the flaky page](../flaky-tests.md).

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_held_copy_stays_held_when_its_links_come_back` (`cluster_fixture`) | A held copy's shard sets its switch on again when its links come back, and it wins an election nobody may stand for |
| `a_group_held_from_standing_is_held_until_let_go` (`shoal-core`, `faults`) | The hold's table keeps a group after it is let go, or loses one still held |
| `a_source_resuming_a_move_hands_the_lead_on_and_frees_its_driver` (`cluster_fixture`) | Under load, node two's timer runs out first and it wins the election the row needs the source to win; or the source's hand-off to node two is read before the source is, and taken for node two's own win |

## Related

- [Resolved #197](retired-driver-holds-slot.md), what the row holds.
- [Item 142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host).
- [Tests That Fail Under Load](../flaky-tests.md).
