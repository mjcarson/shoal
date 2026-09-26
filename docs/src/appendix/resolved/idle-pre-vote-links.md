# 173. A pre-vote over a link that was down sent nothing, so an idle cluster never elected

## Symptom

After the lab's fourth rebuild, europa and titan were restarted while titan's disk was full.
Titan crash-looped on `ENOSPC` for a few minutes, then came back. With no bench running,
`cluster stats` then showed each member leading 4 groups: 12 of the 36 groups had a leader. The other
24 had none for as long as they were watched, over twenty minutes, although two of their three
voters were up. Every set not yet moved off the rebuilt node's old identity was among them. The
rebuild's plan sat at `6/7 moved` because the next set's groups had no leader to drive the move.
Europa's journal repeated, per group:

```text
INFO  openraft::core::raft_core: trigger pre-vote
ERROR openraft::core::raft_core: while requesting pre-vote, error: Unreachable node: …
      the replication link failed: the link to b6996b6b… is not up
```

`b6996b6b` is titan, which was up.

## Cause

A shard's link to a peer dials only when a frame is queued for it. `GroupPeer::pre_vote` asked the
link whether the peer answers pre-votes. That is known only once the link is up. When the link
was down, it returned an error without sending anything. Titan's crash loop had failed every dial
europa's links made and dropped what they held. After that, a group with no leader had nothing
else to say to titan: no heartbeats, no appends, no writes. So its link stayed down, every
pre-vote failed on it, and no election ever reached titan. The control plane's pre-vote had the
same shape.

It needs a cluster with no traffic on those links. Under the bench, any forwarded write or
heartbeat brings a link up, and the next pre-vote goes through.

## Evidence

**Found on the lab, then reproduced.** `an_idle_cluster_elects_again_after_restarts_with_a_voter_gone`
(`shoal/tests/cluster_fixture.rs`) starts three nodes at factor three and writes to every group.
It kills node two for good, kills node one, restarts node zero alone for ten seconds, then
restarts node one, and sends nothing. Against the unfixed tree, after 60 s:

```text
groups never elected on an idle cluster: [(0, 828985111392957698), (0, 5742790432285500188),
(0, 14717791056401494682), (1, 828985111392957698), (1, 5742790432285500188),
(1, 14717791056401494682)]
```

The first version of the test restarted both survivors while the other was up, and passed on the
unfixed tree: the survivor that was up kept the links wanted. The lab's order, one node down
while the other started, is what leaves them idle.

## The fix

A pre-vote over a link that is not up is sent like any other RPC (`GroupPeer::pre_vote`, and
`ControlPeer::pre_vote` on the control lane). Queuing its frame is what makes the link dial. The
rule from [#144](post-heal-elections.md) holds: a peer that cannot be reached answers an error,
never a grant. A link that is up with an older build is still granted locally, as before.

## Alternatives rejected

- **Dial every member's link on a timer.** Every shard would hold a connection to every peer
  whether or not it has anything to say, which is what lazy links exist to avoid.
- **Grant locally when the link is down.** That is the bug #144 fixed: a node cut off from every
  peer would grant itself a quorum.

## Invariants to uphold

- **Anything an election needs from a peer has to be able to bring the link up.** A request that
  is not sent when the link is down never brings it up.
- **A down link is never a grant.**

## Still open

- An older build answers a pre-vote sent before the link negotiated capabilities with an error,
  so one election round in a rolling upgrade can be lost. The next round finds the link up.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `an_idle_cluster_elects_again_after_restarts_with_a_voter_gone` (`shoal/tests/cluster_fixture.rs`) | Groups stay leaderless on an idle cluster whose links to a restarted peer went down |
| `a_silently_cut_node_rejoins_without_elections` (same file) | A cut-off node's pre-votes are granted, or it stands at a new term |
| `a_member_isolated_on_every_lane_heals_without_dying` (same file) | An isolated member elects itself |

## Related

- [#144](post-heal-elections.md), which introduced pre-vote.
- [F38](../../features/inter-node-transport.md), lazy links.
- [Section 8](../../cluster-testing/correctness.md#8-an-unplaced-member-coordinates), where it was found.
