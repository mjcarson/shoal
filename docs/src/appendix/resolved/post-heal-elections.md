# 144. A node cut off by dropped packets came back at a higher term and unseated healthy leaders

## Symptom

On the lab, hyperion was cut off from its peers' data and control ports by dropped packets for
20 seconds under the mixed bench
([cluster testing](../../cluster-testing/correctness.md#partition-one-node)). Once
[#143](silent-partition-hops.md) was fixed, the cluster served everything but hyperion's groups
during the partition. After the heal it did not recover. From a few seconds after the heal,
throughput dropped to zero for a second or two at a time, reads included, for about 20 seconds.
Six seconds after the heal, leaders on europa, titan and hyperion all stepped down at once, and
writes timed out at 5 s again. Nothing acknowledged was lost.

## Cause

A cut-off member keeps standing for election. openraft's election timer runs whether or not
anybody answers, so each time a follower on hyperion heard nothing from its leader for an election
timeout, it voted for itself at the next term and asked everyone. Nobody answered, so it did it
again. [#106](isolated-member-term-inflation.md) stopped exactly this for a node whose links are
all *down*: a shard that can reach nobody stops standing. But a partition by dropped packets keeps
every connection up, so `is_isolated` never said so. Each of hyperion's groups rose by a term per
timeout for the whole partition, and so did its control member.

When the packets flowed again, hyperion's vote requests and its answers to the new leaders carried
those terms. A leader that sees a higher term steps down, so each healthy leader on europa and
titan lost its lead to a member that could not win (its log was behind) and could not be ignored.
Each group then had to elect again at the inflated term, with some of them unseated more than
once.

## Evidence

**Reproduced** with `a_silently_cut_node_rejoins_without_elections` in
`shoal/tests/cluster_fixture.rs`. Three nodes at a 1 s failover base, a write through every group,
then node one blackholed on every lane for ten seconds and healed. The test reads every group's
term on node one before and during the cut, and on node zero just before and six seconds after the
heal. Against the tree without pre-vote:

```text
node one's terms before {3932…: 1, 5477…: 1, 9750…: 2, 1358…: 1, 1559…: 1, 1667…: 1}, cut off {3932…: 5, 5477…: 1, 9750…: 6, 1358…: 1, 1559…: 6, 1667…: 5}
node zero's terms before the heal {3932…: 1, 5477…: 2, 9750…: 2, 1358…: 2, 1559…: 1, 1667…: 1}, after {3932…: 6, 5477…: 3, 9750…: 6, 1358…: 3, 1559…: 6, 1667…: 6}
group 3932230016932016001's term on node one went from 1 to 5 while it was cut off
```

Four groups on node one rose four or five terms in ten seconds, and every one of them dragged node
zero's leader to the same term after the heal. The control group rose from term 1 to 43 over the
same ten seconds, at its 150–300 ms timeouts.

With the fix, three runs in a row: node one's group terms and control term unchanged while cut off,
and node zero's terms unchanged or one higher after the heal. One higher is O63 handing a group
back to node one as its placement primary, a transfer rather than an election.

## The fix

Pre-Vote, as the Raft thesis describes it (§9.6), for the data groups and the control group. Before
a member stands at a new term it asks the others whether they *would* grant it. A member grants
only by the same rules as a vote: its leader's lease has lapsed and the candidate's log is at least
as current as its own. A pre-vote never persists a vote and never moves anyone's term. A member
that nobody answers, or whose peers still hear from a leader, gets no quorum of grants and so
never raises its term.

openraft 0.10 carries it, off by default (`Config::enable_pre_vote`), behind a `pre_vote` RPC that
a network has to implement.

- `group_config` (`shard/groups.rs`) and the control plane's config (`control/plane.rs`) set
  `enable_pre_vote: Some(true)`.
- `ReplicateKind::PreVote` (11) on the replication lane and `ControlKind::PreVote` (8) on the
  control lane carry the library's `VoteRequest`. The receivers hand it to `Raft::pre_vote`, which
  answers by the vote rules without persisting anything. On a data group, the empty-volatile-copy
  refusal of [#109](volatile-majority-loss.md) is applied to a pre-vote exactly as to a vote, so
  the two never disagree.
- `GroupPeer::pre_vote` and `ControlPeer::pre_vote` (`replication/network.rs`,
  `control/network.rs`) decide by the link they would send over:
  - **up, with a peer that granted `CAP_PRE_VOTE_V1` at the hello:** send it and return the answer;
  - **up, with a peer that did not** (an older build): grant locally, which is openraft's own
    default for a network without pre-vote, and exactly what the election did before;
  - **not up:** an error, never a grant.

A pre-vote over a link that is up but silent waits out its RPC deadline and fails, which is also
not a grant.

`CAP_PRE_VOTE_V1` is advertised by this build and deliberately left out of
`REQUIRED_CAPABILITIES`, so a node running the previous build is still a member. A rolling upgrade
runs with pre-vote sent only between upgraded nodes, and it covers the whole cluster once every
node is upgraded. Leader transfer (`TransferLeader`, [F45](../../features/replica-migration.md),
[#139](leadership-handoff-on-stop.md), [O63](../optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary))
does not go through a pre-vote: openraft elects the named member directly, since the whole point
is to elect while the old leader's lease still holds.

## Alternatives rejected

- **Extend #106's isolation to silence.** A shard could stop standing when every link has been
  silent for [#143](silent-partition-hops.md)'s threshold, not only when every link is down. That
  covers a node cut off from everyone, but not a node cut off from some peers, or a node that
  hears some members of a group but not its leader. Pre-Vote covers every case, because it asks
  the question that matters (would anyone grant this?) rather than inferring the answer from the
  links. #106 stays: it saves the pre-vote RPCs a node with no links would send.
- **CheckQuorum (a leader steps down when it cannot reach a quorum).** It addresses a different
  failure, a stale leader on the minority side, and does not stop a follower inflating its term.
  openraft's leader lease already keeps followers from voting while their leader is heard from.
- **Bump the wire version instead of a capability.** Every other version change so far altered
  how a body is encoded. This one adds a message kind that a peer either acts on or does not. An
  optional capability says exactly that, and gates the one place the kind is sent
  (`Negotiated::has`), the way `CLIENT_CAP_READ_OPTIONS` gates a client
  ([F48](../../features/rolling-compatibility.md)).
- **Send the pre-vote to an older peer and treat its refusal as a denial.** The older build
  cannot decode the kind and answers with an error. A cluster mid-upgrade could then never elect
  a new leader through its older members.

## Invariants to uphold

- **A pre-vote to a member that cannot be reached is never a grant.** Granting on a failed or
  missing link lets a node cut off from everybody grant itself a quorum, and the term inflates
  as before. Only a peer *known* to lack the capability is granted locally.
- **A pre-vote and a vote are judged by the same rules on the receiver.** Any refusal added to
  the vote arm (such as #109's) belongs to the pre-vote arm too, or a candidate passes the
  pre-vote, stands, and is refused at a term it has already spent.
- **`CAP_PRE_VOTE_V1` stays out of `REQUIRED_CAPABILITIES`** for as long as `MIN_PEER_VERSION`
  admits a build without it.
- **Leader transfer does not pre-vote.** A transfer is meant to win while every follower still
  holds the old leader's lease, which a pre-vote would refuse.

## Still open

- Failover now costs one more round trip, the pre-vote, before a candidate stands. Against a
  lease of the whole `election_timeout_max` that is noise, and the lab measurement is in the
  [partition test](../../cluster-testing/correctness.md#partition-one-node).
- A leader on the cut-off side keeps believing it leads until it hears the higher term. Writes
  proposed to it locally wait out `write_timeout`, which [#143](silent-partition-hops.md)'s
  *Still open* already names.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_silently_cut_node_rejoins_without_elections` (`shoal/tests/cluster_fixture.rs`) | A node blackholed for ten election timeouts raises its group and control terms and unseats the healthy leaders on its return |
| `pre_vote_is_an_optional_capability` (`shoal-proto`) | The kinds move off their bytes, or the capability becomes required and a previous build is refused membership |
| `a_group_config_keeps_the_timers_its_base_derives` (`shoal-core`) | The data groups run without pre-vote, or their config stops validating and falls back to openraft's defaults |
| Partition one node ([cluster testing](../../cluster-testing/correctness.md#partition-one-node)) | Elections and stalls for tens of seconds after a silent partition heals |

## Related

- [Resolved #106](isolated-member-term-inflation.md), the same inflation for a node whose links
  are all down.
- [Resolved #143](silent-partition-hops.md), the partition's first finding.
- [F48](../../features/rolling-compatibility.md), capabilities and rolling compatibility.
