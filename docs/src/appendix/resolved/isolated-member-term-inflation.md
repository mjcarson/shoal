# 106. A member isolated on every lane long enough to inflate its term tripped an openraft debug assertion when healed

## Symptom

A cluster node cut off on every lane - control and data, both directions - kept electing: the
control group's member on it timed out, voted for itself at a higher term, was answered by
nobody, and did it again, so after a minute of isolation its term was far above the survivors'.
When the lanes were healed the survivors' leader reached it and the member's engine took the
following path with a vote that was its own uncommitted one, which openraft asserts is
committed, so the control thread panicked and the child died. That was the first shape of
[F43](../../features/node-recovery.md)'s retention test, which cut the data lanes alone from
then on. The same inflation happens to every tablet group on the node, and a healed member's
term above the leader's makes the leader step down, so a heal cost an election on every group
the isolated node was in even when nothing asserted.

## Cause

Nothing stopped a member from standing when it could reach nobody. openraft's election timer
runs whether or not a single peer is reachable; a candidate that gets no answer times out at the
next term and asks again, and there is no pre-vote round in the configuration to notice that
nobody would grant. The term inflated by one per election timeout - a hundred and fifty to
three hundred milliseconds for the control group - and everything the item describes followed
from the term, not from any state the node's runtime let the engine reach.

## Evidence

**Reproduced.** `a_member_isolated_on_every_lane_heals_without_dying` in
`shoal/tests/cluster_fixture.rs`: three nodes with every lane through a proxy, node two
isolated for twelve seconds under writes through node zero, then healed; its control term read
before, during and after through the topology view, which now carries it. Against the tree at
`b626b99`:

```text
node two's control term: 1 before, 52 isolated, 54 healed
thread 'a_member_isolated_on_every_lane_heals_without_dying' panicked at shoal/tests/cluster_fixture.rs:9070:5:
node two's control term went from 1 to 52 while isolated
```

Fifty-one elections in twelve seconds, and the survivors' term pushed from 1 to 54 by the
heal. The child did not die in that run: the assertion the item quotes was met once, after
sixty seconds of isolation, and whether it fires is the engine's timing against the heal.
With the fix, twice: `1 before, 1 isolated, 1 healed` - no election asked for, no leader
disturbed, and a write through node two served within a second of the heal.

Whether the assertion is openraft's bug or a state this runtime lets the engine reach was the
item's first question. The trace is not in hand and the state is no longer reachable: a
member that cannot reach anybody no longer holds a self-vote of its own for the leader's
append to meet, so the answer is left as the item left it and the path is closed from this
side.

## The fix

A member that can reach nobody stops standing for election, and stands again when it can.
`PeerNetwork::is_isolated` and `ShardNetwork::is_isolated` say whether a node has dialled at
least one peer over that lane and none of its links is up - cut, reconnecting or backing off.
The control loop judges it on every report tick and every ping tick and flips
`runtime_config().elect` once per transition, with a warning on the way down and a note on the
way up; every shard judges its replication links on its deadline tick and flips `elect` on
every group it hosts. A member alone cannot win an election; what this removes is the term it
would have spent asking. The topology view carries the control member's `term`, beside the
`term` [Resolved #103](returning-leader.md) put on every group's report, so a test reads what
an isolation cost.

## Alternatives rejected

**Pre-vote.** The textbook answer: a candidate asks whether it would be granted before it bumps
its term. openraft has the switch, but neither of Shoal's networks implements the RPC and the
default implementation grants, so the switch alone changes nothing; a `PreVote` frame on both
lanes is a wire change gated on the peer version, for the same outcome the link state gives
without one.

**Judge isolation from the ping table.** The pings miss at their interval, a second; the links
go down the moment their connections do, and an election timeout is shorter than a ping.

**Stop standing after a bound on lost quorum, as the item suggested.** A bound in time is a
second timer beside openraft's with nothing to say that the link state does not say sooner.

**Pin an openraft fix for the assertion.** The assertion was never traced to a line that is
wrong, and it is unreachable from a member whose term did not inflate.

## Invariants to uphold

- **A node with no link is not isolated; a node whose every link is down is.** A fresh node
  before it dials, and a single-node cluster, elect as they always did; a node whose peers are
  all dead does not stand, which it could not have won.
- **`elect` is flipped on transitions only**, once each way, by one judge per lane: the
  control loop for the control member, each shard for its groups. The head start
  `start_group` gives a non-primary is a separate switch of the same flag; a heal during a
  head start ends it early, which costs a primary its first lead and nothing else.
- **A member's term is what the elections cost.** The topology view's `term` and the group
  report's `term` are read by the tests of this page and of [Resolved #103](returning-leader.md).

Found on the way, and corrected in the same change: [Resolved #109](volatile-majority-loss.md)'s
markers were scanned on every rebuild rather than once at the shard's start, so a fresh
cluster's bootstrap took its own single-member groups for ones that had lost their memory and
stalled; and two of the wire-version tests waited for a *restarted* node's own links, which a
node that leads nothing after coming back - a returning leader waiting out its lease, an empty
volatile copy waiting to be fed - no longer has, so they wait for the links the others made to
it, which is what the node came back on.

## Still open

- A node partitioned from *some* members still stands and still inflates, more slowly; only a
  node that can reach nobody is gated. Pre-vote would cover the partial case and is filed.
- The assertion's owner is unanswered, as above.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_member_isolated_on_every_lane_heals_without_dying` | `shoal/tests/cluster_fixture.rs` | The isolated member's control term climbs by dozens, and the heal costs the survivors an election or the child |
| `strong_read_refuses_isolated_old_primary`, `uncommitted_suffix_never_enters_checkpoint`, `one_reads_converge_without_exposing_uncommitted_state` | `shoal/tests/cluster_fixture.rs` | An isolated member that must still serve `One` reads and refuse strong ones does not, or a healed member does not converge |
| `quorum_loss_is_unavailable_without_data_loss`, `minority_cannot_commit_membership_changes` | `shoal/tests/cluster_fixture.rs` | A member whose peers are all cut elects, or one whose peers come back cannot |

## Related

[F39. Membership](../../features/membership.md), the control group and the pinger;
[F43. Node recovery](../../features/node-recovery.md), whose retention test met this;
[Resolved #103](returning-leader.md), the other term this session stopped inflating;
[C3. Membership](../../distributed/membership.md).
