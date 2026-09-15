# 103. A returning leader was refused its own re-election until its old lease lapsed, and hops to it waited

## Symptom

A tablet group's leader killed and started again before `election_timeout_max` - twice the
failover base - had passed asked for its old term back, since its persisted vote named itself,
and every follower refused it: their lease of that same leader had not lapsed. It asked again
at a higher term, was refused again, and its groups were led only once the lease lapsed and an
election ran. Meanwhile a write from another node that still named it as leader hopped to it,
landed on a member that was `Electing`, and waited for a leader within the forwarded deadline
rather than being refused - so a client saw whole seconds with no answer instead of a burst of
`NotLeader` it could retry elsewhere. The failover arm's smoke runs showed it: zero completed
operations for most of the seconds after the restart, with the write tail at the forwarded
deadline, and the restarted node's child log full of `reject vote-request: leader lease has
not yet expire`.

## Cause

Two halves. `propose_through`'s `Lease::Electing` arm waited on the handle's metrics for a
leader for the whole remaining deadline whether the proposal was the client's own or a hop from
another node, and a hop has somewhere better to be: the node that sent it can ask again once
its hint moves. And `start_group` gave every non-primary a head start at first start - `elect`
off for two election timeouts, so the placement primary leads first - and nothing to a member
restarted with a vote naming itself: with `enable_leader_restore` off openraft demotes that
vote to an uncommitted one at startup, its lease is loaded as zero, and the member stands at
its first tick, at the next term, into followers whose lease of it runs for two bases yet.

## Evidence

**Reproduced.** `a_leader_restarted_inside_its_lease_stalls_no_hop` in
`shoal/tests/cluster_fixture.rs`: three nodes at a base of one second and a five second
deadline, node one killed and restarted at once, a key it led written through node zero
without a retry every tenth of a second until it lands. Against the tree at `fc12d80` with
the two changes neutralised, twice:

```text
a write hopping to the returning leader was held for 2.011152419s: Ok(0x790610000cb0)
a write hopping to the returning leader was held for 2.877397642s: Ok(0x71f14c001a70)
```

The first write was answered - it succeeded - but only once the survivors had elected, held
on the returning node's electing member for two to three seconds; and the group's term went
from 1 to 3. With the fix, three runs: twenty to thirty-one writes refused `NotLeader`, the
slowest refusal in 54 ms, the first success 2.1 to 3.2 seconds after the restart, the term at
2 - one election, the survivors' - and the restarted node's log carrying `a returning leader
waits out its old lease before standing` and no vote request sent.

## The fix

- A hop (`may_hop == false`) that lands on a member whose group is `Electing` is answered
  `NotLeader` at once, naming the member; a client's own proposal on the node it reached still
  waits for the election, since the client is already at the only node it knows.
- A member restarted with a vote naming itself as the leader of a group of more than one voter
  starts with `elect` off for one lease length, `election_timeout_max`, and on again after it.
  The vote is read from the metrics rather than `current_leader`, which openraft leaves empty
  for a demoted self-vote. The survivors' election runs undisturbed at their own timeout; the
  returning member neither bumps the term nor is refused.
- `GroupReport` carries `term`, so a test can read what an election cost.

The failover window itself is what it was - the followers' lease, then their timeout, three
to four times the base ([Resolved #110](dead-primary-write-failures.md)) - and is not this
item's.

## Alternatives rejected

**Clear the persisted vote on restart, so the member starts as a plain follower.** It is the
same outcome with a write to the WAL and a vote openraft would otherwise have had; the runtime
switch does it without touching what is on disk.

**Answer every `Electing` proposal `NotLeader`, hops and the client's own alike.** A client
with no retry that reached the one node it knows would then be refused where it used to be
served after the election. The hop is the case with somewhere else to go.

**A leadership transfer back to the returning leader.** openraft's `leadership_transfer`
bypasses the lease, and only the current leader can start one; there is none while the
followers wait, and moving the lead back is the follow-up F42 filed and this page does not
build.

## Invariants to uphold

- **A hop onto an electing member is a refusal, not a wait.** The forwarded deadline is the
  origin's to spend; a member that cannot commit says so.
- **A returning leader stands only after one lease length.** The head start is
  `election_timeout_max`, which is the followers' lease of it; shorter and it is refused for
  nothing, longer and a group whose survivors cannot elect waits on it.
- **`term` in the report is the copy's current term.** A test that reads it after an election
  expects the survivors' one step, plus at most one for a split vote.

## Still open

- The failover window is lease plus election. The returning leader no longer lengthens it;
  nothing shortens it.
- The read barrier's `Electing` wait (`shard/reads.rs`) is unchanged: a barrier waits for a
  leader within its deadline on the node it reached, and a hop for a barrier is the lane's,
  which was `NotLeader` on an unwritten frame since F42.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_leader_restarted_inside_its_lease_stalls_no_hop` | `shoal/tests/cluster_fixture.rs` | A write hopping to the returning leader is held for seconds rather than refused, or the group's term climbs by more than the survivors' election and a split vote |
| `a_dead_primary_fails_writes_only_until_its_election` | `shoal/tests/cluster_fixture.rs` | The refusal to a dead leader's group stops being immediate |
| `cluster_needs_no_external_coordinator`, `whole_cluster_restart_preserves_durable_history`, `quorum_history_survives_repeated_elections` | `shoal/tests/cluster_fixture.rs` | A restarted leader that must be able to lead again does not, or a whole cluster restarted elects nobody |

## Related

[F42. Primary failover](../../features/primary-failover.md), which filed this;
[C7. Failover](../../distributed/failover.md); [Resolved #110](dead-primary-write-failures.md),
the lease seen from the client; [item 106](../known-issues.md#106-a-member-isolated-on-every-lane-long-enough-to-inflate-its-term-trips-an-openraft-debug-assertion-when-healed),
the term inflation this half also stops for a returning leader.
