# 100. `duplicate_node_identity_is_fenced` failed under the fixture suite at full parallelism

## Symptom

The M3 fencing test passed alone in under two seconds, passed at `--test-threads 6`, and at
the default thread count under the whole suite failed every time with `the clone kept running`:
a clone started from a copy of node one's directory at the same incarnation as the original,
from other ports, was not refused within its sixty-second wait. The item left open which of
three it was - the clone never ready, never joined, or joined and admitted - and the suite has
been run at six threads since.

## Cause

None of the three. The clone was ready, was never admitted, and never *asked*.

A member that restarts from its own directory is `Recovering`: it does not join through the
seeds, it observes itself through the leader with `ObserveMember`, and the leader's state
machine fences it if the cluster already holds its incarnation from another address. That
observation was gated on the member's own control group naming a leader - `maybe_observe`
returned while `self.leader` was `None` - and `propose` found the leader through the local
group alone: the hint `client_write` returned, else `current_leader` from the metrics, else a
three-second wait on those metrics.

On the clone's restart the loaded vote is committed, so the metrics name the old leader at
once and alone the observation is proposed and answered `Fenced` in milliseconds. But the vote
is loaded with **no lease**, and nothing refreshes it: the leader dials a member at its
*committed* control address, which is the original's, so no heartbeat ever reaches the clone.
Within one election timeout of loading - 150 to 300 ms for the control group - the clone stands
for election, its vote becomes its own uncommitted one, `current_leader` is `None` from then
on, and the two voters it needs never include the leader, whose lease is fresh. If the
observation had not been proposed by then, it never was: the gate in `maybe_observe` held it
back for a leader the clone's own metrics would never name again, and a proposal already in
flight waited three seconds on the same metrics and gave up. Under a loaded machine the child's
control thread does not reach the proposal inside 150 ms, so the clone ran on, a candidate at
an ever higher term, refused every vote, and fenced by nobody.

The original does not meet the same race because the leader replicates to *it*: an
`AppendEntries` at the leader's term reaches it however long it has stood, its vote is
committed again, and the observation goes through.

## Evidence

**Reproduced deterministically.** `a_clone_that_stands_before_it_observes_is_fenced` in
`shoal/tests/cluster_fixture.rs` starts the clone with its first observation held back two
seconds - several election timeouts - through `control::plane::hold_observe`, which the
fixture carries as `ChildOverrides::observe_hold_ms`. Against the tree at `16f9a53` with the
gate in `maybe_observe` already removed, so that the observation was at least attempted:

```text
thread 'a_clone_that_stands_before_it_observes_is_fenced' panicked at shoal/tests/cluster_fixture.rs:2934:10:
the clone kept running: it stood before it observed and never found the leader
```

The clone's child log shows it a `Candidate` from term 4 to term 38 over the thirty seconds,
and eight times `the observation did not commit: no control leader could be reached`.

**The suite at the default thread count on this host is another matter.** Run twice at
thirty-two threads after [Resolved #102](fixture-port-block.md): 61 and 82 of 98 tests failed,
the fencing test among them once - with `shard 0 failed: exited before reporting ready`, not
with the clone running on - and the children's stderr carries thirty-five copies of glommio's
`Failed to register a probe` from `io_uring_get_probe` returning null at executor start. That
is the host refusing a few hundred io_uring instances at once, and it takes the shards and the
control threads of every test with it, this one included. The item's `fails every time` at
full parallelism was recorded at F40 on this host with fewer tests in the suite; whether that
run's failures were the race above or the probe is not known, and the race is what the code
reading found and the test reproduces.

## The fix

Two halves in `shoal-core/src/server/control/plane.rs`:

- `maybe_observe` no longer waits for the local group to name a leader. A recovering member
  observes as soon as it may, and after the backoff when it could not.
- `propose` asks the committed members where the leader is when the local group names none:
  `members_to_ask` lists every other member the state holds up and placeable, and the hop
  loop takes one as its hint after the local hint and the metrics, before waiting on the
  metrics. A member that is not the leader answers `NotLeader { leader }`, which is the hint
  the next hop follows; a member that cannot be reached is skipped while another remains.
  The clone's proposal therefore reaches the leader through the link the clone dialled, which
  the leader accepts at the committed incarnation, and the state machine answers `Fenced`.

`hold_observe` is the test hook: a process-wide hold on the first observation, zero by
default, armed by the fixture child before its pool starts the way a rehome crash point is.

## Alternatives rejected

**Fence at the door: refuse a hello at the committed incarnation from another control
address.** The clone's link would be refused and the clone would end on the refusal. But a
member restarted at a new address arrives at the door at the committed incarnation from another
address too - its bumped incarnation is not committed until it observes - so the door cannot
tell a clone from a move, and [F50](../../features/cluster-operations.md)'s address change
would be refused with it. The state machine can tell them apart, which is why fencing lives
there.

**Keep a recovering member from standing until it has observed.** A whole-cluster restart is
every member recovering at once; a group in which nobody stands elects nobody, and nobody
observes.

**Raise the fixture's readiness timeout.** The clone was ready; it was the fence that never
came, and no timeout reaches a member that never asks.

## Invariants to uphold

- **A recovering member's observation depends on no leader of its own.** `maybe_observe` is
  gated on status, an observation in flight and the backoff, and nothing else; `propose` finds
  the leader through the members when the local group cannot.
- **A member asked for the leader answers with the leader it knows, or none.**
  `handle_propose`'s `NotLeader { leader }` is what the hop follows; a member that answered
  with a stale leader is answered by that leader's own `NotLeader`, within `PROPOSE_HOPS` plus
  the members asked.
- **The fence is the state machine's.** An equal incarnation from another control address is
  `Fenced` in `observe`, and the door admits it so the proposal can arrive.

## Still open

- A member standing before it observes still inflates its term until it is fenced or fed,
  which is [item 106](../known-issues.md#106-a-member-isolated-on-every-lane-long-enough-to-inflate-its-term-trips-an-openraft-debug-assertion-when-healed)'s shape
  on a shorter fuse.
- The fixture suite at thirty-two threads on this host is not a measurement of anything but
  io_uring's limits; it stays at six threads, for that reason rather than for this item.
- Found on the way and filed: `certificate_rotation_binds_identity` fails about half its runs
  alone on this host, on which of two members leads when node zero restarts refusing one of
  them ([item 112](../known-issues.md#112-certificate_rotation_binds_identity-fails-about-half-its-runs-alone-on-the-development-host)).
  And `node_transfer_budgets_bound_concurrent_sources` allowed one full bucket for three
  sources and passed on the margin until this change moved the timing around it; its bound
  now counts a bucket per source that has streamed, which is what the budget is.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_clone_that_stands_before_it_observes_is_fenced` | `shoal/tests/cluster_fixture.rs` | The clone runs on unfenced for the whole wait, a candidate at a climbing term |
| `duplicate_node_identity_is_fenced` | `shoal/tests/cluster_fixture.rs` | The same, at whatever odds the machine's load gives the race |
| `address_change_is_observed_and_a_stale_clone_is_fenced` | `shoal/tests/cluster_fixture.rs` | A moved member's observation goes through the members it can reach |

## Related

[F39. Membership](../../features/membership.md), the observation and the fence;
[F50. Cluster operations](../../features/cluster-operations.md), the address change the door
must admit; [Resolved #102](fixture-port-block.md), the other suite-load failure;
[item 106](../known-issues.md#106-a-member-isolated-on-every-lane-long-enough-to-inflate-its-term-trips-an-openraft-debug-assertion-when-healed).
