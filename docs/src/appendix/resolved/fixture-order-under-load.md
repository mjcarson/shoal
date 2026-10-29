# 234, 293, 294. Fixture rows expected an order of events a loaded host does not keep

Each of these rows assumed that things would happen in an order an idle host keeps and a loaded one
need not: two streams at once, a group with a leader for every write, a plan step moving at its
first try. What the engine did was right each time; the rows now arrange the order they need, or
accept the order the engine's design allows.

Filed from [F83](../../features/rack-and-zone-domains.md)'s run (234, 2026-10-12) and from
[Tests That Fail Under Load](../flaky-tests.md) (293 and 294); fixed in the flaky-test plan's second
wave (2026-10-27).

## Symptom

- **234**, `automatic_removal_and_rejoin_preserve_fencing`: the expiry plan completed with every set
  moved, and its record held a step `Failed` with *stale version: … written against topology version
  40 and the cluster is at 41* beside the step that moved the same set. The row asserted every step
  `Moved`.
- **293**, `node_transfer_budgets_bound_concurrent_sources`, three ways:
  - in F82's run *the spare received 1,589,450 bytes in 1.6s, over the budget's bound of 1,507,453*;
  - in F86's a setup batch *elected no leader within the deadline*;
  - in F95's and W4's *the second stream was never refused*, one stream installing at a time.
- **294**, `a_routed_client_follows_the_leader_it_is_told_of`: a routed write refused `NotLeader`,
  *group … is electing …; nobody leads it here yet*, which the row returned as its failure, in two
  runs.

## Cause

- **234.** A plan step proposes its move against the topology version it read, and a stale version
  is proposed again eight times 50 ms apart. A version moving faster than that, here the expired
  member's removal and the spare's promotion landing beside the moves, spends the tries. The step is
  recorded `Failed`, and the planner plans the set again, since its failures are under
  `REPLAN_FAILURES`. That is the design's recovery, and the row read it as a failure.
- **293.**
  - The rate bound counted the streams the spare had *installed*, plus one. A stream's install is
    counted only once it ends, after its last byte, so a second source's first bucket could arrive
    while the first stream still counted as none installed. The bound ran one bucket short.
  - The refusal the row expected needs two streams assembling at once. Nothing arranged that: the
    three moves begin together on an idle host, and under load their streams ran one after another.
  - The setup batch met an election and returned its refusal as the row's failure.
- **294.** A client retries only within the budget its caller gives it, and with none it tries once
  (F42). The row's writes had none, so a write meeting a group mid-election was the row's failure.

## Evidence

**Established by reading the source and the runs' messages.** For 293, the spare's figures said
one stream at its peak and none refused when the refusal was missed. For the bound, 1,589,450 bytes
at 1.6 s is more than one sender's bucket and rate, 512 KiB plus 1.6 s at 512 KiB/s, and less than
two buckets and the rate. 294's message names an election, and 234's record names the replanned
step.

## The fix

- **234**: ~~the row judges each tablet the plan named. It must have a `Moved` step, and any other
  step of it may only have failed for a stale version and been planned again.~~ The plan's outcome,
  the sets held and the tombstone are asserted as before. ~~The retry budget measured in time that the
  item suggested is left open.~~ Since [Resolved #331](plan-move-lost-to-the-version.md) a step whose
  move lost every try stays pending, so the row asserts every step `Moved` again.
- **293**:
  - **The spare counts the streams it began** (`SnapshotStats::begun`, a figure beside
    `refused_budget`, every begin it took), and the rate bound counts sources from it.
  - **Each source holds its first stream before its chunks**, six seconds (`HOLD_STREAMS`,
    `faults::hold_streams`). The stream stays short of its bytes on the spare, against its budget, so
    the other sources' begins meet the budget full and are refused, whatever the load.
  - **The setup's batches are sent again until they land.**
- **294**: the row sends each write with a retry budget of ten seconds, as a client that wants it to
  land does. A write refused mid-election follows the new leader on its next try, and the hop bound
  stands.

## Alternatives rejected

- **Throttle the links into the spare** (293). It would make the streams long enough to overlap,
  but the senders would then wait on the link rather than on their budget, and the row asserts that
  they waited on their budget.
- **Lower the sender's budget so streams last longer** (293). Overlap would be likelier, not
  arranged.
- **Retry inside the client by default** (294). A client's retry is its caller's choice, so that a
  caller that counts its tries sees each one.

## Invariants to uphold

- **A row that needs two things at once arranges it**, rather than counting on timing.
- **A row counts a recovery the design makes as success**, and asserts the outcome the design
  promises.
- **A stream begun counts against its sender's first bucket** in any bound on what a receiver takes.

## Still open

- ~~**234's engine half:** a plan step's retries for a stale version are eight tries 50 ms apart, which
  a version moving fast under churn spends. A budget in time, the plan's own interval, would ride it
  out without a replan. Recorded in [TODOs](../todos.md#a-plan-steps-stale-version-retries-in-time),
  not built: the replan costs a set's move again only when its first step failed.~~ Closed by
  [Resolved #331](plan-move-lost-to-the-version.md): a failed step counted against its set, and two
  block it, so a move that lost every try now leaves its step pending for the next pass.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `automatic_removal_and_rejoin_preserve_fencing` (`cluster_fixture`) | ~~Under churn, a replanned step's first, failed try fails the row~~ Since #331 it asserts every step `Moved`, so a stale step failed and planned again fails it |
| `node_transfer_budgets_bound_concurrent_sources` (`cluster_fixture`) | Without the hold, under load, no second stream is refused; with the bound on installs, a second bucket passes it; without the retries, a setup batch fails the row |
| `a_routed_client_follows_the_leader_it_is_told_of` (`cluster_fixture`) | Under load, a write mid-election fails the row |

## Related

- [F46](../../features/capacity-rebalancing.md), the stream budget and the planner's recovery.
- [F42](../../features/primary-failover.md), a client's retry budget.
- [Tests That Fail Under Load](../flaky-tests.md).
