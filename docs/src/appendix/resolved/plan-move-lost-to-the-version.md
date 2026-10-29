# 331. A plan's move that lost every try to the topology version was failed

A capacity plan's steps are issued by the control leader as ordinary moves
([F46](../../features/capacity-rebalancing.md)). A move is a versioned operation: it names the
topology version it was written against, and the state refuses it `StaleVersion` at any other. A
set whose move fails twice under a plan is left blocked by name
([Resolved #177](blocked-plan-retry.md)).

Found and fixed in the flaky-test plan's evidence loop (2026-10-28).

## Symptom

`node_transfer_budgets_bound_concurrent_sources` failed in w9's tenth round with its rebalance
`Completed` and four steps where it asserts three. One step was failed:

```text
"state":{"Failed":{"reason":"the move was refused: Refused { reason: \"stale version: the request was written against topology version 37 and the cluster is at 39\", kind: StaleVersion }"}}
```

The plan planned the same set again as a fourth step, which moved. Nothing was wrong with the move,
but a failed step counts against its set. A second one under load would have left the set blocked by
name, and the rebalance or drain with it, when every move it asked for was sound.

## Cause

The leader proposes a plan's move against `topology_version` as its state has applied it. Every
entry ahead of the move in the log that moves the version makes the move stale. That includes the
moves in flight committing their phases, the plans' own progress, backups and reports. On a busy log
there is nearly always one. The leader reads the version at its applied index, and entries already
appended commit before its own.

F46 met this once and retried a stale move eight times, 50 ms apart (`PLAN_MOVE_RETRIES`). Under load
a proposal took about 135 ms, and the plan's two other moves moved the version seventeen times in
1.5 s. Every try lost, and the last refusal was committed as the step's failure.

## Evidence

**Established by the loop's child logs, then reproduced.** In w9's tenth round, the control leader's
log has the plan's third step issued with the map at version 22. Seventeen versions follow in a
second and a half while the plan's other two moves stream, one every 88 ms:

```text
01:15:39.533495Z INFO …plane: msg="plan progress" plan=e85c701a-… update=Step { tablet: 0, op: Some(d200e821-…), state: Moving }
01:15:39.539135Z INFO …shard: msg="installed a tablet map" version=23 placed=true routes=true members=5
…
01:15:40.820380Z INFO …shard: msg="installed a tablet map" version=37 placed=true routes=true members=5
01:15:40.919111Z INFO …shard: msg="installed a tablet map" version=38 placed=true routes=true members=5
01:15:41.014935Z INFO …shard: msg="installed a tablet map" version=39 placed=true routes=true members=5
01:15:59.174955Z INFO …plane: msg="plan progress" plan=e85c701a-… update=Steps { steps: [PlanStep { tablet: 0, … state: Pending }], blocked: None }
```

The set was planned again 18 s later, once the other two had moved. The logs are in
`target/lab/flaky/runs/w9/round-10/logs`; `child-1776833.log` is the control leader's.

**Reproduced** by `a_plan_move_that_loses_every_try_stays_pending`, written first. It is the weights
row's rebalance, two sets onto a spare, with every node making its next eight proposals of a plan's
move stale (`faults::stale_plan_moves`, the fixture's `STALE_PLAN_MOVES`). Each is written against
the version before the current one, committed, and refused by the state as the race's is. On the
tree before:

```text
a step failed: {…,"outcome":{"Completed":{"bytes":14632,"moved":2}},"phase":"Done",…,"replanned":2,…,"steps":[{…,"state":{"Failed":{"reason":"the move was refused: Refused { reason: \"stale version: the request was written against topology version 14 and the cluster is at 15\", kind: StaleVersion }"}},"tablet":1,…},{…,"state":"Moved","tablet":0,…},{…,"state":"Moved","tablet":1,…}]}
```

## The fix

**A plan's move whose every try was refused `StaleVersion` leaves its step pending.** The refusal is
the state refusing the entry outright, so no move was recorded and nothing is committed for the
step. The plan's one proposal in flight is released, logged at INFO. The next pass of
`drive_plans` issues the step again under a fresh move id, with eight more tries. A pass comes a
quarter second after the next state change, or at the plan interval. A refusal of any other kind
still fails the step.

## Alternatives rejected

- **Judge a plan's move at apply, with no version.** `apply_move` checks everything a move depends
  on against the state it applies to: the source in the set as served, the destination up and
  outside it, and a move of the same set queued behind another. The retry already takes whatever
  version is current, so the version guards nothing for the leader's own moves. The leader's other
  orders skip the check by a nil op (`ScrubNow`, a device's failure), and a device's drain checks
  none. But a move's op is its identity, which the step records to follow the move, so it cannot be
  nil. Any other mark is a change to what a committed entry means, and a node on an older build
  would refuse the entry its newer peers apply: the control state would diverge. It needs a wire
  version of its own, filed on [Todos](../todos.md#a-plans-move-judged-at-apply).
- **Retry without a bound inside the proposal.** The plan holds its one proposal in flight while it
  waits, and nothing else of it moves. Its moves done are not marked and its next step is not
  issued.
- **More tries, or a longer pause.** Each try loses to whatever commits during its proposal, and how
  many lose in a row is the log's business. A step failed after any count is the defect again.

## Invariants to uphold

- **A plan's step fails only on a refusal of its move**, never on the version moving under the
  proposal.
- **A step is left pending only where nothing was recorded.** A `StaleVersion` refusal records no
  move. An error from the proposal, or an answer lost with the lead, may have recorded one and is
  not this path.
- **The fault costs one load while unarmed** (`stale_this_plan_move`).

## Still open

- **A version that moves faster than a proposal's round trip, for as long as a plan runs, holds its
  step pass after pass.** Nothing bounds it, and the plan is not blocked by it. The plan's moves
  finishing is what quiets the log: in w9, the versions stopped seven seconds after the moves began
  streaming.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_plan_move_that_loses_every_try_stays_pending` (`cluster_fixture`) | The first step's move is failed `StaleVersion` and its set planned again as a third step |
| `node_transfer_budgets_bound_concurrent_sources` (`cluster_fixture`) | Under load, the third move loses every try to the other two's progress and the plan records four steps |

## Related

- [F46](../../features/capacity-rebalancing.md), the plans and the first retry of a stale move.
- [Resolved #177](blocked-plan-retry.md), the two failures that block a set.
- [Resolved #195](scheduled-scrub-starved.md), a node's scheduled scrubs starved by version churn.
- [Tests That Fail Under Load](../flaky-tests.md).
