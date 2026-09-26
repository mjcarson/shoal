# 177. A plan blocked by failed moves could not be tried again

## Symptom

Rebuild 7 of [section 8](../../cluster-testing/correctness.md#8-an-unplaced-member-coordinates)
ended with its plan blocked:

```text
b52c4467 remove Blocked 17/19 moved - blocked: tablet 1: its move failed 2 times
```

Both failures were [#174](snapshot-cut-queue.md)'s cut queue. One set was left with the removed
identity still one of its three copies, one short of the factor. Asking for the removal again was
accepted and did nothing:

```text
sending "remove 1eab2e99… f2519d71…" as 50c262fe…
accepted: Applied { version: 1864 }
Error: no plan 50c262fe… is recorded (Internal)
```

The plan stayed blocked. Nothing an operator could send would plan that set again.

## Cause

A set whose move fails `REPLAN_FAILURES` (two) times under a plan is left blocked by name. That is
right when the failure is the set's own. But `PlanRecord::failures_of` counted every failure the plan
had ever recorded, and a decommission or removal asked again for a member already leaving or
removing was answered `Applied` with nothing changed. There was no other operation that clears a
plan's failures.

## Evidence

**Found on the lab, then pinned in a test.** The lab exchange above.
`asking_again_retries_a_plan_its_failures_blocked` (`shoal-core/src/server/control/types.rs`) builds
a decommission plan blocked by two failed moves of one set, and asks for the decommission again.
The old code answered `Applied` at the same version and left the failures counting.

## The fix

- **Asking the same decommission or removal again retries the member's plan**
  (`ControlState::retry_plan_for`). If the plan is blocked, its failures so far stop counting:
  `PlanRecord::retried_from` moves past every step taken, and the topology version moves. The
  leader's next pass then plans the blocked sets again. A plan that is not blocked is left as it is.
- **`cluster admin` follows the retried plan.** When the operation it sent recorded no plan of its
  own, it follows the single open plan of the same kind.

## Alternatives rejected

- **A separate `retry <plan>` operation.** It would be one more verb for what the operator already
  typed. Asking for the same thing again is what an operator does, and it was already accepted.
- **Raise or remove `REPLAN_FAILURES`.** A set that fails for its own reason would then be retried
  for ever. The bound is right; forgiving it has to be a decision.

## Invariants to uphold

- **Only the operator forgives a failure.** Nothing in the leader resets `retried_from`.
- **The failures that block a set are counted from the last retry.** Every past step stays in the
  record, for its history.

## Still open

- The set blocked on the lab could not be tried again with this build, because `cluster upgrade`
  refuses while a set is under the factor. It was retried after the build was installed by hand:
  see section 8.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `asking_again_retries_a_plan_its_failures_blocked` (`shoal-core/src/server/control/types.rs`) | A decommission asked again leaves its blocked plan blocked, or a failure after the retry does not count |
| `a_member_is_decommissioned_removed_and_tombstoned` (same file) | Asking again changes a plan that is not blocked |

## Related

- [F46](../../features/capacity-rebalancing.md), plans and their blocked reasons.
- [#174](snapshot-cut-queue.md), whose stalls caused the failures.
