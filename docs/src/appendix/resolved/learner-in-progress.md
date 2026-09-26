# 178. A move failed if its learner was added while a membership change was still committing

## Symptom

The workspace suite run at the end of section 8's work failed
`migration_resumes_after_each_phase_failure` at its "control leader at reconfiguring" round:

```text
left: Object {"Failed": Object {"reason": String("adding 02a1ab50…/0 as a learner of group
  d915f961cde6acb4: the cluster is already undergoing a configuration change at log Some(LogId {
  leader_id: LeaderId { term: 7, …")}}
right: String("Moved")
```

The move ended `Failed` when it should have finished, after the test killed the control leader
part way through a set's reconfiguration.

## Cause

`add_learner` (`shard/migrate.rs`) failed the group's move on any error other than a hand-off to
another leader. That includes openraft's `ChangeMembershipError::InProgress`, which a leader answers
while a membership it or its predecessor proposed is still uncommitted. A group whose leader has just
changed can be committing the joint configuration of the move being resumed. Adding the learner then
meets `InProgress` and fails. [F45](../../features/replica-migration.md) says `InProgress` "is waited
for, never failed", and the reconfiguration step did wait for it. The learner step did not.

## Evidence

**Established by running it, and from the source.** The failure above is from the suite run, which
the test's fault schedule reaches intermittently. The test's other intermittent failure, openraft's
`log_state_reader` assertion, is [item 142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s
and predates this work.

## The fix

`add_learner` waits out `InProgress` the way the reconfiguration step does: it tries again every
poll, up to the move's timeout, and only then fails.

## Alternatives rejected

- **Fail, and let the plan replan the set.** A plan retries a failed set, but a move asked without a
  plan, or one past `REPLAN_FAILURES`, would stop on a condition that clears in milliseconds.

## Invariants to uphold

- **Every membership step of a move waits out `InProgress`, and none fails on it.**

## Still open

- `migration_resumes_after_each_phase_failure` still fails intermittently on openraft's own
  assertion ([item 142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `migration_resumes_after_each_phase_failure` (`shoal/tests/cluster_fixture.rs`) | A move resumed after a control leader is killed mid-reconfiguration fails adding its learner, some runs |

## Related

- [F45](../../features/replica-migration.md), the move's phases.
