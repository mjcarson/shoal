# 195. A node's scheduled scrubs were refused for a stale version, every time and silently

Filed and fixed in one change, from round 15 of the lab testing: the loop of the fixture's six
heaviest tests that [#142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)
is followed with ([#142 in round 15](../../cluster-testing/correctness.md#142-in-round-15)).

## Symptom

`scheduled_scrub_quarantines_without_an_operator` failed in the sixth round of the loop, after 35
passes:

```text
Error: NotReady("node 1's committed quarantines never became true: {... \"quarantined\":[], ...
```

The test forgets a partition on node one and waits 40 s for a scheduled scrub to find the
divergent copy. The fixture's child logs showed the forget done, and every scrub judged after it
`Clean`: of the table's three groups, two were scrubbed every four seconds for the whole test, and
the third, `a24e6647d4cd4c84`, was never scrubbed once. Its leader logged
`proposing a scheduled scrub` every four seconds, and no node logged a digest or a judgement for
it. The forgotten partition was in that group.

## Cause

A scheduled scrub is a `ControlCommand::Repair` in `Verify` mode, which a group's leader proposes
through the control plane on its interval (`Shard::schedule_scrubs`), written against the
topology version of the node's own map. The control state refuses any `Repair` whose
`expected_version` is not exactly its current version (`check_version`), and every repair record
and every progress report a driver commits moves that version.

Three leaders each scrubbing every four seconds moved it several times a second. A node whose map
reached it a moment after the others wrote each scrub against a version already passed, and was
refused `StaleVersion`, every time. The scheduler dropped the reply unread, so nothing said so,
and the node's groups were never verified.

## Evidence

**Established by reproducing it** in the fixture loop (`target/lab/r15/142/loop-keep.sh`), one
failure in 36 runs of the six tests, with the child logs kept: the starved group's leader proposed
a scrub every four seconds from 01:32:26 until the test gave up at 01:33:34, and none of them was
applied anywhere, while the other two groups' leaders were scrubbed every four seconds throughout. The new unit test, against the old
check, refuses the scheduler's scrub `StaleVersion`.

## The fix

**A scheduled verify scrub is proposed at `ANY_VERSION` and judged against no version.** A
`Repair` in `Verify` mode derives its groups from the control state it applies to and changes no
placement, so the version guards nothing for it. The control state skips the check for it alone;
a `Repair` that installs, and every other command, is judged against the version it was written
from as before. **The scheduler reads its reply**, and says when a scheduled scrub is refused or
not proposed.

## Alternatives rejected

- **Retrying at the refusal's version.** Under the same churn the retry races the same way; it
  shortens the starvation without ending it.
- **Dropping the check from every `Repair`.** An operator's repair installs copies, and the
  version is how it is kept from acting on a placement that moved under the operator.
- **A slower scrub interval.** The fixture's four seconds only made it likely; any interval shorter
  than the churn it causes starves a lagging node sometimes.

## Invariants to uphold

- **A command the cluster issues to itself on a timer is never written against a version it cannot
  keep up with.** Optimistic concurrency is for an operator's view.
- **`ANY_VERSION` is honoured only by a command that changes no placement.**
- **A background proposal reads its answer.** A refusal that nobody reads is a feature that has
  silently stopped.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `control::types::tests::a_scheduled_verify_scrub_is_judged_against_no_version` | The scheduler's verify scrub is refused once the version moves, and a repair at `ANY_VERSION` would not be |
| `scheduled_scrub_quarantines_without_an_operator` under the loop | A lagging leader's group is never scrubbed, and its divergent copy never quarantined |

## Related

- [F44](../../features/repair.md), scrubs and repairs.
- [#142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host), whose loop found it.
