# 335. The scrub cursor row compared one copy's cursor with another's

`scrub_resumes_from_its_cursor` (`shoal/tests/cluster_fixture.rs`) holds S11's resumption: a light
scrub held between its placement groups, the node leading a group whose pass is under way killed,
and the group's new leader going on with the same pass from the committed cursor
([F89](../../features/scrub-and-repair.md)). It read the cursor at the leader, killed the leader,
and asserted that another node's copy never read below it.

Found in the flaky-test plan's first workspace run after the confirmation loops (2026-10-28), its
second failure, the first in F91's run with no message kept.

## Symptom

```text
thread 'scrub_resumes_from_its_cursor' panicked at shoal/tests/cluster_fixture.rs:31232:13:
the cursor went back: 2 after 3
```

## Cause

The row compared two copies' applied state. A leader applies a step once a quorum holds it, and its
followers apply it once they learn the commit, from the leader's next append. A leader killed in
between leaves its followers holding the step unapplied. A new leader applies its predecessor's
entries only once it commits one of its own, so for a moment the watched copy reads one step below
what the old leader read.

The cursor itself cannot go back. A `ScrubStep` is applied only from where the cursor is: the same
pass, still running, its `after` the cursor's, and a key past it (`PlacementState::apply`). Every
copy applies the same committed steps in order, so each copy's cursor only moves forward.

## Evidence

**Established by reading the source** and the run's message: the step's rule, and the row reading
the cursor at one node and judging it at another. **Not reproduced**: three runs with every node's
shard loop slowed by 3 ms a message passed. Copies of the victim's group read every 10 ms beside
the leader's, before the kill, agreed at every sample. The window is a leader's append wide, which
a host loaded by a workspace run widens.

## The fix

**The watched copy is held to its own reads**: they never go back, and the pass ends at or past
where the old leader read it, having done every placement group of its group. A copy below the old
leader's cursor while its new leader commits its first entry is a copy behind, not a cursor that
went back.

## Alternatives rejected

- **Read the cursor at the group's new leader once it leads.** It commits its first entry before it
  applies its predecessor's, so it reads below the old leader's cursor the same way for a moment.
- **Wait after the kill before the first read.** A time a loaded host outruns.

## Invariants to uphold

- **A row compares a copy's applied state with that copy's own earlier reads**, never with another
  copy's, unless it waits for the other to apply past the same index.
- **A scrub step applies only from where the cursor is**, which is what makes a copy's cursor move
  only forward.

## Still open

Nothing.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `scrub_resumes_from_its_cursor` (`cluster_fixture`) | A follower behind the commit of the old leader's last step, read first, fails the row although no cursor went back |

## Related

- [F89](../../features/scrub-and-repair.md), the scrub's passes and cursor.
- [Tests That Fail Under Load](../flaky-tests.md#left-the-page).
