# 197. A move's source resumed the drive and reconfigured itself out, and a driver whose copy was retired under it held its shard's slot for the migration timeout

Filed and fixed in one change, from round 16 of the lab testing: the loop of the fixture's six
heaviest tests that [#142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)
is followed with, with every failing round's logs kept
([#142 in round 16](../../cluster-testing/correctness.md#142-in-round-16)). It is the stuck move
round 15 saw once and could not explain.

## Symptom

`migration_resumes_after_each_phase_failure` failed in the loop's first round:

```text
Error: ChildFailed("move b557a2bf… did not finish within 240s: {… \"phase\":\"Published\",
  \"groups\":{\"10820355652101285960\":{\"phase\":\"Done\",\"outcome\":\"Moved\", …},
             \"17036337662592232082\":{\"phase\":\"Retiring\",\"outcome\":null, …}} …
```

The test kills the driver of a set's move at each phase in turn and expects the move to resume
and finish. Its last round killed the driver right after it committed `Retiring`; the group's
other copies elected the move's destination, and for four minutes nobody drove the group's
retirement to `Done`. Round 15 had seen the same shape once, at `Configured`, without logs.

## Cause

Two defects in the move driver (`shoal-core/src/server/shard/migrate.rs`), one setting up the
other. The child logs showed them on the node that had become the group's leader and drove
nothing: it had led the same group as the *source* of the move before, three rounds earlier.

**A driver resumed at `reconfiguring` on the move's source drove its own removal.** "The source
never drives its own removal" - the hand-off of the lead to a member of the target - sat inside
the branch a driver runs when it starts *before* `Reconfiguring`, after the catch-up. A driver
killed right after committing `Reconfiguring` leaves an election, and when the source wins it,
it resumes at `Reconfiguring`, skips the branch, and proposes the uniform membership that removes
itself. In the loop's earlier round the source did exactly that (`the uniform membership naming
the target is committed`, 17:47:04) and went on into `activate`, polling for the destination to
apply it.

**A copy retired under a running driver froze the driver's view of itself.** Two seconds later
the published map reached the source and `retire_group` shut the group's Raft handle down under
the driver. A shut-down handle keeps the metrics it last published, its lead among them:
`MoveContext::leads` compared `current_leader` with this shard and saw itself leading, and
`destination_lag` read a replication map that would never move. `activate` polled silently, and
would have until `migration.timeout` - 600 s by default, 300 in the test - since nothing it
checked could change. `driving_moves` held the `(op, group)` for as long, and with
`migration.concurrent` at its default of one, `drive_moves` returned at the concurrency cap on
every tick from then on: the shard drove nothing. When the same shard later led the next move's
group after that move's driver was killed at `Retiring`, it did not drive it either, and the
test's 240 s passed.

## Evidence

**Established by reproducing it** in the loop with the child logs kept
(`target/lab/r16/142/loop-keep.sh`, `target/child-logs-r16-142/failed-r1/`): the source's
driver logged the uniform membership committed at 17:47:04.6 and nothing after; the same
shard logged `retiring this shard's copy of a moved group` at 17:47:06.5 with the Raft's
`recv from rx_shutdown`; it became the next move's group's leader at 17:47:27 and never logged
`driving a group's move` again, while the restarted driver's log never mentioned the move.

Pinned first by `a_source_resuming_a_move_hands_the_lead_on_and_frees_its_driver`, written on
the unfixed tree: the set's leader is armed to die at `reconfiguring`, the third member is paused
so the source wins the election, and the record is sampled through the rest of the move. On the
unfixed tree it fails

```text
the source drove its own removal: {"config":10,"driver":"ecc64a2a…","phase":"Configured",
  "stats":{…"phase_ms":{…"reconfiguring":7074}}}
```

- the source's own id as the driver of `Configured`, after seven seconds reconfiguring itself
out. On the fix the source hands the lead on at once and the move completes. The frozen handle is
not reproduced deterministically: in the fixture the destination applies the uniform membership
before the map publishes, so a resumed source finishes `activate` before its copy is retired.
The loop on the fix is what covers it.

## The fix

- **The hand-off is judged by the phase the transition has not reached**, not by the phase the
  driver started at: a driver on the source whose group is short of `Configured` hands the lead
  to a member of the target and leaves, whether it started at `planned` or resumed at
  `reconfiguring`.
- **`MoveContext::leads` is false on a handle that was shut down** (`running_state.is_err()`),
  so a driver whose copy is retired under it fails its next check `NOT_LEADER`, leaves the move
  for the group's new leader, and frees its shard's slot.
- `ShardReplication::driving_moves` reports how many drivers a shard is running, so a test can
  see a slot held.

## Alternatives rejected

- **Cancelling the driver in `retire_group`.** The driver is a detached task; cancelling it
  would need a handle kept per `(op, group)`, and it would still have to end through
  `MoveDone` to free the slot. Making its own checks see the shutdown ends it on the path it
  already has.
- **Forgetting `driving_moves` entries in `retire_group`.** The task would go on running against
  a dead handle for the timeout, and a `MoveDone` arriving later could remove a newer entry
  under the same key.
- **Raising `migration.concurrent`.** Hides a held slot behind a second; the slot is still held.
- **Transferring the lead in `reconfigure` itself.** The hand-off belongs before the transition
  is proposed, where the driver knows it is the source; inside `reconfigure` it would run on
  every retry of the transition.

## Invariants to uphold

- A move's source never proposes the membership that removes itself, whatever phase it resumes
  at: the hand-off runs before `reconfigure` for any group short of `Configured`.
- Every wait in a driver goes through `leads()`, and `leads()` is false on a handle that was
  shut down. A driver that lost its copy leaves the move; it does not wait out a timeout.
- A driver ends through `MoveDone` on every path, since `driving_moves` is what the concurrency
  cap counts and nothing else removes an entry.
- `retire_group` may shut a group's handle down while a driver for that group runs on the same
  shard; the driver is what has to notice.

## Still open

- The frozen-handle half is pinned by the loop, not by a deterministic test: a fixture destination
  applies the uniform membership before the map publishes, so the window between the source's
  reconfiguration and its retirement does not open there.
- [#142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s stuck
  move was this; its other shapes are its own.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_source_resuming_a_move_hands_the_lead_on_and_frees_its_driver` (`shoal/tests/cluster_fixture.rs`) | A source resumed at `reconfiguring` drives its own removal: the record shows it as the driver of `Configured` |
| `migration_resumes_after_each_phase_failure` (the same) | The loop's shape: a move left at `Retiring` or `Configured` for its deadline after its driver is killed |

## Related

- [F45](../../features/replica-migration.md), the move driver.
- [Resolved #183](restore-driver-uncommitted-done.md), the restore driver's version of a group
  nobody drives again.
- [Cluster testing, round 16](../../cluster-testing/correctness.md#142-in-round-16).
