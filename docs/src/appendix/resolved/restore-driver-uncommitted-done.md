# 183. A restore driver told its loop a group was done when the control plane never recorded it

Found while chasing [item 142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s
restore stall in round 13 of the [lab testing](../../cluster-testing/correctness.md#14-round-13).
It explains one of the two ways that stall could happen. The other is not ruled out, and #142
keeps the stall until it is.

## Symptom

`a_restore_rides_out_an_unreachable_member` failed twice in full suite runs, both times in the
same shape. One group was left at `Pending` with no driver and no attempts, and it was never driven
again within 300 s. The restore's other groups were `Done`. Round 12 read the record and
concluded that either no member led the group for five minutes, or its leader's shard believed it
was already driving a restore.

There was a third way, and the code allowed it: the leader's shard believed the group was
**done**.

## Cause

A shard's restore loop (`drive_restores`, `shoal-core/src/server/shard/restore.rs`) remembers the
phase each of its drivers finished at, in `driven_restores`. When the pushed map lags behind that
phase, the loop trusts its own memory. A remembered `Done` means the group is skipped for good.

The driver (`drive_group_restore`) reported `Done` for any error that was not a lost lead or an
unreachable member. It first tried to commit the group as `Failed`, then ignored whether that
commit landed:

```rust
let _ = context.commit(GroupRestore { phase: RestorePhase::Done, outcome: Some(Failed { .. }), .. }).await;
RestorePhase::Done
```

One such error is the progress commit itself failing: the control plane did not take a phase
within `PROGRESS_TIMEOUT` (30 s), or the control thread was gone. A driver whose `Loading` commit
failed that way therefore failed the group. Its `Failed` commit then met the same control plane
and most likely failed too. The loop was told `Done`, and the record still said `Pending`, with no
driver and no attempts: exactly the record the suite kept.

A second, smaller defect sat in the same path. `RestoreContext::commit` set `reached`, the phase a
hand-back resumes from, *before* it knew the commit had landed. So a driver that lost its lead
could hand the group back at a phase the control plane had never recorded.

## Evidence

**Established by reading the source.** The record in both suite failures, a group at `Pending`
with `driver: None` and `attempts: 0` whose leader never drove it, is what the code above produces
when a first progress commit and the failure commit both fail. A group left at `Pending` whose
shard does *not* believe it done would be driven again at the next tick, so the record also rules
out a lost lead as the cause.

It was not reproduced. `target/lab/r13/142/loop.sh` ran the test in a process with five other
heavy fixture tests at six threads, with child logs on every child, on the unfixed tree: 0 failures
of it in 6 rounds, while the other five tests failed 16 times between them (their deadlines, item
142). The unit test below states the rule on its own.

## The fix

- **A driver reports only a phase the control plane took.** `settled_phase` returns `Done` only
  when the `Failed` commit landed. Otherwise it returns the phase committed last, and the loop
  drives the group again from there.
- **Progress the control plane did not take is not a failure.** Commit errors now begin with
  `UNCOMMITTED`. The driver treats them as a hand-back: nothing in the record changed, so it
  pauses `RESTORE_RETRY_PAUSE` and reports the phase committed last. It no longer tries to commit
  a `Failed` the control plane will not take either. Before the fix, a *finished* restore whose
  final `Done` commit timed out was recorded as `Failed` in `Verifying` once the control plane
  came back. Now it is verified again.
- **`reached` moves only when a commit is applied.**

## Alternatives rejected

- **Retrying the `Failed` commit until it lands.** It would hold the shard's one driver slot for as
  long as the control plane is away, and every other group the shard leads would wait behind it.
  Reporting the committed phase frees the slot and has the same effect, since the next tick drives
  the group again.
- **Ignoring `driven_restores` for a group whose record says it is not done.** That memory exists
  because the pushed map lags a driver's commits. Without it a group would be driven again from an
  earlier phase, and a loading phase refuses a table an install already filled.
- **Clearing `driven_restores` on every map push.** This loses the same protection for the same
  reason.

## Invariants to uphold

- **What a driver reports to its loop is a phase the record holds, or will hold once the map
  catches up.** The loop's memory may be ahead of the map. It must never be ahead of the control
  plane.
- **A commit that did not land changes nothing.** That covers `reached`, the loop's memory and the
  outcome.
- **Only a refusal from the control plane, or an error from the restore's own work, fails a
  group.** The control plane being unavailable is not an outcome.

## Still open

Item 142's restore stall stays filed until it is caught with child logs. This fix removes one way
to reach it. The other way is a driver of another group on the same shard that never finishes, and
every await in `drive_inner` was checked and found bounded (`scrub_group` by the phase timeout,
`repair_snapshot` by the snapshot timeout, the restart wait by `RESTART_TIMEOUT`).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-core` `server::shard::restore::tests::an_unrecorded_failure_is_not_reported_done` | A failure whose `Done` was not committed is reported done, and the loop never drives the group again |
| `shoal` `cluster_fixture::a_restore_rides_out_an_unreachable_member` | Rides out a member cut off for 25 s. It does not force a failed commit, so it passes on the unfixed tree too |

## Related

- [Known Issues #142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host),
  the stall this was found under.
- [Resolved #155](restore-retries-unreachable.md), which made an unreachable member a retry rather
  than a failure. This page does the same for the control plane.
- [F49](../../features/backup-and-recovery.md), the restore's phases.
