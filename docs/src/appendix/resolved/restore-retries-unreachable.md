# 155. A restore failed a group for good when a member was out of reach for a while

*The first half of item 155.* A group that fails because a member could not be reached is now
driven again. ~~A group that fails for any other reason still cannot be finished.~~ The remainder,
a group that failed for any other reason, is [resolved on its own page](restore-retry.md): a
finished restore's failed groups are driven again by `RetryRestore`.

## Symptom

On the lab a restore lost a Movie group, 439,864 records, when one member restarted in its middle
([#153](install-dir-absent.md)). The group's driver asked the restarting member to quarantine its
copy, got `ConnectionReset`, and failed the group with `Done {"Failed": …}`. A restore is refused
on a cluster that already restored, so nothing could finish it short of a new cluster and a new
restore.

## Cause

A group's restore driver (`drive_group_restore`, `shard/restore.rs`) had two ends: done, or handed
back when the driver lost the group's lead (`NOT_LEADER`), for the next leader to resume. Anything
else was a failure, and a failure is `Done`. A member that could not be reached (its link reset
mid-RPC, or silent past the phase's deadline) was a failure like a real refusal, although it would
answer again in seconds.

The hand-back had a flaw of its own. It committed the phase the driver *started* at, not the one
it reached, so a driver that lost its lead after its install began handed the group back at
`Loading`. The next driver's loading check then found the rows the install had written and
refused them: *"… holds N rows of the group's tablets; a restore is into an empty table"*.

## Evidence

**Found on the lab** (`target/lab/t14-*`). **Reproduced** by
`a_restore_rides_out_an_unreachable_member` (`shoal/tests/cluster_fixture.rs`), with node two cut
off on every lane for 25 s from before the restore starts, longer than the 20 s a phase waits on a
member. With the retry disabled:

```text
group 15994085291460080379 came to {"Failed":{"reason":"a member could not be reached:
6173066a-…/0 did not report before the restore: no report within 20s: the replication link went
down before the request was written: IO(Kind(UnexpectedEof))"}}
```

With it: every group `Restored` or `Skipped`, every note read back. Shorter cuts (8 s and 16 s)
passed either way, because each phase already waits out a member within its deadline. The lab's
failure was the connection resetting while the RPC was in flight, which the longer cut stands in
for.

## The fix

- **A member that could not be reached is `TRANSIENT`**: a quarantine RPC that failed without the
  peer refusing (`RpcFailure::Remote` is a refusal, everything else is not), and a member whose
  scrub report never came. The driver waits `RESTORE_RETRY_PAUSE` (5 s) and hands the group back
  with `attempts` one higher, and `drive_restores` drives it again. After `RESTORE_ATTEMPTS` (12)
  it fails as before.
- **A hand-back commits the phase the driver reached** (`RestoreContext::reached`, set by every
  commit), for a lost lead and for a retry alike.

## Alternatives rejected

- **Wait longer in each RPC.** A reset answers at once whatever the deadline, and a longer deadline
  holds a phase for a member that is gone for good.
- **Let an operator retry a failed group.** It is still wanted for real failures (the open
  remainder). For a member that is back in seconds, the driver should not need anyone.

## Invariants to uphold

- **A refusal is never retried as unreachable.** Only a failure to reach the member carries
  `TRANSIENT`.
- **A handed-back group resumes at the phase it reached.** A phase is not run twice past the
  point that makes it refuse its own work.
- **The retries are bounded.** A member gone for good fails the group within about a minute.

## Still open

- ~~A group that failed for any other reason cannot be retried: the remainder of item 155.~~
  Resolved: a finished restore's failed groups are driven again by `RetryRestore`
  ([the remainder](restore-retry.md)).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_restore_rides_out_an_unreachable_member` (`shoal/tests/cluster_fixture.rs`) | A member out of reach past a phase's deadline fails its groups' restores for good |

## Related

- [Resolved #153](install-dir-absent.md), what made a member restart in the lab's restore.
- [F49](../../features/backup-and-recovery.md), backup and restore.
