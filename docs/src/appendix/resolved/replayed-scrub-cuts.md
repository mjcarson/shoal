# 164. A copy replaying a run of scrubs read its archives once for each

## Symptom

On the lab, the verify of titan's repaired copy of group `2c0307ebca0fd179` did not arrive
within the scrub's five minute deadline, so the repair failed, although the install had taken
two seconds. The next automatic repair, six minutes later, found the copy clean.

## Cause

After the install, titan's group was restarted from the repair snapshot and replayed the log
above its boundary. That stretch of the log held about forty scrub entries: every earlier
repair's two rounds, and every nudge that `advance_past` proposes to move a checkpoint along.
Each one applied started a digest task over the group's cut, 319,974 archived partitions, and
they all ran at once. The one digest the repair was waiting for was the last of them.

Two things were wrong:

- **A nudge computed a digest.** `advance_past` (repair and backup) proposes a scrub under a
  fresh operation only to get an entry past an index. Nobody ever polls it, and every replica
  cut it anyway.
- **An old scrub's cut ran to the end after a newer scrub of the same group had been applied.**
  A group's scrubs are driven one at a time, so a newer one means nobody is waiting for the
  older.

## Evidence

**Found on the lab, established from titan's journal:**

```text
03:54:56.70 a tablet group is up group=2c0307ebca0fd179
03:54:57.42 applied a scrub group=2c0307ebca0fd179 op=babf23ba… index=1085262 resident=1922 archived=319974
03:54:57.49 applied a scrub group=2c0307ebca0fd179 op=d19f49c1… index=1085263 resident=1922 archived=319974
  … one every 70 ms, each a different operation …
03:59:55.67 (leader) a group's repair is done … outcome=Failed { reason: "after the install the
  copies still disagree: Clean { unreported: [titan] }" }
```

No fixture test reproduces the timing: a fixture group holds sixty partitions, and forty cuts of
them finish in milliseconds.

## The fix

- **A nudge is proposed under `NUDGE`, the nil operation, and no replica cuts it**
  (`apply_scrub` returns at once). It is still an entry and moves the log along.
- **A newer scrub of a group cancels the older one's digest task** (`Replication::digest_tasks`),
  which is answered `Unknown`. A replay of a run of scrubs cuts only the last.

## Alternatives rejected

- **Skip a scrub's cut while its group is replaying.** A copy cannot tell a replay from a burst of
  live applies, and the verifying scrub is itself applied during the replay here.
- **A cheaper cut.** That is [O54](../optimizations.md#o54-a-scrub-reads-every-archived-partition-of-a-group-once-per-pass),
  and it does not remove forty cuts' worth of work.

## Invariants to uphold

- **No replica cuts a scrub under `NUDGE`, and no driver polls one.** A new caller that wants a
  digest proposes a real operation.
- **At most one digest task per group reads its cut.** This relies on a group's scrubs being driven
  one at a time. A new caller that scrubs a group while a repair or a restore does would cancel
  the other's cut, and has to be serialized with them.

## Still open

- A restore's nudge (`drive_inner`'s "boundary" scrub) still polls every member for a digest it
  does not use. It is a real operation, so it is correct, only wasteful: a cut per member per
  restored group.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `an_unreadable_partition_stalls_one_copy_and_repairs_it`, `repair_install_is_atomic_at_every_crash_point` (`shoal/tests/cluster_fixture.rs`) | Run the repair's nudges and its two rounds through the changed paths. Neither reproduces the lab's timing |

## Related

- [Resolved #162](stale-scrub-digest.md), the replayed scrubs' other failure.
- [Resolved #160](unreadable-partition-stalls-one-copy.md), whose repairs found it.
