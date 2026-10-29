# 274. The pool admin row took a refit in its window for the repeat's write, and read the leader once

A pool mutation repeated with the same operation id is answered `Repeated` from the applied state
and writes nothing ([F85](../../features/devices-and-pools.md), [F92](../../features/reclamation-and-device-lifecycle.md)).
`pool_admin_is_authorized_versioned_and_idempotent` holds that for every pool mutation through one
helper, `answered_as_every_mutation`: it measures the control leader's log before the repeat, once
the log has been quiet for a second, and again 300 ms after it.

Filed from the workspace run of Resolved #260 to #273; fixed in the flaky-test plan's
confirmation (2026-10-27).

## Symptom

In fifteen loaded runs, from F93's to W4's, the row failed with *reweight_device repeated wrote to
the log*, the log 334 or 170 bytes longer. In nine of those runs it failed again at three threads,
and it passed every time it ran alone.

## Cause

Two things, one after the other.

- **The row took another command for the repeat.** A reweight starts a refit of the pool: the
  control leader commits `SetPoolWeights` a moment after the mutation. The row waited for a quiet
  second before it measured, but a loaded host commits the refit later than that, inside the
  window. The row counted the log's bytes, and so read the refit as the repeat's write.
- **The row read the control leader once.** With the window read by its entries, the row failed
  next at `leader_index(0)?.expect("a leader")`: node zero named no leader for a moment, as it does
  between leaders or before it has heard from a new one, and the helper did not wait.

The engine was right both times.

## Evidence

**Established by running it**, with the row reading the window's entries (the fixture's `LOG_FROM`,
since 2026-10-27). In the confirmation's A/B, three of the six rounds failed the row (`w5ab` on
[the flaky page](../flaky-tests.md#runs)), and each printed the window first:

```text
reweight_device's repeat wrote nothing of its own; the window took [{"log_id":{"index":27,"leader_id":{"node_id":"0c92d36c-…","term":1}},"payload":{"Normal":{"SetPoolWeights":{"generation":4,"pool":"fast","weights":{…}}}}}]
```

Then the next mutation's measure failed:

```text
thread 'pool_admin_is_authorized_versioned_and_idempotent' panicked at shoal/tests/cluster_fixture.rs:31920:43:
a leader
```

Each window took a refit and nothing of the repeat's. The leader read that failed was the next
mutation's, and nothing the row printed says whether the group was between leaders then or node
zero had not yet heard from one; either names nobody.

## The fix

- **The row judges the repeat by its own entries.** It reads the entries the window appended and
  fails only on one that carries the repeated operation's id. A refit or a member's report
  committing beside it is printed and passes. This change was first made as reporting, and is
  kept as the fix.
- **The control leader is waited for** (`wait_leader_among`, 30 s), in this row and in three others
  that read it the same way: `admin_mutations_require_principal_and_operation_identity`,
  `fresh_failure_reports_do_not_mask_shard_failure` and
  `removal_grace_survives_control_leader_restart`.

## Alternatives rejected

- **Wait longer for the log to settle.** A refit commits whenever the leader gets to it, and the
  row already waits for a quiet second. No settle is long enough on a loaded host.
- **Pause refits for the window.** The row would no longer test the repeat against a pool that
  moves, which is what its version check is for.

## Invariants to uphold

- **A repeat is judged by what it wrote, never by what the log took.** The log is shared by every
  command the cluster commits.
- **A read of the control leader is a wait.** A node may name nobody at any moment.

## Still open

- **A repeat taken by a node that has not applied the first commit proposes it again.** The
  proposal is deduplicated at apply, but it is a log entry, and the row would catch it now by its
  operation id. It has not been seen: every failure's window held another command.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `pool_admin_is_authorized_versioned_and_idempotent` (`cluster_fixture`) | Under load, a refit in the window is read as the repeat's write, or the helper finds no leader named |
| `admin_mutations_require_principal_and_operation_identity`, `fresh_failure_reports_do_not_mask_shard_failure`, `removal_grace_survives_control_leader_restart` (`cluster_fixture`) | A moment between control leaders panics them where they read the leader |

## Related

- [F85](../../features/devices-and-pools.md) and [F92](../../features/reclamation-and-device-lifecycle.md),
  the pool mutations and their repeat rule.
- [Resolved #328](clusters-beside-each-other.md), found by the same confirmation.
- [Tests That Fail Under Load](../flaky-tests.md).
