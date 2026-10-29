# 263. A restore dealt an archive to a node that had since left, and never finished

A restore puts a bucket's archives of objects again once its tables are restored, each through
shard zero of the node it is dealt to ([F93](../../features/operations-and-the-real-cluster.md)).
`apply_restore` dealt them round the placement the cluster was initialized over, which nothing
writes again once a member is decommissioned, removed, tombstoned or recovered past. An archive
dealt to a member that had left was driven by nobody: only its node drives it, a member that is not
one is refused its progress, `RetryRestore` is refused while any archive is unfinished, and a retry
kept the archive's node. The restore stayed running for good. Filed by a code review of F87 to F93.

## Symptom

A restore into a cluster that had lost a member since it was initialized, or lost one during the
archive phase, never finished and could not be retried. A member only down stalled its archives
until it came back, as it still does.

## Cause

`apply_restore` took `self.initialized`, the placement `Initialize` committed, for its dealing, and
`drive_archives` drives an archive only on `progress.node`. Nothing dealt an archive again when its
node stopped being a member.

## Evidence

**Reproduced** by `an_archive_dealt_to_a_member_that_left_is_dealt_again`, written first and run on
the unfixed tree: a state of two members with a restore's archive dealt to the second, which is then
removing and tombstoned.

```text
thread 'server::control::types::tests::an_archive_dealt_to_a_member_that_left_is_dealt_again'
panicked at shoal-core/src/server/control/types.rs:5646:9:
assertion `left == right` failed
  left: (NodeId(c63ade99-…), 0, 32, Putting)
 right: (NodeId(4b084ef8-…), 1, 32, Pending)
```

## The fix

- **A restore deals its archives over the placeable members** (`archive_nodes`), up and plain, in
  node order; the initialized placement only when none is.
- **An unfinished archive whose node stops being a plain member is dealt again**
  (`redeal_archives`), after every command the control state applies, so whatever moved the member
  out - a decommission, a removal, a tombstone, a recovery - moves its archives too. It goes round
  the members by its place in the record, a generation up and `Pending`, from the objects it put, so
  the new node resumes past them and a driver of the old deal is too late to say anything. The
  command's answer names the version the dealing moved to.

## Alternatives rejected

- **Dealing again at the commands that move a member out.** There are several, and a new one would
  have to remember; one pass after every apply cannot be forgotten, and finds nothing when no
  restore is recorded.
- **Letting any member drive an archive whose node is not placeable.** Two drivers of one archive
  would put its objects at once; one node a generation is what the progress commits are judged by.
- **Dealing again an archive whose node is only down.** It comes back, usually, and a deal that
  moved on every heartbeat missed would race its own driver.

## Invariants to uphold

- **Every unfinished archive is dealt to a plain member**, after every command applied.
- **A deal moves the generation**, so the progress of a driver of an older deal is never applied.

## Still open

An archive whose node is down and plain waits for it to come back or to leave, as it did.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `control::types::tests::an_archive_dealt_to_a_member_that_left_is_dealt_again` | An archive stays dealt to a member that left, and its restore never finishes |

## Related

[F93](../../features/operations-and-the-real-cluster.md); [Resolved #261](archive-driven-once.md),
the same driver's guard; [F46](../../features/capacity-rebalancing.md) for a member's phases.
