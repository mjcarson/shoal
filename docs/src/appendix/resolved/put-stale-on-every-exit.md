# 269. A put's stale copies were left when its placement read failed on a later try

A put that finds its placement group moved after its pieces landed removes what it left on holders
the group no longer places on, on every way out of `check_put`
([Resolved #253](put-left-on-moved-holder.md)). Two ways out missed what an earlier try had found:
a placement read that failed on a later try returned through `?`, and a `stripe_set` that failed
removed only the stripes the current try had reached. Filed by a code review of F87 to F93; the
second reading found the `stripe_set` branch.

## Symptom

None seen. The copies are a retired put's, since its caller gives the put up either way, so they
are a stray group's chunks that the move driver's release takes while the slice is still in the
group's `left`, and the stray-group sweep (`Reclaimer::sweep`) otherwise, once the group settles,
after `reclaim_grace` and a pass. They cost their bytes and held a drained device's removal back
by that much, and never blocked it.

## Cause

`check_put` keeps the stale holders a try found as `last_stale` and tries again when the group moved
under its marking. The next try's `read_placements(&keys).await?` returned on a failure before
reaching any removal, and only the loop's exhaustion passed `last_stale` on. A `stripe_set` that
failed for a stripe passed `remove_stale` the current try's `stale`, which held only the stripes
before it.

## Evidence

**Established by reading the source**, twice. Not reproduced: reaching either exit needs a group to
move between a put's marking and its next placement read, and that read to fail, and no hook makes
both happen on demand. The fixture's hooks that would, a put held before its check, a marking
answered `Moved` and a placement read failed, were judged more machinery than a delay the sweep
already bounds.

## The fix

The placement read is a `match` that passes `last_stale` to `remove_stale` before it returns its
error, and the `stripe_set` branch passes the current try's stale holders and the last try's it
does not already hold. `remove_stale` reads each group's state again under a seal, so a holder the
group places on again since the last try is left.

## Alternatives rejected

- **Leaving them to the sweep.** It removes them, a grace and a pass later; Resolved #253's
  invariant is that the put removes what it left, which is what lets a drained device empty as soon
  as its moves settle.
- **Removing every holder the put wrote to on an error.** A holder the group still places on is the
  put's own copy, whose removal is the reclaimer's once the put's id is retired.

## Invariants to uphold

- **Every way out of `check_put` passes what any try found stale to `remove_stale`**, a `?`
  included: one added has to as well.

## Still open

A placement read that fails on the first try, before anything is found stale, leaves the put's
copies on holders its group moved off to the sweep, as before: nothing has named them yet.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| (none new) | The exits are reached only by a move and a failed read together, which no hook makes; Resolved #253's three put rows run the path and pass |

## Related

[Resolved #253](put-left-on-moved-holder.md), whose invariant this completes;
[F92](../../features/reclamation-and-device-lifecycle.md) for the sweep.
