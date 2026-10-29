# 253. A put through a stale placement left its chunk on a holder its group had moved off

A put places each stripe where its placement group's state, as its coordinating shard last read it,
names the holders, and reads the state again once its pieces are durable
([F88](../../features/recovery-and-moves.md#how-a-writer-learns-its-groups-state)). A group that moved
since the shard last read it is found moved then: the put marks the new holders it did not reach
missed, so recovery rebuilds them, and commits. What its piece wrote on the old holder stayed there.
Before M20 nothing was ever removed, so that was one more chunk among many. Since
[F92](../../features/reclamation-and-device-lifecycle.md) a move's old holders are released and a
drained device is removed once it holds nothing, and these copies are on slices whose release had
already run: nothing would remove them, and a drained device never emptied. Found by F92's own lab
run, the drain under load, the day it was committed, and fixed in four steps: the put's own copy,
then a piece that lands after its removal, which the drain taken again found, then a sweep by each
node of whatever any late writer leaves on a slice its group has left, which the third taking found
was still needed, then a put refused under its group's moved state, which a probe of the fourth
caught on the device.

## Symptom

On the lab, `shoaladm bench`'s `device_drain` arm (`rw50` at 1 MiB, two in flight, at `e4808f2`)
drained titan's first SSD a third of the way in. Its 2,738 placement groups moved and every slice they
left was released, `moves 0 behind 0`, and the pool had rebuilt 328 positions; but the event never
finished, since the drained device still reported 306 chunks, 1.2 GiB. Every one belonged to an
object whose id was minted after the drain began. Its removal would have been refused for as long
as it held them.

## Cause

`ObjectTask::check_put` compared each stripe's holders under the state read now with the holders
its pieces reached, and marked missed every current position its pieces did not reach. It never
looked the other way: at a holder its pieces reached that the state now does not name. The shard's
cache of a group's state (`MetaClient::placements`) is read once and kept, so a shard that put into
a group before its move places the next put there by the old state, onto the old holder, after that
slice's release. The release reads the group's state under its seal and removes what is there; a
piece that arrives after it is nobody's.

A piece can also land late. A put goes on once `k + f` holders hold a stripe, and the last
holder's piece is still on its way; a stale holder's late piece was neither counted reached nor, once
removed, kept from landing after the removal, and a piece that met the removal's seal waited for it
and ran after it.

The put's removal ran only on the way out that commits. `check_put` refuses a put whose pieces hold
fewer than `k + f` positions under the state read now, and a stale put meets that whenever its group
left two of its slices in one move, as a drain's refit does: of three copies by host, the one old
holder still current is all it holds. It returned the refusal before it had collected anything
stale, so the copies on the two slices the group left stayed, and the try's retirement does not
reach them either: the reclaimer takes a retired object's chunks from the holders its group places
on and the slices it names left, and these it had released before the put.

And a put is not the only late writer, by reading. A rebuild reads its group's state, then copies to the holder
that state names, as a background request that waits for the slice's foreground and its device's
recovery ceiling; a move's copy does the same. A switch between the read and the copy makes that
holder a slice the group left, and a copy held back long enough lands after the release and after
the 30 s the slice refuses it for. Once the group's state no longer names the slice left, nothing
ever asks that slice about the group again: no release, since it was released, and no light scrub,
which lists a group only on the slices it places on.

## Evidence

**Reproduced** by `a_put_through_a_stale_placement_leaves_nothing_on_a_drained_device`, written for
this and run against `e4808f2`: node zero puts 512 objects of 20,000 bytes, caching as many groups;
node two's first device is drained and emptied by its releases; node zero puts 256 more.

```text
thread 'a_put_through_a_stale_placement_leaves_nothing_on_a_drained_device' panicked at shoal/tests/cluster_fixture.rs:27272:5:
6 chunks were left on the drained device, 6 of puts after the drain
test result: FAILED. 0 passed; 1 failed
```

Taken again at `76078e1`, the first half of the fix, the same arm's moves settled within eight
minutes and left 6 chunks on the drained device, all written after their groups' switch: stragglers.
`a_late_write_of_what_was_removed_is_refused` holds the slice's half.

Taken a third time at `5383d7a`, with both halves, the first of three drains settled (2,744 groups
switched, 2,774 slices released, nothing behind) and its device still never emptied in the event's
900 s; the second and third emptied, 356 s and 346 s after their requests, the second with 147
rebuilds beside its moves. Something still landed late
on occasion. The first drain's leftovers went with its cluster before they were listed, so the
writer was not identified on the device then; the fourth taking, below, found a refused put, and a
rebuild's copy waiting on its ceiling across a switch can land so too, by reading. The sweep does
not depend on which it was. **Reproduced** by
`a_chunk_left_where_its_group_does_not_place_is_swept`, written for this: node
two's drained device is emptied by its releases, a chunk of a group now on the other device is put on
it as a late piece lands, and node two started again so its report counts it. Run with the sweep
switched off, which is `5383d7a`'s behaviour, the chunk was never removed and the device never could
be:

```text
thread 'a_chunk_left_where_its_group_does_not_place_is_swept' panicked at shoal/tests/cluster_fixture.rs:26774:9:
the late chunk swept from the drained device never came
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 228 filtered out; finished in 251.96s
```

The drain taken a fourth time ran `5383d7a`'s node again, not the sweep's: the lab's inventories
name a prebuilt program, which was not rebuilt, and the capture recorded the worktree's commit
beside it (item 254). Its first drain never emptied, as before. A probe of its second listed the
drained device once the moves settled: one chunk, of group 1477/0, written at 21:24:08. That group
had switched at 21:20:41, leaving two slices, titan's first SSD and another node's second, and both
were released at 21:21:11; the chunk's object id was minted at 21:24:08, so it was a fresh put by a
shard that cached the group's state from before the switch, refused at its check and put again.
**Reproduced** by `a_put_refused_by_a_moved_group_leaves_nothing_where_it_moved_from`, written for
this and run against the put check before the fix: nodes one and two drain their first devices
together, reclamation is paused once they are emptied, and node zero puts 256 more.

```text
thread 'a_put_refused_by_a_moved_group_leaves_nothing_where_it_moved_from' panicked at shoal/tests/cluster_fixture.rs:27334:9:
assertion `left == right` failed: 5 chunks were left on node 1's drained device by puts after the drains
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 229 filtered out; finished in 304.81s
```

## The fix

`check_put` collects every holder a piece reached that the group's state read now does not name,
and once the put's marks are committed `remove_stale` takes each one:
1. seals `Object { consumer, pg, object }` on the holder;
2. reads the group's state again at its leader, while sealed;
3. unseals and leaves it if the group places there again;
4. otherwise removes the object's chunks there under the seal, counted among the pool's removals.

The object is not current yet, so no reader is owed a grace. A holder that does not answer keeps its
copy, said in the log at DEBUG. Every target the put's placement named that the state now does not
is taken, its piece counted or not. The stale holders are found for every stripe before any stripe
is judged, and `remove_stale` runs on every way out of the check: the put committed, refused because
a stripe cannot be placed or is held by fewer than `k + f` positions, a mark that failed, or groups
that moved under it too often, which takes what its last try found.

**A slice remembers what it removed for a seal's lapse** (`Slice::refuse_removed`, 30 s): a whole
chunk's piece, a stage of units or of parity, of an object or a group a reclamation removed in that
window is refused `Removed` (store code 12) after its seal wait. A piece on its way when the removal
ran, or one the seal held back, is turned away rather than bringing back what nothing refers to.

**Every node sweeps its slices of stray groups** (`reclaim.rs`, `Reclaimer::sweep`, on shard zero
at every reclamation pass). A slice lists the placement group directories it has (`Slice::groups`,
one directory read a consumer). A group whose pool's target does not place it on the slice is a
candidate; one found so for `reclaim_grace` has its state read at its leader, at most 256 of a slice
a pass. A group that is not moving, does not name the slice left, and places nothing on it is stray,
and is swept:
1. sealed `Group` on the slice;
2. its state read again at its leader, while sealed;
3. unsealed and left if it is no longer stray;
4. otherwise removed under the seal, its emptied directory with it, and counted `released`.

The grace is the release's own, so a reader of an old view and a release of what a move left both
outlive it; a slice of a failed or removed device is never swept. The guard is the release's, which
X1's model holds to P16, less its need for the slice to be named left: a slice the group's committed
state neither places on nor brings a move to holds nothing that state refers to. X1 has no late copy
to model, so the fixture row holds the sweep, not a schedule.

## Alternatives rejected

**Naming the stale holder in the group's `left`**, so the move driver releases it past its grace as
it releases what a move leaves. Durable where this is best effort, but a placement command is
proposed through a local copy of the group, which a put's coordinator need not host, and a new
command for a put to send through a leader it may not reach is more than the case needs.

**Refreshing the shard's cache whenever a move switches.** A switch moves no topology version and
is committed in the group's own log, which a shard that coordinates a put need not follow; the put's
re-read after its pieces is where staleness is found, and stays the place it is handled.

**Reading every group's state before every put.** A read through the group's leader for every put,
to save a removal a put makes only after a move.

~~**A sweep of a drained device for what no group places there.** It would list every placement group
on every ended device's slices each pass; the put knows exactly what it left and where.~~ Taken as
the third step: the put does know what it left, but a rebuild's or a move's late copy is nobody's to
remember, and neither is a dying coordinator's. The cost was the reason against it, and is bounded:
a group the pool's target places on the slice is never read, so a pass of a settled pool reads
nothing, and a drained device's still-moving groups are read at most 256 a slice a pass.

**A longer refusal window.** Thirty seconds was chosen as a seal's lapse; a background copy waits on
its device's ceiling for as long as the foreground keeps it busy, which has no bound, so no window
is long enough, and a longer one refuses a forced id put again for longer.

**A rebuild or a move that reads its group's state again before every piece.** A read through the
leader per piece, on the paths recovery's rate depends on, for a race the sweep closes anyway.

**Sweeping every slice for every group, read or not.** A pass of a settled pool would read every
group's state on every node; the target answer, computed locally, rules out every group that belongs
where it is.

**Refusing a late piece by its label.** A put's piece carries the put's label, the same one a
fresh put of the same object would carry, so a label cannot tell a late piece from a later write.

**Leaving a refused put's copies to its retirement.** A put refused at its check is given up, and
its id retired, but the reclaimer reaches a retired object's chunks through the holders its group
places on and the slices it names left; a slice the group left and released before the put is
neither.

**Waiting for every piece before the put's check.** It would put the slowest holder back on every
put's path, which the straggler wait exists to take off.

## Invariants to uphold

- **A put's piece on a holder its group no longer places on is removed by the put**, under a seal
  set before the state is read again; nothing else knows it is there. **Whether the put commits or
  is refused**: every way out of `check_put` after its pieces were written passes what it found
  stale to `remove_stale`, and a new way out that does not leaves a refused put's copies behind.
- **A removal of a put's stale copy never runs while its group places on that holder again**: the
  read under the seal decides.
- **A drained device empties only once every put that may still place on it by an old state has
  checked**: its removal is admitted on its node's report of nothing held, never on time.
- **A slice refuses a write of what it removed for a seal's lapse after the removal**, so the window
  outlasts any piece in flight; an id put again inside it is refused, which only a forced id does.
- **A stray group is removed only under a seal set before the read that found it stray, and never
  while its group moves**: a move, rebuild or put that begins after that read and would place there
  waits on the seal, and a group placed there again before it is left alone.
- **A sweep never reads a group its pool's target places on the slice**, so a settled pool costs a
  pass one directory listing a slice; a change that sends every group to the read makes every pass
  a read of every group's state.
- **A group's release takes its emptied directory**, so a sweep does not find it again at every
  pass.

## Still open

~~A coordinator that dies between its pieces and its removal leaves its copy, and a drained device
holding one is refused removal until `remove-device --lost`. A sweep of ended devices would close
it, filed with F92's [todos](../todos.md#what-f92-left-for-later).~~ The sweep closes it: a dying
coordinator's copy is a stray group's chunk like any late piece, removed a grace and a pass after it
is found. ~~Nothing is left open.~~ ~~Two ways out of `check_put` still leave a later try's stale
copies for the sweep: a placement read that fails, and a `stripe_set` that fails (item 269).~~
Both pass what an earlier try found to `remove_stale` since [Resolved #269](put-stale-on-every-exit.md);
a placement read that fails on the first try, before anything is found, is left to the sweep.

## Tests

| Test | What breaks if it is reverted |
| --- | --- |
| `cluster_fixture` `a_put_through_a_stale_placement_leaves_nothing_on_a_drained_device` | A put by a stale placement leaves its chunk on a drained device, which then never empties and is never removed |
| `cluster_fixture` `drained_device_leaves_only_when_its_chunks_are_elsewhere` | A drain under writes in place ends holding chunks |
| `device_store` `a_late_write_of_what_was_removed_is_refused` | A late stage of a removed object lands, or another object of its group is refused |
| `cluster_fixture` `a_put_refused_by_a_moved_group_leaves_nothing_where_it_moved_from` | A put refused because its group left two of its slices leaves its copies on them, and a drained device holds them |
| `cluster_fixture` `a_chunk_left_where_its_group_does_not_place_is_swept` | A late piece on a slice its group left stays there for good, and a drained device holding one is never removed |
| `device_store` `a_slice_lists_its_groups_and_a_released_one_is_no_longer_listed` | A sweep cannot find a group's directory, or finds a released group's emptied directory again at every pass |

## Related

[F92](../../features/reclamation-and-device-lifecycle.md) for the drain and the release this completes;
[F88](../../features/recovery-and-moves.md#how-a-writer-learns-its-groups-state) for the put's re-read
of its group's state; [Resolved #246](moved-group-taken-at-base.md) for the other way a shard's
placement was stale.
