# 250 (the rest). A placement group that held nothing kept placing on a failed device until its move had a slot

A pool's placement groups place at its pin until they move, and a group moves when its holders at
its own generation differ from its pool's target's ([F88](../../features/recovery-and-moves.md#moves)).
Each move was admitted by the control group, sixteen arriving on a device at once
(`MOVES_A_DEVICE`). This is item 250's first half. Its second half, a failed device claimed again
at its node's next start, was [Resolved #250 (in part)](failed-device-returns.md) by F92.

Fixed with the user's word on the design (2026-10-28), after the model took it first.

## Symptom

- **On [F90](../../features/erasure-coded-pools.md#on-the-lab)'s 2+1 lab cluster**, after a
  device was failed and its moves had settled, nine of ten runs of `stripes` were refused
  `NotReady`, each naming a position on the failed slice.
- **In the flaky-test plan's evidence loop `w8`**, `removed_device_id_is_never_accepted_again` put
  an object into a group not yet moved off a device removed as lost. The put committed with its
  third position missed, and its rebuild was refused `DeviceFailed` 275 times in two minutes.

## Cause

A group with no placement state places at its pool's pin, and the pin still names the failed
device. The pool's moves take every group off it, but each move waited for an arrival's slot. A host
of two devices that loses one sends every group moving off it to the other: about two thousand
groups through sixteen slots, a minute or more on the fixture and minutes on the lab. Until its
turn, a group placed on the failed device. A replicated put into one missed that position, and its
rebuild had nowhere to go, since a rebuild writes a group's own holders. A 2+1 put, whose `k + f` is
its width, was refused.

A group with no state is not a group that holds nothing, so the recorded direction, placing such a
group at the target, would have been worse ([below](#alternatives-rejected)). But most of the groups
moving off a failed device hold nothing, and a slot bounds the bytes a device takes at once. A move
of a group that holds nothing moves no byte, so it waited for no reason.

## Evidence

**Established by the lab and the loop's child logs, then reproduced** by three rows, written first,
whose clusters hold every node's move drivers from asking for admission (`ADMIT off`), as a cluster
whose arrivals' slots are all taken would. On the tree before, with node two's first device failed
and nothing put, no group moved in 300 s:

```text
an_empty_group_moves_off_a_failed_device_without_a_slot:
every group moved off the failed device with no admission never came: [{…,"moves":0,…,"recovery":{…,"behind":2006,"moves_aborted":0,"moves_switched":0,…}]
an_empty_erasure_coded_group_moves_off_a_failed_device_without_a_slot:
every group moved off the failed device with no admission never came: [{…,"moves":0,…,"redundancy":"2+1",…,"recovery":{…,"behind":2089,…,"moves_switched":0,…}]
a_put_held_across_its_empty_groups_move_is_made_whole:
every group moved off the failed device with no admission never came: [{…,"moves":0,…,"recovery":{…,"behind":2028,…,"moves_switched":0,…}]
```

The run's whole output is in `target/lab/i250/repro-before-full.txt`.

**The model took it first**: `stripe_search -- empty` runs the move protocol in a world whose group
holds nothing until a late put, and the safe policy broke nothing at 2,000 seeds a configuration
([X1](../../object-storage/stripe-model.md#an-empty-placement-group-item-250), `b5a8ae8e`).

## The fix

- **A group whose old holders list nothing of it begins its move with no slot**
  (`Mover::holds_nothing`). Every serving old holder is asked, by a listing that commits nothing and
  waits for no idle time. Any chunk listed, any holder that does not answer, or none serving, takes
  the group as holding. The verdict is kept by group and target, so a group is listed once a move.
- **The copier takes a slot before its first byte moves.** What the walk and the listings after
  the begin find is what decides, as for every move. A move with no slot that finds a stripe to copy,
  a put having landed since, asks for one then and is given up if refused.
- **Those moves run first**, within a budget of their own, `moves_inflight` again, and skip the
  count of each device's arrivals. The groups still placing on a slice that serves nothing go ahead
  of the rest, and releases of left slices go last. A group not known to hold anything is listed in
  its own task, and one found holding is admitted at the next poll, which counts its devices'
  arrivals first ([Resolved #243](move-slots-on-the-map.md)).
- **A move begun with no slot is its own driver's alone.** A later poll resumes it, and a driver
  that did not begin it, a new leader, finds it moving with no slot and gives it up. It is listed
  again at the next poll.
- **Nothing committed means anything new**: every build since wire 10 applies `MoveBegin` and
  `MoveSwitch` alike, and taking a slot or not is the driver's choice, which no replica judges. No
  wire version.

## Alternatives rejected

- **Place a group with no state at its pool's target**, the recorded direction. A group with no
  state can hold stripes. A put that missed nothing writes no row, a row that leaves nothing stale
  keeps no state, and a group back at its base is forgotten. Such a group's chunks are at the pin.
  Placed at the target, its readers would name holders that hold none of them. Nothing would mark
  those positions missed, the move driver would skip the group as already there, and the sweep
  would take the pin's copies for strays. Made safe, it needs a committed fact that a group is
  empty, fenced before its check: a begin, a listing and a switch, which is a move.
- **Place at the target only when the pin's set names an ended device.** The same hazard, on the
  groups that hold data on the failed device. And coordinators read the map at different times, so
  two could place one group at two generations under one epoch.
- **Keep every move under a slot, empty groups first.** They would still wait behind sixteen
  arrivals a device, and that wait is the window.
- **Read a coordinator's cached placement again whenever it names a slice that serves nothing.**
  It would spare a put staged by a view read before its group moved. It is a check on every put's
  placement for as long as any device is removed or retired, which tombstones keep forever, on a
  path every change to is measured on the lab first. The put's check already reads the group again
  and refreshes the view: a moved group costs each coordinating shard one rebuilt position, at 2+1
  one put refused `NotReady` and tried again.

## Invariants to uphold

- **A slot bounds bytes, never safety.** What a move copies is what its walk and its listings after
  the begin find, with a slot or without one.
- **A move with no slot takes one before its first byte arrives**, and is given up if refused.
- **A move begun with no slot is its own driver's alone**, and any other driver gives it up.
- **A group is taken as empty only when every old holder that serves listed nothing of it.**
- **No release is ordered ahead of a move** ([Resolved #333](removed-device-serves-nothing.md)).

## Still open

- **A put inside the first pass after a failure** still meets the dead slice. A replicated put
  commits a copy short, rebuilt once its group moves, and a 2+1 put is refused and tried again. The
  window is now one driver's pass, where it was the admission queue.
- **A coordinator's cached placement**, as above.
- **[O111](../optimizations.md#o111-a-placement-groups-move-costs-two-control-commits-beside-its-own-three)'s
  two control commits are gone for an empty group's move** and stay for the rest. Not timed.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `an_empty_group_moves_off_a_failed_device_without_a_slot` (`cluster_fixture`) | With no admission to be had, no group moves off the failed device, and puts after it place there |
| `an_empty_erasure_coded_group_moves_off_a_failed_device_without_a_slot` (`cluster_fixture`) | At 2+1 the same, and a put into a group still on the failed device is refused on its first try |
| `a_put_held_across_its_empty_groups_move_is_made_whole` (`cluster_fixture`) | The held puts' groups never move, and a position placed on the failed device stays missed |
| `put_races_an_empty_groups_move` (`shoal-model`, saved) | A put unchecked after its stripes, racing an empty group's move, breaks P17 in the model |
| `the_missed_record_follows_the_model` (`stripe_model_conformance`) | The product's missed record differs from the model's in an empty group's world |
| `removed_device_id_is_never_accepted_again` (`cluster_fixture`) | Its wait for no group behind is the engine's window again, which loaded runs met |

## Related

- [F88](../../features/recovery-and-moves.md#moves), the moves and their admission.
- [X1](../../object-storage/stripe-model.md#an-empty-placement-group-item-250), the empty group's
  search.
- [Resolved #250 (in part)](failed-device-returns.md), the item's other half.
- [Resolved #333](removed-device-serves-nothing.md), found planning this fix.
- [Resolved #243](move-slots-on-the-map.md), why an admission is asked for only where the slots
  leave room.
