# 250 (in part). A failed device came back once its node restarted

A device fails on a node that is up, its node reports it, and the control leader retires it: its era
ends and its placement groups move off it, rebuilt from the other holders
([F88](../../features/recovery-and-moves.md#a-failed-device-a-down-node)). The pool map held a retired device
off the map only for as long as its node reported it failed. A restart that found the disk whole
claimed it, reported it serving, cleared the failure, and the next observation began an era for it
again: a device whose chunks had been rebuilt elsewhere was placed on once more. This is the second
half of [item 250](empty-groups-move-without-a-slot.md),
filed by [F90](../../features/erasure-coded-pools.md#on-the-lab)'s lab; ~~its first half, a group never
written that places at its pool's pin, stays open there.~~ Its first half is
[Resolved #250, the rest](empty-groups-move-without-a-slot.md). Fixed by
[F92](../../features/reclamation-and-device-lifecycle.md#a-devices-life), which needed a retired device
to stay retired before a device could be removed.

## Symptom

On F90's 2+1 cluster titan's `/xfs2` was failed by an operator and its moves settled; a restart of
the nodes then ended the refusals that item 250's first half describes, "since the device was
claimed again and served, which is what an operator would not want of a device failed on purpose".
The device joined the pool map in a new era, the generation moved, and the pool's target followed
it, so groups moved back onto a disk an operator had failed.

## Cause

`PoolMap::observe` skipped a device only while `device.failed.is_some() && device.current().is_none()`.
`set_failed` clears `failed` whenever its node's report no longer names the device, and a node
reports every device it claims as serving until one fails. So the guard lasted exactly as long as the
failure did. `retire_device` recorded nothing beside the ended era, so nothing remembered why the era
had ended.

## Evidence

**Established by reading the code and by its own test.** F88's
`a_failed_device_retires_once_and_returns_only_once_its_failure_clears` asserted the return: once the
failure cleared, `observe` began an era again and the generation moved to 3. That test passed at
`3303a91`. Its replacement, `a_failed_device_retires_once_and_stays_retired`, run against `observe`
with the old guard put back (the device skipped only while drained or removed):

```text
thread 'shared::poolmap::tests::a_failed_device_retires_once_and_stays_retired' panicked at shoal-proto/src/shared/poolmap.rs:1376:9:
assertion failed: !map.observe(node, &host("a"), &claim).expect("observed")
test result: FAILED. 0 passed; 1 failed
```

The lab's restart was F90's, by reading its notes; it was not run again.

## The fix

- `DeviceRecord` gains `retired: Option<String>`, the failure that retired it, set by
  `retire_device` and never cleared.
- `observe` begins no era for a device that is drained, retired or removed (`DeviceRecord::is_ended`)
  and off the map, whatever its node claims; the node's `held` check counts such a device held, so it
  proposes nothing every few seconds.
- The node fails such a device in memory by name once its control state names it retired or removed
  (`DeviceRecord::refusal`), so it serves nothing: "device ... was retired (...): its chunks were
  placed elsewhere, so remove it and add it again as a new device".
- The way back is a removal (`remove-device`, admitted for a retired device at once) and a new
  device: the disk wiped, claimed under a new id.

## Alternatives rejected

**Clearing the retirement when the failure clears**, as the code did, which is the defect. An
operator who failed a device meant it, and a device that failed on its own may fail again; either
way its chunks were rebuilt elsewhere and are stale.

**Refusing the node's start** over a retired device it claims. One device's history would stop every
shard and table of its node; failing that device alone in memory costs nothing else.

**Leaving the device on the map and placing nothing on it** without recording why. The node could not
then say why it refuses it, and an operator could not tell a retired device from one draining.

## Invariants to uphold

- **A device's era, once ended by a retirement, a drain or a removal, is never begun again.** The way
  back is a new device.
- **A report never clears a retirement.** `set_failed` changes `failed` alone.
- **A node serves no device its control state names retired or removed.**

## Still open

~~Item 250's first half, on the known issues page: a placement group never written that places at its
pool's pin while the pin names a failed device. F92's fixture rows did not reproduce it on a
replicated pool.~~ Fixed as [Resolved #250, the rest](empty-groups-move-without-a-slot.md): a group
that holds nothing moves off a failed device with no slot.

## Tests

| Test | What breaks if it is reverted |
| --- | --- |
| `poolmap` `a_failed_device_retires_once_and_stays_retired` | A retired device's era begins again once its failure clears |
| `poolmap` `a_drained_device_is_removed_and_its_id_is_never_accepted_again` | A drained or removed device is observed back, or a removed one takes an order, a drain or a weight |
| `cluster_fixture` `removed_device_id_is_never_accepted_again` | A node serves a removed device it claims at a restart, or the map begins an era for it |

## Related

[F92](../../features/reclamation-and-device-lifecycle.md) for the device's life this fixes a door of;
[F88](../../features/recovery-and-moves.md) for the retirement; [item 250](empty-groups-move-without-a-slot.md)
for the item's other half.
